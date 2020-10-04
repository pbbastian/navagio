use crossbeam::{queue::ArrayQueue, thread};
use std::{
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Copy, Clone)]
struct ResourceHandle(usize);

pub struct Resource<'graph, T: Sync> {
    handle: ResourceHandle,
    ptr: *mut T,
    phantom: PhantomData<&'graph ()>,
}

impl<'graph, T: Sync> Clone for Resource<'graph, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'graph, T: Sync> Copy for Resource<'graph, T> {}

pub struct JobGraph<'graph> {
    jobs: Vec<JobNode<'graph>>,
    resources: Vec<ResourceNode<'graph>>,
}

struct JobBuilderData<'graph, 'builder> {
    graph: &'builder mut JobGraph<'graph>,
    name: &'graph str,
    refs: Vec<ResourceHandle>,
    muts: Vec<ResourceHandle>,
}

pub struct JobBuilder<'graph, 'builder, T> {
    data: JobBuilderData<'graph, 'builder>,
    resources: T,
}

struct JobNode<'a> {
    name: &'a str,
    f: Box<dyn FnMut() + Send + 'a>,
    refs: Vec<ResourceHandle>,
    muts: Vec<ResourceHandle>,
}

struct ResourceNode<'a> {
    _name: &'a str,
}

struct JobData<'a> {
    name: &'a str,
    f: *mut (dyn FnMut() + Send + 'a),
    dependents: Vec<usize>,
    dependencies: Vec<usize>,
}

unsafe impl<'a> Send for JobData<'a> {}
unsafe impl<'a> Sync for JobData<'a> {}

impl<'graph> JobGraph<'graph> {
    pub fn new() -> Self {
        JobGraph {
            jobs: vec![],
            resources: vec![],
        }
    }

    pub fn add_job<'builder>(
        &'builder mut self,
        name: &'graph str,
    ) -> JobBuilder<'graph, 'builder, ()> {
        JobBuilder {
            data: JobBuilderData {
                graph: self,
                name,
                refs: vec![],
                muts: vec![],
            },
            resources: (),
        }
    }

    pub fn add_resource<T: Sync + Clone>(
        &mut self,
        name: &'graph str,
        resource: &'graph mut T,
    ) -> Resource<'graph, T> {
        let handle = ResourceHandle(self.resources.len());
        self.resources.push(ResourceNode { _name: name });
        Resource {
            handle,
            ptr: resource,
            phantom: Default::default(),
        }
    }

    pub fn run(mut self) {
        let mut jobs: Vec<_> = self
            .jobs
            .iter_mut()
            .map(|job_node| JobData {
                name: job_node.name,
                f: job_node.f.as_mut(),
                dependents: vec![],
                dependencies: vec![],
            })
            .collect();
        let mut last_mut = Vec::new();
        last_mut.resize(self.resources.len(), None);
        let mut root_jobs = Vec::new();

        for (i, job_node) in self.jobs.iter().enumerate() {
            let mut any_dependencies = false;

            for resource in &job_node.refs {
                if let Some(job_index) = last_mut[resource.0] {
                    let job_data: &mut JobData = &mut jobs[job_index];
                    job_data.dependents.push(i);
                    any_dependencies = true;
                    jobs[i].dependencies.push(job_index);
                }
            }

            for resource in &job_node.muts {
                if let Some(job_index) = last_mut[resource.0] {
                    let job_data: &mut JobData = &mut jobs[job_index];
                    job_data.dependents.push(i);
                    any_dependencies = true;
                    jobs[i].dependencies.push(job_index);
                }
                last_mut[resource.0] = Some(i);
            }

            if !any_dependencies {
                root_jobs.push(i);
            }
        }

        for job in &mut jobs {
            job.dependents.sort();
            job.dependents.dedup();

            job.dependencies.sort();
            job.dependencies.dedup();
        }

        let counters: Vec<_> = jobs
            .iter()
            .map(|j| AtomicUsize::new(j.dependencies.len()))
            .collect();

        root_jobs.sort();
        root_jobs.dedup();

        let thread_count = num_cpus::get();
        println!("[job graph] Spawning {} threads", thread_count);

        let queue = ArrayQueue::new(65_536);
        for index in root_jobs.iter().copied() {
            println!("[job graph] Scheduling root \"{}\"", jobs[index].name);
            queue.push(index).unwrap();
        }

        let queue = &queue;
        let jobs = &jobs;
        let num_remaining = AtomicUsize::new(root_jobs.len());

        thread::scope(|s| {
            for _i in 0..thread_count {
                let num_remaining = &num_remaining;
                let counters = &counters;
                s.spawn(move |_| {
                    while num_remaining.load(Ordering::SeqCst) > 0 {
                        if let Ok(job_index) = queue.pop() {
                            let job = &jobs[job_index];
                            let f = unsafe { job.f.as_mut().unwrap() };
                            f();
                            for dependent in job.dependents.iter().copied() {
                                println!(
                                    "[job graph] \"{}\" decrementing counter for \"{}\"",
                                    jobs[job_index].name, jobs[dependent].name
                                );
                                if counters[dependent].fetch_sub(1, Ordering::SeqCst) == 1 {
                                    println!(
                                        "[job graph] \"{}\" scheduling job \"{}\"",
                                        jobs[job_index].name, jobs[dependent].name
                                    );
                                    num_remaining.fetch_add(1, Ordering::SeqCst);
                                    queue.push(dependent).unwrap();
                                }
                            }
                            num_remaining.fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                });
            }
        })
        .unwrap();
    }
}

impl<'a, 'b, T: 'a + Send> JobBuilder<'a, 'b, T> {
    fn next_ref<TResource: 'a + Sync, TResult>(
        mut self,
        resource: Resource<'a, TResource>,
        mut f: impl FnMut(T, &'a TResource) -> TResult,
    ) -> JobBuilder<'a, 'b, TResult> {
        self.data.refs.push(resource.handle);
        JobBuilder {
            data: self.data,
            // [unsafe] Access to the ref is guarded by the graph.
            resources: f(self.resources, unsafe { resource.ptr.as_ref() }.unwrap()),
        }
    }

    fn next_mut<TResource: 'a + Sync, TResult>(
        mut self,
        resource: Resource<'a, TResource>,
        mut f: impl FnMut(T, &'a mut TResource) -> TResult,
    ) -> JobBuilder<'a, 'b, TResult> {
        self.data.muts.push(resource.handle);
        JobBuilder {
            data: self.data,
            // [unsafe] Access to the ref is guarded by the graph.
            resources: f(self.resources, unsafe { resource.ptr.as_mut() }.unwrap()),
        }
    }

    fn build<F>(self, mut f: F)
    where
        F: FnMut(T) + Send + 'a,
    {
        let resource = self.resources;
        self.data.graph.jobs.push(JobNode {
            name: self.data.name,
            f: Box::new(move || {
                let r = unsafe { std::ptr::read(&resource) };
                f(r);
            }),
            refs: self.data.refs,
            muts: self.data.muts,
        });
    }
}

impl<'a, 'b> JobBuilder<'a, 'b, ()> {
    pub fn schedule(self, mut f: impl FnMut() + Send + 'a) {
        self.build(move |_| f());
    }

    pub fn with_ref<T1: Sync>(self, resource: Resource<'a, T1>) -> JobBuilder<'a, 'b, (&'a T1,)> {
        self.next_ref(resource, |_, r| (r,))
    }

    pub fn with_mut<T1: Sync>(
        self,
        resource: Resource<'a, T1>,
    ) -> JobBuilder<'a, 'b, (&'a mut T1,)> {
        self.next_mut(resource, |_, r| (r,))
    }
}

impl<'a, 'b, T1: 'a + Send> JobBuilder<'a, 'b, (T1,)> {
    pub fn schedule(self, mut f: impl FnMut(T1) + Send + 'a) {
        self.build(move |r| f(r.0));
    }

    pub fn with_ref<T2: Sync>(
        self,
        resource: Resource<'a, T2>,
    ) -> JobBuilder<'a, 'b, (T1, &'a T2)> {
        self.next_ref(resource, |rs, r| (rs.0, r))
    }

    pub fn with_mut<T2: Sync>(
        self,
        resource: Resource<'a, T2>,
    ) -> JobBuilder<'a, 'b, (T1, &'a mut T2)> {
        self.next_mut(resource, |rs, r| (rs.0, r))
    }
}

impl<'a, 'b, T1: 'a + Send, T2: 'a + Send> JobBuilder<'a, 'b, (T1, T2)> {
    pub fn schedule(self, mut f: impl FnMut(T1, T2) + Send + 'a) {
        self.build(move |r| f(r.0, r.1));
    }

    pub fn with_ref<T3: Sync>(
        self,
        resource: Resource<'a, T3>,
    ) -> JobBuilder<'a, 'b, (T1, T2, &'a T3)> {
        self.next_ref(resource, |rs, r| (rs.0, rs.1, r))
    }

    pub fn with_mut<T3: Sync>(
        self,
        resource: Resource<'a, T3>,
    ) -> JobBuilder<'a, 'b, (T1, T2, &'a mut T3)> {
        self.next_mut(resource, |rs, r| (rs.0, rs.1, r))
    }
}

impl<'a, 'b, T1: 'a + Send, T2: 'a + Send, T3: 'a + Send> JobBuilder<'a, 'b, (T1, T2, T3)> {
    pub fn schedule(self, mut f: impl FnMut(T1, T2, T3) + Send + 'a) {
        self.build(move |r| f(r.0, r.1, r.2));
    }

    pub fn with_ref<T4: Sync>(
        self,
        resource: Resource<'a, T4>,
    ) -> JobBuilder<'a, 'b, (T1, T2, T3, &'a T4)> {
        self.next_ref(resource, |rs, r| (rs.0, rs.1, rs.2, r))
    }

    pub fn with_mut<T4: Sync>(
        self,
        resource: Resource<'a, T4>,
    ) -> JobBuilder<'a, 'b, (T1, T2, T3, &'a mut T4)> {
        self.next_mut(resource, |rs, r| (rs.0, rs.1, rs.2, r))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_empty_graph() {
        let mut _task_graph = JobGraph::new();
    }

    #[test]
    fn it_works() {
        let mut numbers = vec![1, 2, 3];
        let mut other_numbers = vec![7, 8, 9];
        let mut more_numbers = vec![4, 5, 6];
        let mut job_graph = JobGraph::new();

        let r1 = job_graph.add_resource("Numbers", &mut numbers);
        let r2 = job_graph.add_resource("Other numbers", &mut other_numbers);
        let r3 = job_graph.add_resource("More numbers", &mut more_numbers);

        job_graph
            .add_job("Job 1")
            .with_mut(r1)
            .with_mut(r3)
            .schedule(move |r1, r3| {
                println!("Job 1");
                r1.push(10);
                r3.push(11);
            });

        job_graph
            .add_job("Job 2")
            .with_ref(r1)
            .with_mut(r2)
            .schedule(move |r1, r2| {
                println!("Job 2");
                r2.extend_from_slice(r1);
            });

        job_graph.add_job("Job 3").with_ref(r1).schedule(move |r1| {
            println!("Job 3");
            println!("r1: {:?}", r1);
        });

        job_graph
            .add_job("Job 4")
            .with_ref(r2)
            .with_ref(r3)
            .schedule(move |r2, r3| {
                println!("Job 4");
                println!("r2: {:?}", r2);
                println!("r3: {:?}", r3);
            });

        job_graph.run();
        other_numbers.push(7);
    }
}
