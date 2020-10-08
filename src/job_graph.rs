mod job_builder;

use crossbeam::{queue::ArrayQueue, thread};
use std::{
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
};

pub use job_builder::*;

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

enum LastAccess {
    Ref(Option<usize>, Vec<usize>),
    Mut(usize),
}

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
        new_builder(self, name)
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
        let mut last_accesses = Vec::new();
        last_accesses.resize_with(self.resources.len(), || LastAccess::Ref(None, Vec::new()));
        let mut root_jobs = Vec::new();

        for (i, job_node) in self.jobs.iter().enumerate() {
            let mut any_dependencies = false;

            for resource in &job_node.refs {
                let last_access = &mut last_accesses[resource.0];
                match last_access {
                    LastAccess::Ref(mut_job_index, ref_job_indices) => {
                        if let Some(job_index) = *mut_job_index {
                            any_dependencies = true;
                            jobs[i].dependencies.push(job_index);
                            jobs[job_index].dependents.push(i);
                        }
                        ref_job_indices.push(i);
                    }
                    LastAccess::Mut(mut_job_index) => {
                        any_dependencies = true;
                        let mut_job_index = *mut_job_index;
                        jobs[i].dependencies.push(mut_job_index);
                        jobs[mut_job_index].dependents.push(i);
                        *last_access = LastAccess::Ref(Some(mut_job_index), Vec::new());
                    }
                }
            }

            for resource in &job_node.muts {
                let last_access = &mut last_accesses[resource.0];
                match last_access {
                    LastAccess::Ref(_, ref_job_indices) => {
                        if ref_job_indices.len() > 0 {
                            any_dependencies = true;
                        }
                        for ref_job_index in ref_job_indices.iter().copied() {
                            jobs[i].dependencies.push(ref_job_index);
                            jobs[ref_job_index].dependents.push(i);
                        }
                    }
                    LastAccess::Mut(mut_job_index) => {
                        any_dependencies = true;
                        let mut_job_index = *mut_job_index;
                        jobs[i].dependencies.push(mut_job_index);
                        jobs[mut_job_index].dependents.push(i);
                    }
                }
                *last_access = LastAccess::Mut(i);
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

        // Should depend on Job 1
        job_graph
            .add_job("Job 2")
            .with_ref(r1)
            .with_mut(r2)
            .schedule(move |r1, r2| {
                println!("Job 2");
                r2.extend_from_slice(r1);
            });

        // Should depend on Job 1
        job_graph.add_job("Job 3").with_ref(r1).schedule(move |r1| {
            println!("Job 3");
            println!("r1: {:?}", r1);
        });

        // Should depend on Job 1 and Job 2
        job_graph
            .add_job("Job 4")
            .with_ref(r2)
            .with_ref(r3)
            .schedule(move |r2, r3| {
                println!("Job 4");
                println!("r2: {:?}", r2);
                println!("r3: {:?}", r3);
            });

        // Should depend on Job 1 and Job 2
        job_graph
            .add_job("Job 5")
            .with_ref(r1)
            .with_ref(r2)
            .with_ref(r3)
            .schedule(move |_, _, _| {
                println!("Job 5");
            });

        job_graph.run();
        other_numbers.push(7);
    }
}
