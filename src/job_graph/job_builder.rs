use super::{JobGraph, JobNode, Resource, ResourceHandle};

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

pub(super) fn new_builder<'graph, 'builder>(
    graph: &'builder mut JobGraph<'graph>,
    name: &'graph str,
) -> JobBuilder<'graph, 'builder, ()> {
    JobBuilder {
        data: JobBuilderData {
            graph,
            name,
            refs: vec![],
            muts: vec![],
        },
        resources: (),
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
