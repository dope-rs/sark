/// One call's affine, allocation-branded task identity.
pub struct Call<P> {
    task: Option<P>,
}

pub(in crate::server::runtime) struct Vacant<'a, P>(&'a mut Option<P>);

impl<P> Default for Call<P> {
    fn default() -> Self {
        Self { task: None }
    }
}

impl<P> Call<P> {
    pub(in crate::server::runtime) const fn new(task: P) -> Self {
        Self { task: Some(task) }
    }

    pub(in crate::server::runtime) fn task(&self) -> Option<&P> {
        self.task.as_ref()
    }

    pub(in crate::server::runtime) fn vacant(&mut self) -> Option<Vacant<'_, P>> {
        self.task.is_none().then_some(Vacant(&mut self.task))
    }

    pub(in crate::server::runtime) fn take(&mut self) -> Option<P> {
        self.task.take()
    }

    pub(in crate::server::runtime) fn cancel(&mut self, cancel: impl FnOnce(P)) {
        if let Some(task) = self.task.take() {
            cancel(task);
        }
    }
}

impl<P> Vacant<'_, P> {
    pub(in crate::server::runtime) fn insert(self, task: P) {
        *self.0 = Some(task);
    }
}
