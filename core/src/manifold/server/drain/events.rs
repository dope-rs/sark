use dope_core::driver::schedule::app;

use crate::manifold::server::{
    self, Event,
    drain::{self, Empty, Progress},
};

#[doc(hidden)]
pub struct Events<'turn, 'd> {
    share: app::Share<'turn, 'd>,
    progress: Progress,
}

impl<'turn, 'd> Events<'turn, 'd> {
    pub fn fair<T>(
        work: app::Work<'turn, 'd>,
        run: impl for<'scope> FnOnce(Events<'scope, 'd>) -> T,
    ) -> T {
        work.share::<2, _>(|share| {
            run(Events {
                share,
                progress: Progress::default(),
            })
        })
    }

    pub fn next_ready<T>(
        &mut self,
        acquire: impl FnOnce() -> Option<T>,
    ) -> Result<Option<T>, Empty> {
        match self.take(|| acquire().ok_or(Empty)) {
            Some(item) => item.map(Some),
            None => Ok(None),
        }
    }

    /// Admits one ready event before its owner is consumed.
    pub fn permit(&mut self) -> Option<app::Permit<'_, 'd>> {
        match self.share.work().permit() {
            Some(permit) => {
                self.progress.record();
                Some(permit)
            }
            None => {
                self.progress.exhaust();
                None
            }
        }
    }

    fn take<T>(&mut self, take: impl FnOnce() -> T) -> Option<T> {
        Some(self.permit()?.run(take))
    }

    pub fn next<C>(&mut self, conn: &mut C) -> Option<Event<C>>
    where
        C: server::Connection,
    {
        let ready = conn.ready()?;
        self.take(|| drain::Ready::take(ready))
    }

    pub fn finish(self) -> Progress {
        self.progress
    }
}

const _: () = {
    assert!(core::mem::size_of::<Events<'static, 'static>>() == 2 * core::mem::size_of::<usize>());
    assert!(core::mem::align_of::<Events<'static, 'static>>() == core::mem::align_of::<usize>());
};
