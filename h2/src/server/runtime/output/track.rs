use o3::buffer::hold;
use sark_core::life::{self, exchange, owners};
use sark_http::owner::{self, cause};

use crate::stream;

pub(in crate::server) struct Proof {
    pub(super) stream: stream::Key,
    pub(super) charge: hold::Charge,
}

#[derive(Clone, Copy)]
pub(in crate::server) enum Progress {
    Blocked,
    Committed,
}

pub(in crate::server) trait Sink<R> {
    fn progress(&self) -> Progress;

    fn accept(&mut self, receipt: R);

    fn reject(&mut self, receipt: R);
}

pub(in crate::server) struct Track<F> {
    progress: Progress,
    accepted: F,
}

impl<F> Track<F> {
    pub(super) const fn new(accepted: F) -> Self {
        Self {
            progress: Progress::Blocked,
            accepted,
        }
    }
}

impl Sink<()> for Track<()> {
    fn progress(&self) -> Progress {
        self.progress
    }

    fn accept(&mut self, (): ()) {
        self.progress = Progress::Committed;
    }

    fn reject(&mut self, (): ()) {
        self.progress = Progress::Committed;
    }
}

impl<C: Copy, M: life::Mode<C, stream::Key>> Sink<Option<Proof>> for Track<(C, &mut M)> {
    fn progress(&self) -> Progress {
        self.progress
    }

    fn accept(&mut self, receipt: Option<Proof>) {
        self.progress = Progress::Committed;
        if let Some(receipt) = receipt {
            owners::Projection::new(&mut *self.accepted.1).apply(owner::Effect::new(
                exchange::Id::new(self.accepted.0, receipt.stream),
                receipt.charge,
                owner::Change::Handoff,
            ));
        }
    }

    fn reject(&mut self, receipt: Option<Proof>) {
        use cause::Staging;

        self.progress = Progress::Committed;
        if let Some(receipt) = receipt {
            owners::Projection::new(&mut *self.accepted.1).apply(owner::Effect::new(
                exchange::Id::new(self.accepted.0, receipt.stream),
                receipt.charge,
                owner::Change::StagingReleased(Staging::Closed),
            ));
        }
    }
}

const _: () = assert!(std::mem::size_of::<Track<()>>() == 1);
