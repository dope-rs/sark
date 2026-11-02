//! H2 implementation of the common HTTP client boundary.

mod domain;
mod plan;

pub use domain::Domain;
use dope::net::link::egress::{data::pair, load::lease};
use o3::{buffer::pool, mem::Return};
pub use plan::Plan;
use sark_core::{life::session, manifold::client::io};

use crate::{
    ClientRole,
    conn::{event, machine::ops::state, outputs::transfer},
};

/// Linear H2 final-link admission.
#[must_use = "dropping a prepared write keeps H2 egress unchanged"]
pub struct Prepared<'a, C: session::Storage + 'a> {
    state: &'a mut state::State<ClientRole>,
    write: transfer::Pooled<'a, C>,
}

impl<'a, C: session::Storage + 'a> Prepared<'a, C> {
    pub(crate) fn new(
        state: &'a mut state::State<ClientRole>,
        write: transfer::Pooled<'a, C>,
    ) -> Self {
        Self { state, write }
    }

    pub(crate) fn output(write: event::Write<(), C>) -> io::Output<C> {
        match write {
            event::Write::Buffered(written) => io::Output::Buffered(written),
            event::Write::Owned {
                prefix_len: len,
                body,
                ..
            }
            | event::Write::Split {
                prefix_len: len,
                body,
                ..
            } => io::Output::Split {
                prefix_len: len,
                body,
            },
        }
    }
}

impl<'a, C: session::Storage + 'a> io::Prepared<C> for Prepared<'a, C> {
    type Prefix = Prefix<'a, C>;
    type Body = Body<'a, C>;

    fn try_bind(self) -> Result<io::Offer<Self::Prefix, Self::Body>, io::Failure> {
        let Self { state, write } = self;
        Ok(match write.try_bind()? {
            transfer::Offer::Prefix { value, owner } => io::Offer::Prefix(value.lease(Prefix {
                state,
                write: owner,
            })),
            transfer::Offer::Body { value, owner } => io::Offer::Body(value.lease(Body {
                state,
                write: owner,
            })),
        })
    }
}

pub struct Prefix<'a, C: session::Storage> {
    state: &'a mut state::State<ClientRole>,
    write: transfer::Prefix<'a, C>,
}

pub struct Body<'a, C: session::Storage> {
    state: &'a mut state::State<ClientRole>,
    write: transfer::Body<'a, C>,
}

impl<C: session::Storage> Return for Prefix<'_, C> {
    type Item = pool::Cursor;

    fn return_item(self, value: Self::Item) {
        self.write.return_item(value);
    }
}

impl<C: session::Storage> Return for Body<'_, C> {
    type Item = pair::Source<C>;

    fn return_item(self, value: Self::Item) {
        self.write.return_item(value);
    }
}

impl<C: session::Storage> lease::Commit for Prefix<'_, C> {
    fn commit(self) {
        let write = self.write.commit();
        self.state.commit_write(write);
    }
}

impl<C: session::Storage> lease::Commit for Body<'_, C> {
    fn commit(self) {
        let write = self.write.commit();
        self.state.commit_write(write);
    }
}
