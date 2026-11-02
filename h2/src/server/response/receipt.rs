use o3::buffer::{hold, resident, storage};
use sark_http::{
    owner::{evidence, handoff},
    payload::owned,
};

use crate::{conn, server::response::Pending};

#[doc(hidden)]
pub struct Receipt<'d> {
    pending: Option<Pending<'d>>,
    linked: hold::Charge,
    copied: Option<evidence::Copy>,
}

impl<'d> Receipt<'d> {
    pub(super) fn from_ready(
        body: handoff::Ready<'d>,
        linked: hold::Charge,
    ) -> Result<Self, conn::Error> {
        let (pending, copied) = match body {
            handoff::Ready::Empty => (None, None),
            handoff::Ready::Owned(mut body) => {
                let body = storage::Shared::try_from(&mut body)
                    .map_err(storage::AllocError::Exhausted)
                    .map_err(owned::Error::Storage)
                    .map_err(conn::Error::Response)?;
                (Pending::single(resident::Shared::from(body)), None)
            }
            handoff::Ready::Copied(body) => {
                let (body, copy) = body.into_parts();
                (Pending::single(resident::Shared::from(body)), Some(copy))
            }
            handoff::Ready::Retained(body) => (Pending::single(resident::Shared::from(body)), None),
            handoff::Ready::Segmented(body) | handoff::Ready::Linked(body) => {
                (Pending::new(body), None)
            }
            handoff::Ready::Fixed(body) => {
                let body: resident::Shared<'d> = resident::Shared::from(body.as_slice());
                (Pending::single(body), None)
            }
        };
        Ok(Self::new(pending, linked, copied))
    }

    pub(super) const fn new(
        pending: Option<Pending<'d>>,
        linked: hold::Charge,
        copied: Option<evidence::Copy>,
    ) -> Self {
        Self {
            pending,
            linked,
            copied,
        }
    }

    pub(super) fn empty() -> Self {
        Self::new(None, hold::Charge::default(), None)
    }

    pub(in crate::server) fn into_pending(self) -> Option<Pending<'d>> {
        self.pending
    }

    pub(crate) fn into_parts(self) -> (Option<Pending<'d>>, hold::Charge, Option<evidence::Copy>) {
        (self.pending, self.linked, self.copied)
    }
}
