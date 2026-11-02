use core::{mem, ops};

use o3::buffer::resident;
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{self, event, machine},
    server::response::Receipt,
    stream,
};

#[doc(hidden)]
pub struct Head<'d> {
    stream: stream::Key,
    receipt: Receipt<'d>,
}

/// Owns the staged headers while their exact connection remains borrowed.
pub(super) struct Staged<'a, 'd, F: rules::Policy, E: event::Evidence> {
    pub(super) connection: &'a mut machine::Connection<
        crate::ServerRole,
        F,
        resident::Shared<'d>,
        rules::RejectPush,
        E,
    >,
    pub(super) stream: stream::Key,
}

impl<'d, F: rules::Policy, E: event::Evidence> Staged<'_, 'd, F, E> {
    pub(super) fn commit(self) -> Result<(), conn::Error> {
        self.connection.streams().headers().commit(self.stream)?;
        mem::forget(self);
        Ok(())
    }

    pub(super) fn defer(
        self,
        receipt: Receipt<'d>,
    ) -> Result<ops::ControlFlow<Head<'d>, Receipt<'d>>, conn::Error> {
        let outcome = Head::new(self.stream, receipt).drive(&mut *self.connection)?;
        mem::forget(self);
        Ok(outcome)
    }
}

impl<F: rules::Policy, E: event::Evidence> Drop for Staged<'_, '_, F, E> {
    fn drop(&mut self) {
        self.connection.streams().headers().discard(self.stream);
    }
}

impl<'d> Head<'d> {
    pub(super) fn empty(stream: stream::Key) -> Self {
        Self {
            stream,
            receipt: Receipt::empty(),
        }
    }

    pub(super) fn new(stream: stream::Key, receipt: Receipt<'d>) -> Self {
        Self { stream, receipt }
    }

    pub(crate) fn drive<F: rules::Policy, E: event::Evidence>(
        self,
        conn: &mut machine::Connection<
            crate::ServerRole,
            F,
            resident::Shared<'d>,
            rules::RejectPush,
            E,
        >,
    ) -> Result<ops::ControlFlow<Self, Receipt<'d>>, conn::Error> {
        match conn.streams().headers().try_commit(self.stream)? {
            ops::ControlFlow::Continue(()) => Ok(ops::ControlFlow::Continue(self.receipt)),
            ops::ControlFlow::Break(_) => Ok(ops::ControlFlow::Break(self)),
        }
    }

    pub(crate) fn discard<F: rules::Policy, E: event::Evidence>(
        self,
        conn: &mut machine::Connection<
            crate::ServerRole,
            F,
            resident::Shared<'d>,
            rules::RejectPush,
            E,
        >,
    ) {
        conn.streams().headers().discard(self.stream);
    }
}

const _: () = {
    assert!(
        mem::size_of::<Staged<'static, 'static, rules::RetainRaw, event::Retirement>>()
            == 2 * mem::size_of::<usize>()
    );
    let fields = mem::size_of::<stream::Key>() + mem::size_of::<Receipt<'_>>();
    assert!(mem::size_of::<Head<'_>>() >= fields);
    assert!(mem::size_of::<Head<'_>>() < fields + mem::align_of::<Head<'_>>());
    assert!(mem::align_of::<Head<'_>>() == mem::align_of::<usize>());
};
