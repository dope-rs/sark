use core::{marker, mem};

use sark_http::{
    field,
    field::metadata::{
        block, lower,
        section::{rules, state},
        strings,
    },
};
use state::decode::{Indexed, Session, Start};

use crate::hpack::decode::{self, sink};

pub(in crate::conn::ingress) struct Hpack<'a, P: rules::Policy, A: rules::Push> {
    session: &'a mut Session<P>,
    push: marker::PhantomData<fn() -> A>,
}

impl<'a, P: rules::Policy, A: rules::Push> Hpack<'a, P, A> {
    pub(super) fn new(session: &'a mut Session<P>) -> Self {
        Self {
            session,
            push: marker::PhantomData,
        }
    }

    fn failed(self, cause: lower::Error) -> (Self, decode::Failure<lower::Error>) {
        (self, decode::Failure::Sink(cause))
    }
}

impl<P: rules::Policy, A: rules::Push> sink::Direct for Hpack<'_, P, A> {
    type Error = decode::Failure<lower::Error>;

    fn string(&mut self, cursor: &mut strings::Cursor, input: &[u8]) -> Result<(), Self::Error> {
        match self.session.try_string(cursor, input) {
            Ok(()) => Ok(()),
            Err(lower::StringError::Bound(cause)) => Err(decode::Error::from(cause).into()),
            Err(lower::StringError::Decode { cause, progress }) => {
                Err(decode::Error::encoding(cause, progress).into())
            }
            Err(lower::StringError::Store(cause)) => Err(decode::Failure::Sink(cause)),
        }
    }

    fn rollback(&mut self) {
        self.session.rollback();
    }
}

impl<P: rules::Policy, A: rules::Push> sink::Store for Hpack<'_, P, A> {
    fn validate(&mut self, value: block::Validation) -> Result<(), Self::Error> {
        self.session.validate(value);
        Ok(())
    }

    fn begin(
        self,
        name: Option<block::Named<'_>>,
        indexing: field::Indexing,
    ) -> sink::Output<Self, Start<(), ()>> {
        match self.session.begin(name, indexing) {
            Ok(start) => Ok((self, start)),
            Err(cause) => Err(self.failed(cause)),
        }
    }

    fn indexed(self, name: Option<block::Named<'_>>) -> sink::Output<Self, Indexed<()>> {
        match self.session.indexed(name) {
            Ok(indexed) => Ok((self, indexed)),
            Err(cause) => Err(self.failed(cause)),
        }
    }

    fn field(
        self,
        value: block::Valued<'_>,
        indexing: field::Indexing,
        name: block::Named<'_>,
    ) -> Result<Self, (Self, Self::Error)> {
        match self.session.field::<A>(name, value, indexing) {
            Ok(()) => Ok(self),
            Err(cause) => Err(self.failed(cause)),
        }
    }

    fn direct(self) -> Result<Self, (Self, Self::Error)> {
        match self.session.direct::<A>() {
            Ok(()) => Ok(self),
            Err(cause) => Err(self.failed(cause)),
        }
    }
}

const _: () =
    assert!(mem::size_of::<decode::Failure<lower::Error>>() == mem::size_of::<lower::Error>());
