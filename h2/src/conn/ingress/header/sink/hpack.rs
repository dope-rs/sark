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

use crate::{
    conn::ingress::header::sink::Header,
    hpack::decode::{self, sink},
};

pub(in crate::conn::ingress) struct Hpack<P: rules::Policy, A: rules::Push> {
    session: Session<P>,
    push: marker::PhantomData<fn() -> A>,
}

impl<P: rules::Policy, A: rules::Push> Hpack<P, A> {
    pub(super) fn new(session: Session<P>) -> Self {
        Self {
            session,
            push: marker::PhantomData,
        }
    }

    pub(in crate::conn::ingress) fn into_header(self) -> Header<P> {
        Header {
            session: self.session,
        }
    }

    fn failed(self, cause: lower::Error) -> (Self, decode::Failure<lower::Error>) {
        (self, decode::Failure::Sink(cause))
    }
}

impl<P: rules::Policy, A: rules::Push> sink::Direct for Hpack<P, A> {
    type Error = decode::Failure<lower::Error>;

    fn string(&mut self, cursor: &mut strings::Cursor, input: &[u8]) -> Result<(), Self::Error> {
        match self.session.string(*cursor, input) {
            lower::Step::Ready { cursor: next } => {
                *cursor = next;
                Ok(())
            }
            lower::Step::Bound { cause } => Err(decode::Error::from(cause).into()),
            lower::Step::Decode { cause, progress } => {
                Err(decode::Error::encoding(cause, progress).into())
            }
            lower::Step::Store { cause } => Err(decode::Failure::Sink(cause)),
        }
    }

    fn rollback(&mut self) {
        self.session.rollback();
    }
}

impl<P: rules::Policy, A: rules::Push> sink::Store for Hpack<P, A> {
    type Rule = ();
    type Completion = ();

    fn validate(&mut self, value: block::Validation) -> Result<(), Self::Error> {
        self.session.validate(value);
        Ok(())
    }

    fn begin(
        mut self,
        name: Option<block::Named<'_>>,
        indexing: field::Indexing,
    ) -> sink::Output<Self, sink::Begin<Self::Rule, Self::Completion>> {
        let start = match self.session.begin(name, indexing) {
            Ok(start) => start,
            Err(cause) => return Err(self.failed(cause)),
        };
        Ok((
            self,
            match start {
                Start::Skip => sink::Begin::Skip,
                Start::Buffered(rule) => sink::Begin::Buffered(rule),
                Start::Direct(done) | Start::Scratch(done) => sink::Begin::Direct(done),
            },
        ))
    }

    fn indexed(
        mut self,
        name: Option<block::Named<'_>>,
    ) -> sink::Output<Self, sink::Indexed<Self::Rule>> {
        let indexed = match self.session.indexed(name) {
            Ok(indexed) => indexed,
            Err(cause) => return Err(self.failed(cause)),
        };
        Ok((
            self,
            match indexed {
                Indexed::Skip => sink::Indexed::Skip,
                Indexed::Buffered(rule) => sink::Indexed::Buffered(rule),
            },
        ))
    }

    fn field(
        mut self,
        (): Self::Rule,
        value: block::Valued<'_>,
        indexing: field::Indexing,
        name: block::Named<'_>,
    ) -> Result<Self, (Self, Self::Error)> {
        match self.session.field::<A>(name, value, indexing) {
            Ok(()) => Ok(self),
            Err(cause) => Err(self.failed(cause)),
        }
    }

    fn direct(mut self, (): Self::Completion) -> Result<Self, (Self, Self::Error)> {
        match self.session.direct::<A>() {
            Ok(()) => Ok(self),
            Err(cause) => Err(self.failed(cause)),
        }
    }
}

const _: () =
    assert!(mem::size_of::<decode::Failure<lower::Error>>() == mem::size_of::<lower::Error>());
