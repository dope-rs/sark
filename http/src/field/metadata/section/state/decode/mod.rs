mod decoded;
mod session;

use core::{convert::Infallible, mem};

pub use decoded::Decoded;
use lower::plain;
pub use session::Session;
use state::{RetainedHead, Selected, fixed};

use crate::field::{
    self, head,
    metadata::{
        block, lower,
        map::record,
        section,
        section::{rules, state, storage},
    },
};

#[doc(hidden)]
pub enum Start<R, D> {
    Skip,
    Buffered(R),
    Direct(D),
    Scratch(D),
}

#[doc(hidden)]
pub enum Indexed<R> {
    Skip,
    Buffered(R),
}

const _: () = assert!(mem::size_of::<Indexed<usize>>() <= mem::size_of::<Start<usize, Plan>>());

#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct Plan;

#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct Omit;

const _: () = assert!(mem::size_of::<Plan>() == 0);
const _: () = assert!(mem::size_of::<Omit>() == 0);

#[doc(hidden)]
pub trait Completion<P: section::Policy>: Copy {
    type State;

    /// Leaves the borrowed input in its completed or terminal rollback state.
    /// Rejection preserves that owner and returns the exact cause.
    fn try_finish<A: rules::Push>(
        self,
        state: &mut Self::State,
        input: &mut lower::Input<P::Block>,
        scratch: &mut storage::Scratch,
    ) -> Result<(), lower::Error>;
}

pub type Head<P, C = block::Plan> = RetainedHead<<P as section::Policy>::Block, Selected<P>, C>;

pub enum Final<P: section::Policy> {
    Request(Head<P>),
    Response(Head<P, block::Response>),
    /// Admitted trailers or a push promise, without a request routing plan.
    Other(Head<P, ()>),
    Rejected(Rejected<P>),
}

pub struct Rejected<P: section::Policy> {
    head: Head<P, ()>,
    sealed: block::Validation,
    finished: block::Validation,
    response: Option<block::Response>,
}

impl<P: section::Policy> Rejected<P> {
    pub fn validation(&self) -> (block::Validation, block::Validation) {
        (self.sealed, self.finished)
    }

    pub fn response(&self) -> Option<block::Response> {
        self.response
    }
}

impl<P: section::Policy> Final<P> {
    pub(super) fn new(decoded: Decoded<P>) -> Self {
        let mut head = RetainedHead {
            fields: <P::Block as storage::Block>::from(decoded.storage),
            pick: Selected {
                value: decoded.pick,
            },
            control: (),
            framing: Some(decoded.framing),
        };
        match (decoded.sealed, decoded.outcome) {
            (block::Validation::Valid, block::Outcome::Request(plan)) => {
                Self::Request(head.bind(plan))
            }
            (block::Validation::Valid, block::Outcome::Response(response)) => {
                Self::Response(head.bind(response))
            }
            (block::Validation::Valid, block::Outcome::Other) => Self::Other(head),
            (sealed, outcome) => {
                head.framing = None;
                Self::Rejected(Rejected {
                    head,
                    sealed,
                    finished: outcome.validation(),
                    response: outcome.response(),
                })
            }
        }
    }

    pub fn framing(&self) -> Option<block::Framing> {
        match self {
            Self::Request(head) => head.framing(),
            Self::Response(head) => head.framing(),
            Self::Other(head) => head.framing(),
            Self::Rejected(_) => None,
        }
    }

    pub fn into_parts(
        self,
    ) -> (
        Head<P, Option<block::Plan>>,
        block::Validation,
        block::Validation,
        Option<block::Response>,
    ) {
        let (head, response) = match self {
            Self::Request(head) => (head.into(), None),
            Self::Response(head) => {
                let response = *head.control();
                (head.into(), Some(response))
            }
            Self::Other(head) => (head.into(), None),
            Self::Rejected(rejected) => {
                let (sealed, finished) = rejected.validation();
                let response = rejected.response();
                return (rejected.head.bind(None), sealed, finished, response);
            }
        };
        let check = block::Validation::Valid;
        (head, check, check, response)
    }
}

pub trait Sink<P: section::Policy>: rules::Validate + Sized {
    type Error: Into<lower::Error>;
    type Rule: Copy;
    type Direct: Completion<P, State = Self>;

    fn h2(kind: rules::Kind, connect: block::Connect) -> Self;

    fn begin(
        &mut self,
        input: &mut lower::Input<P::Block>,
        name: block::Named<'_>,
        indexing: field::Indexing,
    ) -> Result<Start<Self::Rule, Self::Direct>, lower::Error>;

    fn indexed(
        &mut self,
        input: &mut lower::Input<P::Block>,
        name: block::Named<'_>,
    ) -> Result<Indexed<Self::Rule>, lower::Error>;

    fn field<A: rules::Push>(
        &mut self,
        input: &mut lower::Input<P::Block>,
        scratch: &mut storage::Scratch,
        rule: Self::Rule,
        name: record::Span<'_>,
        value: record::Span<'_>,
        indexing: field::Indexing,
    ) -> Result<(), lower::Error>;

    fn seal(
        self,
        input: lower::Input<P::Block>,
        scratch: &mut storage::Scratch,
    ) -> Result<Decoded<P>, Self::Error>;
}

impl<P: rules::Plan> Completion<P> for Plan {
    type State = state::Decoder<P>;

    fn try_finish<A: rules::Push>(
        self,
        state: &mut Self::State,
        input: &mut lower::Input<P::Block>,
        scratch: &mut storage::Scratch,
    ) -> Result<(), lower::Error> {
        let Self = self;
        input.try_finish::<P, A>(state, scratch)
    }
}

impl Completion<rules::Discard> for Omit {
    type State = fixed::Discard;

    fn try_finish<A: rules::Push>(
        self,
        state: &mut Self::State,
        input: &mut lower::Input<storage::Fields>,
        scratch: &mut storage::Scratch,
    ) -> Result<(), lower::Error> {
        let Self = self;
        input.try_omit::<A>(state, scratch)
    }
}

impl<P> Completion<P> for Infallible
where
    P: section::Policy<Selection = (), Block: storage::Block<Format = storage::Raw>>,
{
    type State = fixed::Raw;

    fn try_finish<A: rules::Push>(
        self,
        _: &mut Self::State,
        _: &mut lower::Input<P::Block>,
        _: &mut storage::Scratch,
    ) -> Result<(), lower::Error> {
        match self {}
    }
}

impl<P: rules::Plan> Sink<P> for state::Decoder<P> {
    type Error = lower::Error;
    type Rule = rules::Decision<P::Inspect>;
    type Direct = Plan;

    fn h2(kind: rules::Kind, connect: block::Connect) -> Self {
        Self {
            core: state::Core::new(kind, connect, block::Version::Two),
            policy: P::default(),
        }
    }

    fn begin(
        &mut self,
        input: &mut lower::Input<P::Block>,
        name: block::Named<'_>,
        indexing: field::Indexing,
    ) -> Result<Start<Self::Rule, Self::Direct>, lower::Error> {
        let known = name.proof().head();
        let mut next = self.core.control;
        let check = next.accept_name(known);
        let rule = self.decision(name.span(), known, next, check);
        let start = match rule {
            rules::Decision::Tagged(known, tag) => {
                input.try_tagged(known, tag, indexing)?;
                Start::Direct(Plan)
            }
            rules::Decision::Skip => {
                input.try_ready()?;
                Start::Skip
            }
            rules::Decision::Scratch => {
                input.try_ready()?;
                Start::Scratch(Plan)
            }
            rule => {
                input.try_ready()?;
                Start::Buffered(rule)
            }
        };
        self.core.control = next;
        input.indexing = indexing;
        Ok(start)
    }

    fn indexed(
        &mut self,
        input: &mut lower::Input<P::Block>,
        name: block::Named<'_>,
    ) -> Result<Indexed<Self::Rule>, lower::Error> {
        input.try_ready()?;
        Ok(match self.decide(name.span(), name.proof()) {
            rules::Decision::Skip => Indexed::Skip,
            rule => Indexed::Buffered(rule),
        })
    }

    fn field<A: rules::Push>(
        &mut self,
        input: &mut lower::Input<P::Block>,
        scratch: &mut storage::Scratch,
        rule: Self::Rule,
        name: record::Span<'_>,
        value: record::Span<'_>,
        indexing: field::Indexing,
    ) -> Result<(), lower::Error> {
        input.try_ready()?;
        let push = lower::Lower::new(&mut input.lease, scratch, self)
            .try_push_as(rule, name, value, indexing)?;
        push.commit::<A>();
        Ok(())
    }

    fn seal(
        mut self,
        input: lower::Input<P::Block>,
        scratch: &mut storage::Scratch,
    ) -> Result<Decoded<P>, lower::Error> {
        let mut writer = input.try_seal()?;
        let sealed = state::Decoder::seal(&mut self, &mut writer.lease, scratch)?;
        let outcome = self.core.control.finish();
        Ok(Decoded::new(
            writer.finish(),
            self.policy.finish(),
            sealed,
            outcome,
            self.core.control.framing(),
        ))
    }
}

impl<P> Sink<P> for fixed::Raw
where
    P: section::Policy<Selection = (), Block: storage::Block<Format = storage::Raw>>,
{
    type Error = lower::Error;
    type Rule = head::Name;
    type Direct = Infallible;

    fn h2(kind: rules::Kind, connect: block::Connect) -> Self {
        Self {
            core: state::Core::new(kind, connect, block::Version::Two),
        }
    }

    fn begin(
        &mut self,
        input: &mut lower::Input<P::Block>,
        name: block::Named<'_>,
        indexing: field::Indexing,
    ) -> Result<Start<Self::Rule, Self::Direct>, lower::Error> {
        input.try_ready()?;
        let start = if self.accept(name.proof()) != block::Validation::Valid {
            Start::Skip
        } else {
            Start::Buffered(name.proof().head())
        };
        input.indexing = indexing;
        Ok(start)
    }

    fn indexed(
        &mut self,
        input: &mut lower::Input<P::Block>,
        name: block::Named<'_>,
    ) -> Result<Indexed<Self::Rule>, lower::Error> {
        input.try_ready()?;
        Ok(if self.accept(name.proof()) != block::Validation::Valid {
            Indexed::Skip
        } else {
            Indexed::Buffered(name.proof().head())
        })
    }

    fn field<A: rules::Push>(
        &mut self,
        input: &mut lower::Input<P::Block>,
        scratch: &mut storage::Scratch,
        known: Self::Rule,
        name: record::Span<'_>,
        value: record::Span<'_>,
        indexing: field::Indexing,
    ) -> Result<(), lower::Error> {
        input.try_ready()?;
        let field = plain::Plain::try_new(&mut input.lease, name, value, indexing)?;
        field.push_raw::<A>(scratch, self, known);
        Ok(())
    }

    fn seal(
        self,
        input: lower::Input<P::Block>,
        scratch: &mut storage::Scratch,
    ) -> Result<Decoded<P>, lower::Error> {
        let writer = input.try_seal()?;
        scratch.clear();
        let outcome = self.core.control.finish();
        Ok(Decoded::new(
            writer.finish(),
            (),
            block::Validation::Valid,
            outcome,
            self.core.control.framing(),
        ))
    }
}

impl Sink<rules::Discard> for fixed::Discard {
    type Error = lower::Error;
    type Rule = fixed::Omit;
    type Direct = Omit;

    fn h2(kind: rules::Kind, connect: block::Connect) -> Self {
        Self {
            core: state::Core::new(kind, connect, block::Version::Two),
        }
    }

    fn begin(
        &mut self,
        input: &mut lower::Input<storage::Fields>,
        name: block::Named<'_>,
        indexing: field::Indexing,
    ) -> Result<Start<Self::Rule, Self::Direct>, lower::Error> {
        let valid = self.route(name.proof());
        let mut next = self.core.control;
        let check = next.accept_name(name.proof().head());
        let rule = match check {
            block::Validation::Valid => valid,
            block::Validation::Malformed | block::Validation::RejectPush => fixed::Omit::Skip,
        };
        let start = match rule {
            fixed::Omit::Authority => {
                input.try_tagged(head::Name::Authority, storage::Tag::AUTHORITY, indexing)?;
                Start::Direct(Omit)
            }
            fixed::Omit::Skip => {
                input.try_ready()?;
                Start::Skip
            }
            rule => {
                input.try_ready()?;
                Start::Buffered(rule)
            }
        };
        self.core.control = next;
        input.indexing = indexing;
        Ok(start)
    }

    fn indexed(
        &mut self,
        input: &mut lower::Input<storage::Fields>,
        name: block::Named<'_>,
    ) -> Result<Indexed<Self::Rule>, lower::Error> {
        input.try_ready()?;
        if self.accept(name.proof()) != block::Validation::Valid {
            return Ok(Indexed::Skip);
        }
        Ok(match self.route(name.proof()) {
            fixed::Omit::Skip => Indexed::Skip,
            rule => Indexed::Buffered(rule),
        })
    }

    fn field<A: rules::Push>(
        &mut self,
        input: &mut lower::Input<storage::Fields>,
        scratch: &mut storage::Scratch,
        rule: Self::Rule,
        name: record::Span<'_>,
        value: record::Span<'_>,
        indexing: field::Indexing,
    ) -> Result<(), lower::Error> {
        use lower::fixed::Authority;
        input.try_ready()?;
        match rule {
            fixed::Omit::Authority => {
                let field =
                    Authority::try_new(&mut input.lease, scratch, self, name, value, indexing)?;
                field.push::<A>();
            }
            fixed::Omit::Discard(known) => self.discard::<A>(
                known,
                record::View::new(name, value).with_index(indexing),
                input.lease.as_slice(),
                scratch.authority(),
            ),
            fixed::Omit::Skip => {}
        }
        Ok(())
    }

    fn seal(
        mut self,
        input: lower::Input<storage::Fields>,
        scratch: &mut storage::Scratch,
    ) -> Result<Decoded<rules::Discard>, lower::Error> {
        let mut writer = input.try_seal()?;
        let sealed = fixed::Discard::seal(&mut self, &mut writer.lease);
        scratch.clear();
        let outcome = self.core.control.finish();
        Ok(Decoded::new(
            writer.finish(),
            (),
            sealed,
            outcome,
            self.core.control.framing(),
        ))
    }
}
