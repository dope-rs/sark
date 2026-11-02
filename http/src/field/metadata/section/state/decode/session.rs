use o3::buffer::pool;

use crate::field::{
    self,
    metadata::{
        block, lower,
        section::{
            rules,
            state::decode::{Completion, Final, Indexed, Sink, Start},
            storage,
        },
        strings,
    },
};

/// Keeps one fresh metadata state, retained input and scratch owner together.
/// Fallible transitions borrow this owner without moving its storage.
/// Rejection preserves the exact progress and any terminal input rollback.
pub struct Session<P: rules::Policy> {
    section: P::State,
    input: lower::Input<P::Block>,
    scratch: storage::Scratch,
    pending: Pending<<P::State as Sink<P>>::Rule, <P::State as Sink<P>>::Direct>,
}

#[derive(Clone, Copy)]
enum Pending<R, D> {
    Ready,
    Buffered(R, block::Name),
    Direct(D),
}

impl<R, D> Pending<R, D> {
    fn try_ready(&self) -> Result<(), lower::Error> {
        match self {
            Self::Ready => Ok(()),
            Self::Buffered(_, _) | Self::Direct(_) => {
                Err(lower::Error::State(lower::State::Pending))
            }
        }
    }
}

impl<P: rules::Policy> Session<P> {
    pub fn try_new(
        lease: pool::Span,
        kind: rules::Kind,
        connect: block::Connect,
        mut scratch: storage::Scratch,
    ) -> Result<Self, (storage::Scratch, lower::Error)> {
        let writer = match storage::Writer::try_new(lease) {
            Ok(writer) => writer,
            Err(cause) => return Err((scratch, lower::Error::Capacity { cause })),
        };
        scratch.clear();
        Ok(Self {
            section: <P::State as Sink<P>>::h2(kind, connect),
            input: lower::Input::new(writer),
            scratch,
            pending: Pending::Ready,
        })
    }

    pub fn validate(&mut self, check: block::Validation) {
        rules::Validate::value(&mut self.section, check);
    }

    pub fn begin(
        &mut self,
        name: Option<block::Named<'_>>,
        indexing: field::Indexing,
    ) -> Result<Start<(), ()>, lower::Error> {
        self.pending.try_ready()?;
        let Some(name) = name else {
            self.input.try_ready()?;
            return Ok(Start::Skip);
        };
        let start =
            <P::State as Sink<P>>::begin(&mut self.section, &mut self.input, name, indexing)?;
        Ok(match start {
            Start::Scratch(done) => {
                self.pending = Pending::Direct(done);
                self.input.try_scratch(&mut self.scratch)?;
                Start::Direct(())
            }
            Start::Direct(done) => {
                self.pending = Pending::Direct(done);
                Start::Direct(())
            }
            Start::Buffered(rule) => {
                self.pending = Pending::Buffered(rule, name.proof());
                Start::Buffered(())
            }
            Start::Skip => Start::Skip,
        })
    }

    pub fn indexed(&mut self, name: Option<block::Named<'_>>) -> Result<Indexed<()>, lower::Error> {
        self.pending.try_ready()?;
        let Some(name) = name else {
            self.input.try_ready()?;
            return Ok(Indexed::Skip);
        };
        let indexed = <P::State as Sink<P>>::indexed(&mut self.section, &mut self.input, name)?;
        Ok(match indexed {
            Indexed::Skip => Indexed::Skip,
            Indexed::Buffered(rule) => {
                self.pending = Pending::Buffered(rule, name.proof());
                Indexed::Buffered(())
            }
        })
    }

    /// The first name selects policy; this name certifies the stored field's class.
    /// Regular names share grammar, while each pseudo-header has its own class.
    pub fn field<A: rules::Push>(
        &mut self,
        name: block::Named<'_>,
        value: block::Valued<'_>,
        indexing: field::Indexing,
    ) -> Result<(), lower::Error> {
        let Pending::Buffered(rule, expected) = self.pending else {
            let cause = lower::Error::State(match self.pending {
                Pending::Ready => lower::State::Ready,
                Pending::Buffered(_, _) | Pending::Direct(_) => lower::State::Pending,
            });
            return Err(cause);
        };
        let actual = name.proof();
        if actual != expected {
            let cause = lower::Error::Name {
                expected: expected.head(),
                actual: actual.head(),
            };
            return Err(cause);
        }
        rules::Validate::value(&mut self.section, value.validation());
        <P::State as Sink<P>>::field::<A>(
            &mut self.section,
            &mut self.input,
            &mut self.scratch,
            rule,
            name.span(),
            value.span(),
            indexing,
        )?;
        self.pending = Pending::Ready;
        Ok(())
    }

    /// Direct input already owns its class, tag and value; completion never observes a name.
    pub fn direct<A: rules::Push>(&mut self) -> Result<(), lower::Error> {
        let Pending::Direct(done) = self.pending else {
            let cause = lower::Error::State(match self.pending {
                Pending::Ready => lower::State::Ready,
                Pending::Buffered(_, _) | Pending::Direct(_) => lower::State::Pending,
            });
            return Err(cause);
        };
        done.try_finish::<A>(&mut self.section, &mut self.input, &mut self.scratch)?;
        self.pending = Pending::Ready;
        Ok(())
    }

    pub fn string(&mut self, cursor: strings::Cursor, input: &[u8]) -> lower::Step {
        self.input.string(&mut self.scratch, cursor, input)
    }

    pub fn rollback(&mut self) {
        self.input.rollback(&mut self.scratch);
        if self.pending.try_ready().is_err() {
            rules::Validate::value(&mut self.section, block::Validation::Malformed);
            self.pending = Pending::Ready;
        }
    }

    pub fn finish(self) -> (storage::Scratch, Result<Final<P>, lower::Error>) {
        if let Err(error) = self.pending.try_ready() {
            return (self.recycle(), Err(error));
        }
        let Self {
            section,
            input,
            mut scratch,
            ..
        } = self;
        let result = <P::State as Sink<P>>::seal(section, input, &mut scratch)
            .map(Final::new)
            .map_err(Into::into);
        (scratch, result)
    }

    pub fn recycle(self) -> storage::Scratch {
        let Self {
            input, mut scratch, ..
        } = self;
        input.recycle(&mut scratch);
        scratch
    }
}
