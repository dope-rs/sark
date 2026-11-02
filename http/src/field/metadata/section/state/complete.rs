use lower::plain;
use o3::buffer::pool;
use state::{
    decode::{Decoded, Final, Indexed},
    fixed,
};

use crate::field::{
    self, head,
    metadata::{
        block, lower,
        map::record,
        section::{self, rules, state, storage},
    },
};

/// Holds the decoder and the retained bytes under one exclusive scratch loan.
pub struct Decoder<'a, P: rules::Policy> {
    state: P::State,
    writer: storage::Writer<P::Block>,
    scratch: &'a mut storage::Scratch,
    pending: bool,
}

/// One field decision that must be completed before its producer can finish.
pub enum Selection<'field, 'scratch, P: rules::Policy> {
    Skipped(Skipped<'field, 'scratch, P>),
    Buffered(Selected<'field, 'scratch, P>),
}

/// A skipped field still requires its decoded value validation.
pub struct Skipped<'field, 'scratch, P: rules::Policy> {
    decoder: &'field mut Decoder<'scratch, P>,
}

/// Retains the selected name and rule under the decoder's exclusive borrow.
pub struct Selected<'field, 'scratch, P: rules::Policy> {
    decoder: &'field mut Decoder<'scratch, P>,
    name: block::Named<'field>,
    rule: <P::State as Sink<P>>::Rule,
}

impl<'a, P: rules::Policy> Decoder<'a, P> {
    pub fn try_new(
        lease: pool::Span,
        scratch: &'a mut storage::Scratch,
        kind: rules::Kind,
        connect: block::Connect,
    ) -> Result<Self, lower::Error> {
        let writer =
            storage::Writer::try_new(lease).map_err(|cause| lower::Error::Capacity { cause })?;
        scratch.clear();
        Ok(Self {
            state: <P::State as Sink<P>>::h3(kind, connect),
            writer,
            scratch,
            pending: false,
        })
    }

    pub fn select<'field>(
        &'field mut self,
        name: block::Named<'field>,
    ) -> Selection<'field, 'a, P> {
        self.reject();
        self.pending = true;
        match <P::State as Sink<P>>::select(&mut self.state, name) {
            Indexed::Skip => Selection::Skipped(Skipped { decoder: self }),
            Indexed::Buffered(rule) => Selection::Buffered(Selected {
                decoder: self,
                name,
                rule,
            }),
        }
    }

    pub fn finish(mut self) -> Result<Final<P>, lower::Error> {
        self.reject();
        <P::State as Sink<P>>::finish(self.state, self.writer, self.scratch)
            .map(Final::new)
            .map_err(Into::into)
    }

    fn reject(&mut self) {
        if self.pending {
            rules::Validate::value(&mut self.state, block::Validation::Malformed);
        }
    }
}

impl<P: rules::Policy> Skipped<'_, '_, P> {
    pub fn validate(self, check: block::Validation) {
        rules::Validate::value(&mut self.decoder.state, check);
        self.decoder.pending = false;
    }
}

impl<P: rules::Policy> Selected<'_, '_, P> {
    pub fn push<A: rules::Push>(
        self,
        value: block::Valued<'_>,
        indexing: field::Indexing,
    ) -> Result<(), <P::State as Sink<P>>::Rejected> {
        rules::Validate::value(&mut self.decoder.state, value.validation());
        <P::State as Sink<P>>::push::<A>(
            &mut self.decoder.state,
            &mut self.decoder.writer,
            self.decoder.scratch,
            self.rule,
            self.name.span(),
            value.span(),
            indexing,
        )?;
        self.decoder.pending = false;
        Ok(())
    }
}

/// Complete fields preserve their selected rule without incremental staging.
pub trait Sink<P: section::Policy>: rules::Validate + Sized {
    type Error: Into<lower::Error>;
    type Rejected: Into<lower::Error>;
    type Rule: Copy;

    fn h3(kind: rules::Kind, connect: block::Connect) -> Self;
    fn select(&mut self, name: block::Named<'_>) -> Indexed<Self::Rule>;
    fn push<A: rules::Push>(
        &mut self,
        writer: &mut storage::Writer<P::Block>,
        scratch: &mut storage::Scratch,
        rule: Self::Rule,
        name: record::Span<'_>,
        value: record::Span<'_>,
        indexing: field::Indexing,
    ) -> Result<(), Self::Rejected>;
    fn finish(
        self,
        writer: storage::Writer<P::Block>,
        scratch: &mut storage::Scratch,
    ) -> Result<Decoded<P>, Self::Error>;
}

impl<P: rules::Plan> Sink<P> for state::Decoder<P> {
    type Error = lower::Error;
    type Rejected = lower::Error;
    type Rule = rules::Decision<P::Inspect>;

    fn h3(kind: rules::Kind, connect: block::Connect) -> Self {
        Self {
            core: state::Core::new(kind, connect, block::Version::Three),
            policy: P::default(),
        }
    }

    fn select(&mut self, name: block::Named<'_>) -> Indexed<Self::Rule> {
        match self.decide(name.span(), name.proof()) {
            rules::Decision::Skip => Indexed::Skip,
            rule => Indexed::Buffered(rule),
        }
    }

    fn push<A: rules::Push>(
        &mut self,
        writer: &mut storage::Writer<P::Block>,
        scratch: &mut storage::Scratch,
        rule: Self::Rule,
        name: record::Span<'_>,
        value: record::Span<'_>,
        indexing: field::Indexing,
    ) -> Result<(), Self::Rejected> {
        let push = lower::Lower::new(&mut writer.lease, scratch, self)
            .try_push_as(rule, name, value, indexing)?;
        push.commit::<A>();
        Ok(())
    }

    fn finish(
        mut self,
        mut writer: storage::Writer<P::Block>,
        scratch: &mut storage::Scratch,
    ) -> Result<Decoded<P>, lower::Error> {
        let sealed = self.seal(&mut writer.lease, scratch)?;
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
    type Rejected = plain::Error;
    type Rule = head::Name;

    fn h3(kind: rules::Kind, connect: block::Connect) -> Self {
        Self {
            core: state::Core::new(kind, connect, block::Version::Three),
        }
    }

    fn select(&mut self, name: block::Named<'_>) -> Indexed<Self::Rule> {
        match self.accept(name.proof()) {
            block::Validation::Valid => Indexed::Buffered(name.proof().head()),
            block::Validation::Malformed | block::Validation::RejectPush => Indexed::Skip,
        }
    }

    fn push<A: rules::Push>(
        &mut self,
        writer: &mut storage::Writer<P::Block>,
        scratch: &mut storage::Scratch,
        known: Self::Rule,
        name: record::Span<'_>,
        value: record::Span<'_>,
        indexing: field::Indexing,
    ) -> Result<(), Self::Rejected> {
        plain::Plain::try_new(&mut writer.lease, name, value, indexing)?
            .push_raw::<A>(scratch, self, known);
        Ok(())
    }

    fn finish(
        self,
        writer: storage::Writer<P::Block>,
        scratch: &mut storage::Scratch,
    ) -> Result<Decoded<P>, lower::Error> {
        scratch.clear();
        let outcome = self.core.control.finish();
        let sealed = outcome.validation();
        Ok(Decoded::new(
            writer.finish(),
            (),
            sealed,
            outcome,
            self.core.control.framing(),
        ))
    }
}

impl Sink<rules::Discard> for fixed::Discard {
    type Error = lower::Error;
    type Rejected = lower::Error;
    type Rule = fixed::Omit;

    fn h3(kind: rules::Kind, connect: block::Connect) -> Self {
        Self {
            core: state::Core::new(kind, connect, block::Version::Three),
        }
    }

    fn select(&mut self, name: block::Named<'_>) -> Indexed<Self::Rule> {
        if self.accept(name.proof()) != block::Validation::Valid {
            return Indexed::Skip;
        }
        match self.route(name.proof()) {
            fixed::Omit::Skip => Indexed::Skip,
            rule => Indexed::Buffered(rule),
        }
    }

    fn push<A: rules::Push>(
        &mut self,
        writer: &mut storage::Writer<storage::Fields>,
        scratch: &mut storage::Scratch,
        rule: Self::Rule,
        name: record::Span<'_>,
        value: record::Span<'_>,
        indexing: field::Indexing,
    ) -> Result<(), Self::Rejected> {
        use lower::fixed::Authority;
        match rule {
            fixed::Omit::Authority => {
                Authority::try_new(&mut writer.lease, scratch, self, name, value, indexing)?
                    .push::<A>()
            }
            fixed::Omit::Discard(known) => self.discard::<A>(
                known,
                record::View::new(name, value).with_index(indexing),
                writer.lease.as_slice(),
                scratch.authority(),
            ),
            fixed::Omit::Skip => {}
        }
        Ok(())
    }

    fn finish(
        mut self,
        mut writer: storage::Writer<storage::Fields>,
        scratch: &mut storage::Scratch,
    ) -> Result<Decoded<rules::Discard>, lower::Error> {
        let sealed = self.seal(&mut writer.lease);
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
