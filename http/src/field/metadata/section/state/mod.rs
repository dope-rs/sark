use core::ops;

pub mod complete;
#[doc(hidden)]
pub mod decode;
pub mod fixed;
mod range;

use o3::buffer::pool;
use range::{Authority, Source, Stage};

use crate::{
    field::{
        self, head,
        metadata::{
            block, lower,
            map::record,
            packed,
            section::{self, rules, storage},
        },
    },
    uri,
};

#[derive(Debug)]
pub struct RetainedHead<B, S, C = block::Plan> {
    fields: B,
    pick: S,
    control: C,
    framing: Option<block::Framing>,
}

/// Preserves the exact policy that selected bytes from the retained owner.
#[repr(transparent)]
pub struct Selected<P: section::Policy> {
    value: P::Selection,
}

impl<P: section::Policy> Selected<P> {
    pub fn into_inner(self) -> P::Selection {
        self.value
    }
}

impl<P: section::Policy> AsRef<P::Selection> for Selected<P> {
    fn as_ref(&self) -> &P::Selection {
        &self.value
    }
}

impl<B, S, C> RetainedHead<B, S, C> {
    pub fn fields(&self) -> &B {
        &self.fields
    }

    pub fn control(&self) -> &C {
        &self.control
    }

    pub fn selection(&self) -> &S {
        &self.pick
    }

    /// Absent when a rejected section was returned through `Final::into_parts`.
    pub const fn framing(&self) -> Option<block::Framing> {
        self.framing
    }

    pub fn into_parts(self) -> (B, S, C) {
        (self.fields, self.pick, self.control)
    }

    fn bind<T>(self, control: T) -> RetainedHead<B, S, T> {
        RetainedHead {
            fields: self.fields,
            pick: self.pick,
            control,
            framing: self.framing,
        }
    }
}

impl<B, S> RetainedHead<B, S, Option<block::Plan>> {
    pub fn try_request(self) -> Result<RetainedHead<B, S>, Self> {
        let Some(control) = self.control else {
            return Err(self);
        };
        Ok(self.bind(control))
    }
}

impl<B, S> From<RetainedHead<B, S>> for RetainedHead<B, S, Option<block::Plan>> {
    fn from(head: RetainedHead<B, S>) -> Self {
        let control = Some(head.control);
        head.bind(control)
    }
}

impl<B, S> From<RetainedHead<B, S, block::Response>> for RetainedHead<B, S, Option<block::Plan>> {
    fn from(head: RetainedHead<B, S, block::Response>) -> Self {
        head.bind(None)
    }
}

impl<B, S> From<RetainedHead<B, S, ()>> for RetainedHead<B, S, Option<block::Plan>> {
    fn from(head: RetainedHead<B, S, ()>) -> Self {
        head.bind(None)
    }
}

/// The built-in policy admits every field before retaining this immutable owner;
/// a request plan exists only after both final verdicts are valid.
impl field::Source for RetainedHead<packed::Decoded, Selected<rules::RetainRaw>, block::Plan> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field>) {
        for field in self.fields.iter() {
            sink.entry(field::Entry(field));
        }
    }
}

#[doc(hidden)]
pub struct Decoder<P: section::Policy> {
    core: Core,
    policy: P,
}

struct Core {
    control: block::Gate,
    authority: Authority,
}

impl<P: section::Policy> rules::Validate for Decoder<P> {
    fn value(&mut self, check: block::Validation) {
        self.core.value(check);
    }
}

impl<P: section::Policy> Decoder<P> {
    pub fn new(kind: rules::Kind, policy: P) -> Self {
        Self::with(kind, block::Connect::Disabled, block::Version::Two, policy)
    }

    pub fn h3(kind: rules::Kind, connect: block::Connect, policy: P) -> Self {
        Self::with(kind, connect, block::Version::Three, policy)
    }

    pub fn finish(self) -> (P::Selection, block::Validation) {
        let status = self.core.control.finish().validation();
        (self.policy.finish(), status)
    }

    pub fn finish_plan(self) -> (P::Selection, block::Validation, Option<block::Plan>) {
        let (plan, status) = self.core.control.finish().into_parts();
        (self.policy.finish(), status, plan)
    }

    fn with(
        kind: rules::Kind,
        connect: block::Connect,
        version: block::Version,
        policy: P,
    ) -> Self {
        Self {
            core: Core::new(kind, connect, version),
            policy,
        }
    }
}

impl Core {
    fn value(&mut self, check: block::Validation) {
        match check {
            block::Validation::Valid => {}
            block::Validation::Malformed => self.control.reject(),
            block::Validation::RejectPush => self.control.reject_push(),
        }
    }

    fn new(kind: rules::Kind, connect: block::Connect, version: block::Version) -> Self {
        Self {
            control: block::Gate::new(
                match kind {
                    rules::Kind::Request => block::Kind::Request,
                    rules::Kind::Response => block::Kind::Response,
                    rules::Kind::RequestTrailers | rules::Kind::ResponseTrailers => {
                        block::Kind::Trailers
                    }
                    rules::Kind::PushPromise => block::Kind::PushPromise,
                },
                connect,
                version,
            ),
            authority: Authority::absent(),
        }
    }

    const fn is_checked(known: head::Name) -> bool {
        matches!(
            known,
            head::Name::Method
                | head::Name::Status
                | head::Name::Scheme
                | head::Name::Path
                | head::Name::Authority
                | head::Name::Protocol
                | head::Name::Host
                | head::Name::Te
                | head::Name::ContentLength
        )
    }

    fn is_te_list(value: &record::Span<'_>) -> bool {
        let mut matched = 0;
        for &byte in value.iter() {
            match byte {
                b',' if matched == 0 || matched == b"trailers".len() => matched = 0,
                b' ' | b'\t' if matched == 0 || matched == b"trailers".len() => {}
                _ if b"trailers"
                    .get(matched)
                    .is_some_and(|expected| expected.eq_ignore_ascii_case(&byte)) =>
                {
                    matched += 1;
                }
                _ => return false,
            }
        }
        matched == 0 || matched == b"trailers".len()
    }

    fn check_host(
        &mut self,
        value: record::Span<'_>,
        retained: &[u8],
        scratch: &[u8],
    ) -> block::Validation {
        let incoming = match uri::Parsed::parse(value) {
            Ok(value) => value,
            Err(_) => return block::Validation::Malformed,
        };
        if self.control.kind() != block::Kind::Request {
            return block::Validation::Valid;
        }
        let (saved, owner) = match self.authority.source() {
            Source::Absent => return block::Validation::Valid,
            Source::Retained(saved) => (saved, retained),
            Source::Staged(saved) => (saved, scratch),
        };
        let Some(value) = owner.get(saved.range()) else {
            return block::Validation::Malformed;
        };
        let saved = match uri::Authority::parse(value) {
            Ok(saved) => saved,
            Err(_) => return block::Validation::Malformed,
        };
        if saved.spanned().is_same(incoming, self.control.scheme()) {
            block::Validation::Valid
        } else {
            block::Validation::Malformed
        }
    }

    fn check<A: rules::Push>(
        &mut self,
        known: head::Name,
        field: record::View<'_>,
        retained: &[u8],
        scratch: &[u8],
    ) -> rules::Retention {
        if self.control.validation() != block::Validation::Valid {
            return rules::Retention::Discard;
        }
        let push = self.control.kind() == block::Kind::PushPromise;
        let mut status = if known == head::Name::Host {
            self.check_host(field.value, retained, scratch)
        } else {
            self.control.accept_value(known, field.value)
        };
        if status == block::Validation::Valid {
            status = match known {
                head::Name::Scheme if push && !A::is_scheme(field.value) => {
                    block::Validation::RejectPush
                }
                head::Name::Authority if push && !A::is_authority(field.value) => {
                    block::Validation::RejectPush
                }
                head::Name::ContentLength
                    if push && self.control.framing().content_length() != Some(0) =>
                {
                    block::Validation::RejectPush
                }
                head::Name::Te
                    if !field.value.is_equal(b"trailers") && !Self::is_te_list(&field.value) =>
                {
                    block::Validation::Malformed
                }
                head::Name::Invalid
                | head::Name::Method
                | head::Name::Scheme
                | head::Name::Path
                | head::Name::Status
                | head::Name::Authority
                | head::Name::Protocol
                | head::Name::Host
                | head::Name::Connection
                | head::Name::ContentLength
                | head::Name::TransferEncoding
                | head::Name::Expect
                | head::Name::AcceptEncoding
                | head::Name::Te
                | head::Name::KeepAlive
                | head::Name::ProxyConnection
                | head::Name::Upgrade
                | head::Name::Regular => block::Validation::Valid,
            };
        }
        self.value(status);
        match status {
            block::Validation::Valid => rules::Retention::Retain,
            block::Validation::Malformed | block::Validation::RejectPush => {
                rules::Retention::Discard
            }
        }
    }
}

impl<P: rules::Plan> Decoder<P> {
    pub fn seal(
        &mut self,
        retained: &mut pool::Span,
        scratch: &mut storage::Scratch,
    ) -> Result<block::Validation, lower::Error> {
        match self.core.authority.stage() {
            Stage::Absent | Stage::Retained => {}
            Stage::Staged(saved) => {
                let Some(value) = scratch.authority().get(saved.range()) else {
                    self.core.control.reject();
                    scratch.clear();
                    return Ok(self.core.control.validation());
                };
                let (plan, check) = self.core.control.finish().into_parts();
                if check == block::Validation::Valid
                    && plan.is_some_and(|plan| plan.form() == block::Form::Connect)
                {
                    let prefix = storage::Tag::AUTHORITY
                        .prefix_with(value.len(), scratch.indexing)
                        .ok_or(lower::Error::Prefix(storage::Tag::AUTHORITY, value.len()))?;
                    let start = retained.len();
                    let append = pool::Append::try_prefixed(retained, prefix, [value])
                        .map_err(lower::Error::Write)?;
                    let value_at = start + storage::Tag::PREFIX_LEN;
                    let (retained, _) = append.commit();
                    let value = value_at..retained.len();
                    let status = P::commit(rules::Commit::new(
                        &mut self.policy,
                        storage::Tag::AUTHORITY,
                        value.clone(),
                        retained.as_slice(),
                    ));
                    self.core.value(status);
                    match Authority::retained(value) {
                        Some(saved) => self.core.authority = saved,
                        None => self.core.control.reject(),
                    }
                } else {
                    self.core.authority = Authority::absent();
                }
                scratch.clear();
            }
        }
        Ok(self.core.control.validation())
    }
}

impl<P: rules::Plan> Decoder<P> {
    pub fn decide(
        &mut self,
        name: record::Span<'_>,
        proof: block::Name,
    ) -> rules::Decision<P::Inspect> {
        let known = proof.head();
        let mut control = self.core.control;
        let check = control.accept_name(known);
        let decision = self.decision(name, known, control, check);
        self.core.control = control;
        decision
    }

    fn decision(
        &mut self,
        name: record::Span<'_>,
        known: head::Name,
        control: block::Gate,
        check: block::Validation,
    ) -> rules::Decision<P::Inspect> {
        if check != block::Validation::Valid {
            return rules::Decision::Skip;
        }
        let action = P::disposition(rules::Candidate::new(&mut self.policy, name, known));
        match action {
            rules::Disposition::Discard
                if control.kind() == block::Kind::Request && known == head::Name::Authority =>
            {
                rules::Decision::Scratch
            }
            rules::Disposition::Discard if P::SKIP_DISCARDED && !Core::is_checked(known) => {
                rules::Decision::Skip
            }
            rules::Disposition::Discard => rules::Decision::Discard(known),
            rules::Disposition::Inspect(token) => rules::Decision::Inspect(token, known),
            rules::Disposition::Tagged(tag) => rules::Decision::Tagged(known, tag),
        }
    }

    pub fn discard<A: rules::Push>(
        &mut self,
        known: head::Name,
        field: record::View<'_>,
        retained: &[u8],
        scratch: &[u8],
    ) {
        self.core.check::<A>(known, field, retained, scratch);
    }

    pub fn inspect<A: rules::Push>(
        &mut self,
        token: P::Inspect,
        known: head::Name,
        field: record::View<'_>,
        retained: &[u8],
        scratch: &[u8],
    ) {
        if self.core.check::<A>(known, field, retained, scratch) == rules::Retention::Retain {
            let status = P::inspect(
                token,
                rules::Observation::new(&mut self.policy, field, retained),
            );
            self.core.value(status);
        }
    }

    pub fn retain<A: rules::Push>(
        &mut self,
        known: head::Name,
        field: record::View<'_>,
        retained: &[u8],
        scratch: &[u8],
    ) -> rules::Retention {
        self.core.check::<A>(known, field, retained, scratch)
    }

    pub fn store(
        &mut self,
        known: head::Name,
        store: rules::Store,
        range: ops::Range<usize>,
        retained: &[u8],
    ) {
        match store {
            rules::Store::Scratch => {
                if known == head::Name::Authority
                    && self.core.control.kind() == block::Kind::Request
                {
                    match Authority::staged(range) {
                        Some(saved) => self.core.authority = saved,
                        None => self.core.control.reject(),
                    }
                }
            }
            rules::Store::Tagged(tag) => {
                if known == head::Name::Authority
                    && self.core.control.kind() == block::Kind::Request
                {
                    match Authority::retained(range.clone()) {
                        Some(saved) => self.core.authority = saved,
                        None => self.core.control.reject(),
                    }
                }
                let status = P::commit(rules::Commit::new(&mut self.policy, tag, range, retained));
                self.core.value(status);
            }
        }
    }
}

const _: () = assert!(core::mem::size_of::<Core>() == 24);
