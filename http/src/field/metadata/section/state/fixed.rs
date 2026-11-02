use core::ops;

use o3::buffer::pool;
use state::range::Authority;

use crate::field::{
    head,
    metadata::{
        block,
        map::record,
        section::{rules, state},
    },
};

pub struct Raw {
    pub(super) core: state::Core,
}

pub struct Discard {
    pub(super) core: state::Core,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum Omit {
    Authority,
    Discard(head::Name),
    Skip,
}

impl Raw {
    pub fn new(kind: rules::Kind) -> Self {
        Self {
            core: state::Core::new(kind, block::Connect::Disabled, block::Version::Two),
        }
    }

    pub fn finish(self) -> ((), block::Validation) {
        let status = self.core.control.finish().validation();
        ((), status)
    }

    pub fn finish_plan(self) -> ((), block::Validation, Option<block::Plan>) {
        let (plan, status) = self.core.control.finish().into_parts();
        ((), status, plan)
    }

    #[doc(hidden)]
    pub fn accept(&mut self, proof: block::Name) -> block::Validation {
        self.core.control.accept_name(proof.head())
    }

    #[doc(hidden)]
    pub fn retain<A: rules::Push>(
        &mut self,
        known: head::Name,
        field: record::View<'_>,
        retained: &[u8],
        scratch: &[u8],
    ) -> rules::Retention {
        self.core.check::<A>(known, field, retained, scratch)
    }

    #[doc(hidden)]
    pub fn store(&mut self, known: head::Name, range: ops::Range<usize>) {
        if known == head::Name::Authority && self.core.control.kind() == block::Kind::Request {
            match Authority::retained(range) {
                Some(saved) => self.core.authority = saved,
                None => self.core.control.reject(),
            }
        }
    }
}

impl Discard {
    pub fn finish(self) -> ((), block::Validation) {
        let status = self.core.control.finish().validation();
        ((), status)
    }

    pub fn finish_plan(self) -> ((), block::Validation, Option<block::Plan>) {
        let (plan, status) = self.core.control.finish().into_parts();
        ((), status, plan)
    }

    #[doc(hidden)]
    pub fn route(&self, proof: block::Name) -> Omit {
        let known = proof.head();
        if self.core.control.kind() == block::Kind::Request && known == head::Name::Authority {
            Omit::Authority
        } else if state::Core::is_checked(known) {
            Omit::Discard(known)
        } else {
            Omit::Skip
        }
    }

    #[doc(hidden)]
    pub fn accept(&mut self, proof: block::Name) -> block::Validation {
        self.core.control.accept_name(proof.head())
    }

    #[doc(hidden)]
    pub fn discard<A: rules::Push>(
        &mut self,
        known: head::Name,
        field: record::View<'_>,
        retained: &[u8],
        scratch: &[u8],
    ) {
        self.core.check::<A>(known, field, retained, scratch);
    }

    #[doc(hidden)]
    pub fn retain<A: rules::Push>(
        &mut self,
        known: head::Name,
        field: record::View<'_>,
        retained: &[u8],
        scratch: &[u8],
    ) -> rules::Retention {
        self.core.check::<A>(known, field, retained, scratch)
    }

    #[doc(hidden)]
    pub fn store(&mut self, range: ops::Range<usize>) {
        match Authority::retained(range) {
            Some(saved) => self.core.authority = saved,
            None => self.core.control.reject(),
        }
    }

    pub fn seal(&mut self, retained: &mut pool::Span) -> block::Validation {
        let (plan, check) = self.core.control.finish().into_parts();
        if check != block::Validation::Valid
            || plan.is_none_or(|plan| plan.form() != block::Form::Connect)
        {
            retained.truncate(0);
            self.core.authority = Authority::absent();
        }
        check
    }
}

const _: () = assert!(core::mem::size_of::<Raw>() == 24);
const _: () = assert!(core::mem::size_of::<Discard>() == 24);

impl rules::Validate for Raw {
    fn value(&mut self, check: block::Validation) {
        self.core.value(check);
    }
}

impl rules::Validate for Discard {
    fn value(&mut self, check: block::Validation) {
        self.core.value(check);
    }
}
