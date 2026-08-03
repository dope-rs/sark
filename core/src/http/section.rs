use std::ops::Range;

use o3::buffer::Pooled;

use super::{DecodedFieldBlock, Field};
pub type KnownHeadName = sark_protocol::KnownRequestHeadName;

const METHOD: u8 = 1 << 0;
const SCHEME: u8 = 1 << 1;
const PATH: u8 = 1 << 2;
const STATUS: u8 = 1 << 3;
const REQUEST_REQUIRED: u8 = METHOD | SCHEME | PATH;

/// Compile-time identity stored instead of a repeated HTTP field name.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeadTag(u16);

impl HeadTag {
    pub const PATH: Self = Self(0);
    pub const CONTENT_LENGTH: Self = Self(1);
    const USER_BASE: u16 = 2;

    pub const fn user(slot: u16) -> Option<Self> {
        match slot.checked_add(Self::USER_BASE) {
            Some(tag) => Some(Self(tag)),
            None => None,
        }
    }

    pub const fn user_slot(self) -> Option<u16> {
        self.0.checked_sub(Self::USER_BASE)
    }

    pub fn prefix(self, value_len: usize) -> Option<[u8; 6]> {
        let Ok(value_len) = u32::try_from(value_len) else {
            return None;
        };
        let tag = self.0.to_ne_bytes();
        let len = value_len.to_ne_bytes();
        Some([tag[0], tag[1], len[0], len[1], len[2], len[3]])
    }
}

/// Compact retained representation used by generated HTTP/2 and HTTP/3 plans.
/// Each entry is `[tag: u16][value_len: u32][value]` in native byte order.
#[doc(hidden)]
pub struct PlannedFields {
    storage: Pooled,
}

impl PlannedFields {
    pub fn as_bytes(&self) -> &[u8] {
        self.storage.as_ref()
    }

    pub fn iter_from(&self, offset: usize) -> PlannedFieldIter<'_> {
        PlannedFieldIter {
            bytes: self.storage.as_ref().get(offset..).unwrap_or_default(),
            offset,
        }
    }
}

/// Contiguous request-head storage carried after wire-specific decoding.
#[doc(hidden)]
pub struct HeadBytes {
    storage: Pooled,
}

impl HeadBytes {
    pub fn as_bytes(&self) -> &[u8] {
        self.storage.as_ref()
    }
}

pub struct PlannedFieldIter<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl Iterator for PlannedFieldIter<'_> {
    type Item = (HeadTag, Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        let prefix: [u8; 6] = self.bytes.get(..6)?.try_into().ok()?;
        let tag = HeadTag(u16::from_ne_bytes([prefix[0], prefix[1]]));
        let value_len = u32::from_ne_bytes(prefix[2..].try_into().ok()?) as usize;
        let value_start = self.offset.checked_add(6)?;
        let value_end = value_start.checked_add(value_len)?;
        self.bytes = self.bytes.get(6 + value_len..)?;
        self.offset = value_end;
        Some((tag, value_start..value_end))
    }
}

/// Pooled output selected by a monomorphized request-head plan.
#[doc(hidden)]
pub trait HeadBlock: Sized {
    const TAGGED: bool;

    fn from_pooled(storage: Pooled) -> Self;
    fn into_bytes(self) -> HeadBytes;
}

impl HeadBlock for DecodedFieldBlock {
    const TAGGED: bool = false;

    fn from_pooled(storage: Pooled) -> Self {
        Self::from_pooled(storage)
    }

    fn into_bytes(self) -> HeadBytes {
        HeadBytes {
            storage: self.into_primary(),
        }
    }
}

impl HeadBlock for PlannedFields {
    const TAGGED: bool = true;

    fn from_pooled(storage: Pooled) -> Self {
        Self { storage }
    }

    fn into_bytes(self) -> HeadBytes {
        HeadBytes {
            storage: self.storage,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeadDisposition {
    Discard,
    #[doc(hidden)]
    Skip,
    Raw,
    Tagged(HeadTag),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub struct HeadDecision {
    pub disposition: HeadDisposition,
    pub name: KnownHeadName,
}

/// Static retention policy for decoded HTTP/2 and HTTP/3 fields.
/// Wire decoders still decompress and validate every field.
pub trait HeadPlan: Default {
    type Selection;
    type Block: HeadBlock;
    const INSPECT_DISCARDED: bool = true;

    /// Decides where a field belongs from its decoded name alone.
    ///
    /// Keeping this decision independent of the value lets compressed protocols
    /// choose the value's final sink before decompressing it.
    fn disposition(
        &mut self,
        name: &[u8],
        known: KnownHeadName,
        retained: &[u8],
    ) -> HeadDisposition;

    /// Observes a fully decoded, valid field before it is committed.
    fn decoded(&mut self, _field: Field<'_>, _known: KnownHeadName, _retained: &[u8]) {}

    /// Records the stable range assigned to a retained value.
    fn committed(
        &mut self,
        _disposition: HeadDisposition,
        _value_range: Range<usize>,
        _retained: &[u8],
    ) {
    }

    fn finish(self) -> Self::Selection;
}

/// Protocol-facing lowering contract for a validated request head.
#[doc(hidden)]
pub trait HeadConsumer {
    const TAGGED: bool;

    fn disposition(
        &mut self,
        name: &[u8],
        known: Option<KnownHeadName>,
        retained: &[u8],
    ) -> HeadDecision;

    fn decoded(&mut self, decision: HeadDecision, field: Field<'_>, retained: &[u8]) -> bool;

    fn committed(
        &mut self,
        disposition: HeadDisposition,
        value_range: Range<usize>,
        retained: &[u8],
    );
}

#[derive(Clone, Copy, Default)]
pub struct RawHeadPlan;

impl HeadPlan for RawHeadPlan {
    type Selection = ();
    type Block = DecodedFieldBlock;

    fn disposition(
        &mut self,
        _name: &[u8],
        _known: KnownHeadName,
        _retained: &[u8],
    ) -> HeadDisposition {
        HeadDisposition::Raw
    }

    fn finish(self) {}
}

pub struct PlannedHead<P: HeadPlan> {
    fields: P::Block,
    selection: P::Selection,
}

impl<P: HeadPlan> PlannedHead<P> {
    #[doc(hidden)]
    pub const fn new(fields: P::Block, selection: P::Selection) -> Self {
        Self { fields, selection }
    }

    #[doc(hidden)]
    pub fn into_parts(self) -> (P::Block, P::Selection) {
        (self.fields, self.selection)
    }
}

/// Typed HTTP/2 and HTTP/3 field-section state.
#[doc(hidden)]
pub struct HeadSection<P: HeadPlan> {
    seen: u8,
    request: bool,
    saw_regular: bool,
    trailing: bool,
    invalid: bool,
    policy: P,
}

impl<P: HeadPlan> HeadSection<P> {
    pub fn new(request: bool, trailing: bool) -> Self {
        Self {
            seen: 0,
            request,
            saw_regular: false,
            trailing,
            invalid: false,
            policy: P::default(),
        }
    }

    fn accept_name(&mut self, name: KnownHeadName) -> bool {
        if self.invalid {
            return false;
        }
        match name {
            KnownHeadName::Invalid => self.reject(),
            KnownHeadName::Regular | KnownHeadName::ContentLength | KnownHeadName::Te => {
                self.saw_regular = true;
                true
            }
            _ if self.saw_regular || self.trailing => self.reject(),
            KnownHeadName::Method if self.request => self.mark(METHOD),
            KnownHeadName::Scheme if self.request => self.mark(SCHEME),
            KnownHeadName::Path if self.request => self.mark(PATH),
            KnownHeadName::Authority | KnownHeadName::Protocol if self.request => true,
            KnownHeadName::Status if !self.request => self.mark(STATUS),
            _ => self.reject(),
        }
    }

    pub fn finish(self) -> (P::Selection, bool) {
        let valid = if self.invalid {
            false
        } else if self.trailing {
            true
        } else if self.request {
            self.seen & REQUEST_REQUIRED == REQUEST_REQUIRED
        } else {
            self.seen & STATUS != 0
        };
        (self.policy.finish(), valid)
    }

    fn mark(&mut self, bit: u8) -> bool {
        if self.seen & bit != 0 {
            return self.reject();
        }
        self.seen |= bit;
        true
    }

    fn reject(&mut self) -> bool {
        self.invalid = true;
        false
    }
}

impl<P: HeadPlan> HeadConsumer for HeadSection<P> {
    const TAGGED: bool = P::Block::TAGGED;

    fn disposition(
        &mut self,
        name: &[u8],
        known: Option<KnownHeadName>,
        retained: &[u8],
    ) -> HeadDecision {
        let known = known.unwrap_or_else(|| KnownHeadName::classify_compressed(name));
        let mut disposition = if self.accept_name(known) {
            self.policy.disposition(name, known, retained)
        } else {
            HeadDisposition::Discard
        };
        if disposition == HeadDisposition::Discard
            && !P::INSPECT_DISCARDED
            && !matches!(
                known,
                KnownHeadName::Method
                    | KnownHeadName::Scheme
                    | KnownHeadName::Path
                    | KnownHeadName::Te
            )
        {
            disposition = HeadDisposition::Skip;
        }
        HeadDecision {
            disposition,
            name: known,
        }
    }

    fn decoded(&mut self, decision: HeadDecision, field: Field<'_>, retained: &[u8]) -> bool {
        if self.invalid {
            return false;
        }
        let valid = match decision.name {
            KnownHeadName::Method | KnownHeadName::Scheme | KnownHeadName::Path => {
                !field.value.is_empty()
            }
            KnownHeadName::Te => field.value == b"trailers",
            _ => true,
        };
        if valid {
            self.policy.decoded(field, decision.name, retained);
        } else {
            self.reject();
        }
        valid
    }

    fn committed(
        &mut self,
        disposition: HeadDisposition,
        value_range: Range<usize>,
        retained: &[u8],
    ) {
        self.policy.committed(disposition, value_range, retained);
    }
}
