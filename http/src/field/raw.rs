use std::str;

use crate::field::{Entry, Lower, Name, Value, metadata::map::record, storage};

pub(in crate::field) fn name(name: Name<&[u8]>) -> &str {
    // SAFETY: a Name admits only ASCII HTTP token bytes; its immutable borrow
    // preserves that proof and therefore UTF-8 validity for this exact range.
    unsafe { str::from_utf8_unchecked(name.get()) }
}

/// Restores admission retained by an immutable owner across packed storage.
pub trait Admit<T>: Sized {
    /// # Safety
    /// The exact immutable input must retain every invariant of the target proof.
    unsafe fn from_raw(owner: T) -> Self;
}

impl<T: storage::Bytes> Admit<T> for Name<T> {
    /// # Safety
    /// These exact immutable bytes must already prove a nonempty HTTP token.
    unsafe fn from_raw(owner: T) -> Self {
        Self(owner)
    }
}

impl<T: storage::Bytes> Admit<T> for Lower<T> {
    /// # Safety
    /// These exact immutable bytes must already prove a nonempty lowercase HTTP token.
    unsafe fn from_raw(owner: T) -> Self {
        Self(owner)
    }
}

impl<T: storage::Bytes> Admit<T> for Value<T> {
    /// # Safety
    /// These exact immutable bytes must satisfy `Error::is_text`; binary or layout admission is insufficient.
    unsafe fn from_raw(owner: T) -> Self {
        Self(owner)
    }
}

impl<'a> Admit<record::Entry<'a>> for Entry<'a> {
    /// # Safety
    /// This immutable record must prove a lowercase token or supported pseudo-name and HTTP value bytes.
    /// Packed geometry and binary metadata alone are insufficient.
    unsafe fn from_raw(owner: record::Entry<'a>) -> Self {
        Self(owner)
    }
}
