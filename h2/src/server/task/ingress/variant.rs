use std::{mem, ptr};

use o3::{
    depot,
    depot::{index::Initialized, queue::indexed::keyed},
};
use sark_core::{
    body::{self, stream::Stream},
    decode::{request, state},
};

use crate::{
    server::{
        state::exchange,
        task::ingress::{
            Occupied, Removed, Vacancy,
            entry::{self, Entry},
        },
    },
    stream,
};

#[repr(C)]
pub(super) struct Variant<T, Q> {
    tag: u8,
    value: T,
    link: keyed::Link<stream::Key, Q>,
}

#[repr(C)]
struct Prefix<T> {
    tag: u8,
    value: T,
}

impl<T, Q> Variant<T, Q> {
    /// Checks this `repr(C)` mirror within `E`, a `repr(u8)` enum with these fields.
    /// Its per-variant tag places the payload at `T`'s alignment.
    /// The retained plane must select this variant.
    const fn prove<E>() {
        Prefix::<T>::prove::<E>();
        assert!(mem::offset_of!(Self, value) == mem::offset_of!(Prefix<T>, value));
        assert!(mem::size_of::<Self>() <= mem::size_of::<E>());
        assert!(mem::align_of::<Self>() <= mem::align_of::<E>());
    }
}

impl<T> Prefix<T> {
    const fn prove<E>() {
        assert!(mem::offset_of!(Self, tag) == 0);
        assert!(mem::offset_of!(Self, value) == mem::align_of::<T>());
        assert!(mem::size_of::<Self>() <= mem::size_of::<E>());
        assert!(mem::align_of::<Self>() <= mem::align_of::<E>());
    }
}

impl<'d, P: request::Prepared, F: state::Family<P>>
    Variant<exchange::Routed<'d, P, F>, entry::Admission>
{
    pub(super) fn link<'a, B: body::Build<'d>>(
        live: depot::raw::Proof<
            (&'a Entry<'d, P, F, B>, stream::Key),
            Initialized<entry::Admission>,
        >,
    ) -> &'a keyed::Link<stream::Key, entry::Admission> {
        const { Self::prove::<Entry<'d, P, F, B>>() };
        // SAFETY: `repr(u8)` gives this `repr(C)` mirror the Routed field offsets.
        // The evaluated proof bounds the view within Entry, and the retained
        // admission plane selects exactly that variant for this borrow.
        unsafe { &(*ptr::from_ref(*live.source()).cast::<Self>()).link }
    }

    pub(super) fn take<'a, B: body::Build<'d>>(
        occupied: Occupied<'a, 'd, P, F, B, entry::Admission>,
    ) -> Removed<
        Vacancy<'a, 'd, P, F, B, entry::Admission>,
        exchange::Routed<'d, P, F>,
        entry::Admission,
    > {
        const { Self::prove::<Entry<'d, P, F, B>>() };
        let (vacant, value) = occupied.take();
        let value = mem::ManuallyDrop::new(value);
        // SAFETY: `repr(u8)` gives this mirror the exact Routed field layout.
        // The evaluated proof bounds its read, and native admission selects
        // that variant, moving its payload and link out of the enum once.
        let Self { value, link, .. } = unsafe { ptr::from_ref(&*value).cast::<Self>().read() };
        (vacant, value, link)
    }
}

impl<'d, B: body::Build<'d>> Variant<Stream<'d, B>, entry::Build> {
    pub(super) fn link<'a, P: request::Prepared, F: state::Family<P>>(
        live: depot::raw::Proof<
            (&'a Entry<'d, P, F, B>, stream::Key),
            Initialized<entry::Build>,
        >,
    ) -> &'a keyed::Link<stream::Key, entry::Build> {
        const { Self::prove::<Entry<'d, P, F, B>>() };
        // SAFETY: `repr(u8)` gives this `repr(C)` mirror the Streaming field offsets.
        // The evaluated proof bounds the view within Entry, and the retained
        // build plane selects exactly that variant for this borrow.
        unsafe { &(*ptr::from_ref(*live.source()).cast::<Self>()).link }
    }

    pub(super) fn take<'a, P: request::Prepared, F: state::Family<P>>(
        occupied: Occupied<'a, 'd, P, F, B, entry::Build>,
    ) -> Removed<Vacancy<'a, 'd, P, F, B, entry::Build>, Stream<'d, B>, entry::Build> {
        const { Self::prove::<Entry<'d, P, F, B>>() };
        let (vacant, value) = occupied.take();
        let value = mem::ManuallyDrop::new(value);
        // SAFETY: `repr(u8)` gives this mirror the exact Streaming field layout.
        // The evaluated proof bounds its read, and the native build plane selects
        // that variant, moving its payload and link out of the enum once.
        let Self { value, link, .. } = unsafe { ptr::from_ref(&*value).cast::<Self>().read() };
        (vacant, value, link)
    }
}
