use std::{mem, ptr};

use o3::{
    depot,
    depot::{index::Initialized, map::direct, queue::indexed::keyed},
};
use sark_core::{
    body::{self, stream::Stream},
    decode::{request, state},
};

use crate::{
    server::{
        state::exchange,
        task::ingress::{
            Buffered, Occupied, Receiving, Removed, Vacancy,
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

/// Exclusive custody of a slot populated with one exact buffered request.
pub(in crate::server::task) struct BufferedSlot<'a, 'd, P, F, B>
where
    P: request::Prepared,
    F: state::Family<P>,
    B: body::Build<'d>,
{
    occupied: Occupied<'a, 'd, P, F, B>,
}

impl<'a, 'd, P, F, B> BufferedSlot<'a, 'd, P, F, B>
where
    P: request::Prepared,
    F: state::Family<P>,
    B: body::Build<'d>,
{
    pub(super) fn insert(vacant: Vacancy<'a, 'd, P, F, B>, buffered: Buffered<'d, P>) -> Self {
        Self {
            occupied: vacant.occupy(Entry::Buffered(buffered)),
        }
    }

    pub(in crate::server::task) fn take(
        self,
    ) -> <P::Family as state::Family<P>>::State<Receiving<'d>> {
        const { Prefix::<Buffered<'d, P>>::prove::<Entry<'d, P, F, B>>() };
        let entry = mem::ManuallyDrop::new(self.occupied.remove());
        // SAFETY: construction inserts exactly Buffered and retains exclusive
        // custody of that native occupied slot. No mutable view escapes before
        // this consuming removal. The repr(u8) enum has this checked prefix.
        let buffered = unsafe {
            ptr::from_ref(&*entry)
                .cast::<Prefix<Buffered<'d, P>>>()
                .read()
                .value
        };
        buffered.request
    }
}

pub(super) struct Projection<S>(pub(super) S);

pub(super) struct Borrowed<'a, 'slot, T, Q, P> {
    pub(super) slot: &'a direct::Vacant<'slot, stream::Key, T, Q>,
    pub(super) link: &'a keyed::Link<stream::Key, P>,
}

// SAFETY: ingress retains the exact native vacancy and its newly created or removed link together.
unsafe impl<'a, T, Q, P> depot::raw::KeyBorrow<'a, P> for Borrowed<'a, '_, T, Q, P> {
    type Key = stream::Key;

    fn parts(self) -> (stream::Key, &'a keyed::Link<stream::Key, P>) {
        (self.slot.key(), self.link)
    }
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

// SAFETY: the native admission plane retains this exact Routed value, bounded key, and link.
unsafe impl<'a, 'd, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>>
    depot::raw::KeyBorrow<'a, entry::Admission>
    for Projection<
        depot::raw::Proof<(&'a Entry<'d, P, F, B>, stream::Key), Initialized<entry::Admission>>,
    >
{
    type Key = stream::Key;

    fn parts(self) -> (stream::Key, &'a keyed::Link<stream::Key, entry::Admission>) {
        const { Variant::<exchange::Routed<'d, P, F>, entry::Admission>::prove::<Entry<'d, P, F, B>>() };
        let key = *self.0.key();
        // SAFETY: `repr(u8)` gives this `repr(C)` mirror the Routed field offsets.
        // The evaluated proof bounds the view within Entry, and the retained
        // admission plane selects exactly that variant for this borrow.
        let link = unsafe {
            &(*ptr::from_ref(*self.0.source())
                .cast::<Variant<exchange::Routed<'d, P, F>, entry::Admission>>())
            .link
        };
        (key, link)
    }
}

// SAFETY: the native build plane retains this exact Streaming value, bounded key, and link.
unsafe impl<'a, 'd, P: request::Prepared, F: state::Family<P>, B: body::Build<'d>>
    depot::raw::KeyBorrow<'a, entry::Build>
    for Projection<
        depot::raw::Proof<(&'a Entry<'d, P, F, B>, stream::Key), Initialized<entry::Build>>,
    >
{
    type Key = stream::Key;

    fn parts(self) -> (stream::Key, &'a keyed::Link<stream::Key, entry::Build>) {
        const { Variant::<Stream<'d, B>, entry::Build>::prove::<Entry<'d, P, F, B>>() };
        let key = *self.0.key();
        // SAFETY: `repr(u8)` gives this `repr(C)` mirror the Streaming field offsets.
        // The evaluated proof bounds the view within Entry, and the retained
        // build plane selects exactly that variant for this borrow.
        let link = unsafe {
            &(*ptr::from_ref(*self.0.source()).cast::<Variant<Stream<'d, B>, entry::Build>>()).link
        };
        (key, link)
    }
}

impl<'d, P: request::Prepared, F: state::Family<P>>
    Variant<exchange::Routed<'d, P, F>, entry::Admission>
{
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
