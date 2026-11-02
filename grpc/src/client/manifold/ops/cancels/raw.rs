use std::{cell, mem, ptr};

use dope::{core::driver::route::Identity, manifold::client::conn};
use sark_h2::stream;

use crate::client::{
    call::bidi,
    manifold::ops::cancels::{Call, Key, Phase},
};

/// Stable native cancellation storage; queued links retain both initialized fields.
pub(super) struct Entry<'d, ID: Identity> {
    owner: cell::UnsafeCell<mem::MaybeUninit<(conn::Id<'d, ID>, Key)>>,
    call: cell::UnsafeCell<mem::MaybeUninit<Call<'d>>>,
    pub(super) next: cell::Cell<Option<ptr::NonNull<Self>>>,
    phase: cell::Cell<Option<Phase>>,
}

impl<'d, ID: Identity> Entry<'d, ID> {
    pub(super) fn new() -> Self {
        Self {
            owner: cell::UnsafeCell::new(mem::MaybeUninit::uninit()),
            call: cell::UnsafeCell::new(mem::MaybeUninit::uninit()),
            next: cell::Cell::new(None),
            phase: cell::Cell::new(None),
        }
    }

    pub(super) fn owner(&self) -> Option<((conn::Id<'d, ID>, Key), Phase)> {
        let phase = self.phase.get()?;
        // SAFETY: registration initializes the complete identity before
        // publishing either occupied phase; no mutation calls out.
        Some((unsafe { (*self.owner.get()).assume_init_read() }, phase))
    }

    /// # Safety
    /// The native empty-queue capability excludes every queued payload.
    pub(super) unsafe fn register(&self, conn: conn::Id<'d, ID>, key: Key) {
        unsafe { (*self.owner.get()).write((conn, key)) };
        self.phase.set(Some(Phase::Parked));
    }

    /// # Safety
    /// The caller retains this exact registered Parked position's sole enqueue permission.
    pub(super) unsafe fn enqueue(&self, call: Call<'d>) {
        unsafe { (*self.call.get()).write(call) };
        self.phase.set(Some(Phase::Queued));
    }

    /// # Safety
    /// This is the same stream's initialized queued reset, retained by the other split token.
    pub(super) unsafe fn pair(&self, send: bidi::Send<'d, stream::Key>) {
        unsafe { (*self.call.get()).write(Call::ResetPair(send)) };
    }

    /// # Safety
    /// This exact node is the initialized head of its unchanged owner queue.
    pub(super) unsafe fn take(&self) -> (conn::Id<'d, ID>, Call<'d>) {
        let (conn, _) = unsafe { (*self.owner.get()).assume_init_read() };
        let call = unsafe { (*self.call.get()).assume_init_read() };
        self.phase.set(Some(Phase::Parked));
        (conn, call)
    }
}

/// The native call keys retain this exact fixed allocation's full usize extent.
/// # Safety
/// Every admitted native key resolves to its original initialized resident node.
pub(super) unsafe trait Source<'d, ID: Identity> {
    /// # Safety
    /// The key comes from this queue's State or its affine Registered token.
    unsafe fn entry(&self, key: Key) -> &Entry<'d, ID>;
}
