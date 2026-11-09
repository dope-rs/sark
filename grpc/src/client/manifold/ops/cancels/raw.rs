use std::{cell, mem, ptr};

use dope::{core::driver::route::Identity, manifold::client::conn};
use crate::client::manifold::ops::cancels::{Call, Key, Phase, Queue};

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
    /// This exact node is the initialized head of its unchanged owner queue.
    pub(super) unsafe fn take(&self) -> (conn::Id<'d, ID>, Call<'d>) {
        let (conn, _) = unsafe { (*self.owner.get()).assume_init_read() };
        let call = unsafe { (*self.call.get()).assume_init_read() };
        self.phase.set(Some(Phase::Parked));
        (conn, call)
    }
}

impl<'d, ID: Identity> Queue<'d, ID> {
    /// The native call keys retain this exact fixed allocation's full usize extent.
    ///
    /// # Safety
    /// The key comes from this queue's State or its affine Registered token.
    pub(super) unsafe fn entry(&self, key: Key) -> &Entry<'d, ID> {
        // SAFETY: native registrations and affine Registered transitions retain keys
        // from this channel's State. Shared sizes this fixed Box from that same State's
        // immutable limits; no callback runs during a queue mutation.
        unsafe {
            self.entries
                .get_unchecked(key.index(self.offset, self.claims))
        }
    }
}
