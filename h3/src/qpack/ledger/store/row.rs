use core::ptr;

use o3::{
    alloc::tail,
    depot::{self, capacity},
};

use crate::qpack::ledger::store::{
    Store,
    layout::{Count, Node, Queue},
};

/// A row retains the exact allocation whose physical range was established at
/// admission, initialization, or by a native occupied/link projection.
pub(in crate::qpack::ledger) struct Row<S> {
    owner: S,
    index: u32,
}

impl<S> Row<S> {
    pub(in crate::qpack::ledger) fn new(
        proof: depot::raw::Proof<(S, u32), capacity::Physical>,
    ) -> Self {
        let (owner, index) = proof.into_inner();
        Self { owner, index }
    }
}

impl Row<&Store> {
    /// Layout bounds every plane's end by u32::MAX; the retained physical
    /// row proof bounds this element in that exact allocation.
    fn offset(&self, base: capacity::Count, width: u32) -> usize {
        const { assert!(usize::BITS >= u32::BITS) };
        (u64::from(base.raw()) + u64::from(self.index) * u64::from(width)) as usize
    }

    fn read<T: Copy>(&self, start: usize) -> T {
        let bytes = tail::View::new(&self.owner.bytes).as_slice().as_ptr();
        // SAFETY: only this row's plane-specific leaves call read; the retained
        // physical proof bounds start and T in this exact allocation.
        unsafe { ptr::read_unaligned(bytes.add(start).cast()) }
    }

    pub(in crate::qpack::ledger) fn node(&self, plane: Node) -> u32 {
        self.read(self.offset(self.owner.layout().node(plane), 4))
    }

    pub(in crate::qpack::ledger) fn heap(&self) -> u32 {
        self.read(self.offset(self.owner.layout().heap(), 4))
    }

    pub(in crate::qpack::ledger) fn count(&self, plane: Count) -> u64 {
        self.read(self.offset(self.owner.layout().count(plane), 8))
    }

    pub(in crate::qpack::ledger) fn queue(&self, plane: Queue) -> u32 {
        self.read(self.offset(self.owner.layout().queue_start(plane), 4))
    }

    pub(in crate::qpack::ledger) fn stream_id(&self) -> u64 {
        self.read(self.offset(self.owner.layout().stream_id, 8))
    }
}

impl Row<&mut Store> {
    fn write<T: Copy>(&mut self, start: usize, value: T) {
        let bytes = self.owner.bytes.as_mut_slice().as_mut_ptr();
        // SAFETY: the physical row proof retains exclusive plane access and
        // each plane-specific leaf supplies its element's exact offset and type.
        unsafe { ptr::write_unaligned(bytes.add(start).cast(), value) }
    }

    pub(in crate::qpack::ledger) fn view(&self) -> Row<&Store> {
        Row {
            owner: self.owner,
            index: self.index,
        }
    }

    pub(in crate::qpack::ledger) fn node(&mut self, plane: Node, value: u32) {
        let start = self.view().offset(self.owner.layout().node(plane), 4);
        self.write(start, value);
    }

    pub(in crate::qpack::ledger) fn heap(&mut self, value: u32) {
        let start = self.view().offset(self.owner.layout().heap(), 4);
        self.write(start, value);
    }

    pub(in crate::qpack::ledger) fn count(&mut self, plane: Count, value: u64) {
        let start = self.view().offset(self.owner.layout().count(plane), 8);
        self.write(start, value);
    }

    pub(in crate::qpack::ledger) fn queue(&mut self, plane: Queue, value: u32) {
        let start = self
            .view()
            .offset(self.owner.layout().queue_start(plane), 4);
        self.write(start, value);
    }

    pub(in crate::qpack::ledger) fn stream_id(&mut self, value: u64) {
        let start = self.view().offset(self.owner.layout().stream_id, 8);
        self.write(start, value);
    }
}
