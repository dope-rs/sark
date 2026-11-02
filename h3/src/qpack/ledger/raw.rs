use core::cmp;

use o3::depot::{self, heap::hole};

use crate::qpack::ledger::store::{Insert, Occupied, Resident, Retired, Section, Store};

/// The exclusive ledger owner retained throughout one native heap transition.
pub(in crate::qpack::ledger) struct Heap<'a> {
    store: &'a mut Store,
}

impl<'a> Heap<'a> {
    pub(in crate::qpack::ledger) fn push(insert: Insert<'a>) -> Occupied<'a> {
        use hole::raw::Source as _;

        let Insert { owner, section, .. } = insert;
        // SAFETY: Insert retains this exact absent section and the exclusive
        // store whose admitted free slot bounds the next heap position.
        unsafe { Heap { store: &mut *owner }.vacancy(section) }.store();
        Occupied { owner, section }
    }

    pub(in crate::qpack::ledger) fn remove(resident: Resident<'a>) -> Retired<'a> {
        use hole::raw::Source as _;

        let Resident {
            owner,
            section,
            position,
            ..
        } = resident;
        // SAFETY: Resident retains this exact section's initialized heap
        // position and reciprocal reverse map under the original owner loan.
        let heap = unsafe { Heap { store: owner }.entry(position.raw() as usize, ()) }.clear();
        Retired {
            owner: heap.store,
            section,
        }
    }
}

// SAFETY: every operation retains the same exclusive Store and its stable
// SoA layout. The engine alone exposes initialized positions and holes;
// shifts and publication update the matching section's reverse position.
unsafe impl hole::raw::Source for Heap<'_> {
    type Value = Section;

    fn len(&self) -> u32 {
        self.store.header().heap_len
    }

    unsafe fn len_mut(&mut self) -> &mut u32 {
        &mut self.store.header_mut().heap_len
    }

    fn compare(entry: hole::At<&Self, &Self::Value>) -> cmp::Ordering {
        let (owner, position, value) = entry.into_inner();
        // SAFETY: At retains this initialized heap member and the replacement
        // section in the same exclusively held store and ordering domain.
        let other = unsafe { Store::heap_read(depot::raw::Proof::new((&*owner.store, position))) };
        // SAFETY: both identities come from this At's retained ordering domain.
        unsafe { compare(owner.store, *value, other) }
    }

    fn compare_at(left: hole::At<&Self>, right: hole::At<&Self>) -> cmp::Ordering {
        let (first, left, ()) = left.into_inner();
        let (second, right, ()) = right.into_inner();
        // SAFETY: the engine projects both initialized positions from the
        // same retained source, without mutation between these observations.
        let left = unsafe { Store::heap_read(depot::raw::Proof::new((&*first.store, left))) };
        let right = unsafe { Store::heap_read(depot::raw::Proof::new((&*second.store, right))) };
        // SAFETY: both At projections retain the same initialized ordering domain.
        unsafe { compare(first.store, left, right) }
    }

    fn read<const REMOVE: bool>(entry: hole::At<&mut Self>) -> Self::Value {
        let (owner, position, ()) = entry.into_inner();
        // SAFETY: At transfers this exact initialized position into the
        // engine's held replacement or terminal removal state once.
        unsafe {
            Store::heap_clear::<REMOVE>(depot::raw::Proof::new((&mut *owner.store, position)))
        }
    }

    fn shift(step: hole::Step<'_, Self>) {
        let (owner, source, target) = step.into_inner();
        // SAFETY: Step retains the initialized source and exact destination
        // hole in this unchanged store; the moved section stays live.
        let section = unsafe { Store::heap_read(depot::raw::Proof::new((&*owner.store, source))) };
        unsafe { Store::heap_write(depot::raw::Proof::new((&mut *owner.store, target, section))) };
    }

    fn write<const PLACE: bool>(finish: hole::Finish<Self>) -> Self {
        let (owner, position, section) = finish.into_inner();
        // SAFETY: Finish transfers the held live section into this store's
        // final hole. Publication restores its reciprocal reverse position.
        unsafe {
            Store::heap_write(depot::raw::Proof::new((
                &mut *owner.store,
                position,
                section,
            )))
        };
        owner
    }
}

/// # Safety
/// Both sections remain live in this owner's retained heap ordering domain.
unsafe fn compare(owner: &Store, left: Section, right: Section) -> cmp::Ordering {
    // SAFETY: callers retain both sections through the heap engine's
    // initialized-position or held-replacement proofs in this exact owner.
    let left = unsafe { Store::heap_counts(depot::raw::Proof::new((owner, left))) };
    let right = unsafe { Store::heap_counts(depot::raw::Proof::new((owner, right))) };
    (left.minimum(), left.required()).cmp(&(right.minimum(), right.required()))
}
