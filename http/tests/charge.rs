use dope_test::alloc::Tracker;
use handoff::{Project as _, input::Input};
use o3::buffer::{Residency as _, hold, resident, storage};
use sark_http::{
    owner::handoff,
    payload::{owned, response::Body as _},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn charges_retained() {
    let result = storage::Shared::try_copy(b"abc");
    let first = result.expect("the retained fixture must allocate");
    let first = resident::Shared::from(first);
    let resident = first.resident();
    let pointer = first.as_slice().as_ptr();
    let result = first.try_clone();
    let second = result.expect("the second edge must retain the same backing");
    let borrowed = resident::Shared::from(b"tail".as_slice());
    let tail = borrowed.as_slice().as_ptr();
    let mut buffer = owned::Segments::from(first);
    for owner in [second, borrowed] {
        let result = buffer.try_extend(owned::Segments::from(owner));
        let next = result.expect("the mixed body must preserve every segment");
        buffer = next;
    }
    let owner = buffer;
    let expected = hold::Charge {
        logical_bytes: 6,
        resident_bytes: 2 * resident,
        owners: 2,
    };
    assert!(owner.charge() == expected);
    let (ready, charge) = Input::with(owner, |input| {
        let (pending, owner) = input.segmented((), |(), request, slot| {
            let plan = request
                .try_resolve(&slot)
                .expect("the complete body must resolve to its original owner");
            let (result, activity) = Tracker::measure(|| plan.try_detach());
            assert_eq!(activity, [0; 4]);
            result.expect("the committed body charge must remain representable")
        });
        let (result, activity) = Tracker::measure(|| pending.finish(owner));
        assert_eq!(activity, [0; 4]);
        result
    });
    assert!(charge == expected);
    assert!(matches!(ready, handoff::Ready::Linked(_)));
    let handoff::Ready::Linked(owner) = ready else {
        return;
    };
    assert_eq!(owner.len(), 10);
    assert_eq!(owner.count().into_raw(), 3);
    assert!(
        owner
            .segments()
            .map(|part| part.as_ptr())
            .eq([pointer, pointer, tail,])
    );
    assert!(owner.segments().eq([b"abc".as_slice(), b"abc", b"tail"]));
}
