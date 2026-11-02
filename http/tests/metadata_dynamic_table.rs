pub mod common;

use std::num::NonZero;

use common::field::dynamics::{Acquire as _, Insert as _, Model as _, Random};
use dope_test::alloc::Tracker;
use sark_http::{
    field::{
        head,
        metadata::{block, map::record::Entry},
    },
    payload::compress::dynamic::{ENTRY_OVERHEAD, hash::Seed, mode, plan, table},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn plan_has_exact_bounds() {
    let result = plan::Plan::<mode::Staged>::new(4096);
    let plan = result.expect("test operation must succeed");
    assert_eq!(plan.capacity(), 4096);
    let staged = plan.footprint();
    assert_eq!(staged.arena_bytes(), 8_128);
    assert_eq!(staged.index_bytes(), 0);
    assert_eq!(staged.ledger_bytes(), 1_024);
    assert_eq!(staged.tail_bytes(), 9_152);
    assert_eq!(staged.resident(), 9_224);
    assert_eq!(staged.allocations(), 1);

    let result = plan::Plan::<mode::Staged>::new(0);
    let empty = result.expect("test operation must succeed");
    assert_eq!(empty.footprint().tail_bytes(), 0);
    assert_eq!(empty.footprint().resident(), 72);
    let mut table = (empty.try_table(0)).expect("test operation must succeed");
    let error = (table.try_begin())
        .err()
        .expect("empty dynamic table must reject an insertion");
    assert_eq!(error.required, ENTRY_OVERHEAD);
    assert_eq!(error.limit, 0);

    let result = plan::Plan::<mode::Direct>::new(4096);
    let direct = result.expect("test operation must succeed");
    let direct = direct.footprint();
    assert_eq!(direct.arena_bytes(), 4_064);
    assert_eq!(direct.index_bytes(), 1_088);
    assert_eq!(direct.ledger_bytes(), 3_072);
    assert_eq!(direct.tail_bytes(), 8_224);
    assert_eq!(direct.resident(), 8_328);
    assert_eq!(direct.allocations(), 1);
}

#[test]
fn index_keeps_load_limit() {
    let result = plan::Plan::<mode::Direct>::new(112 * ENTRY_OVERHEAD);
    let lower = result.expect("test operation must succeed").footprint();
    let result = plan::Plan::<mode::Direct>::new(113 * ENTRY_OVERHEAD);
    let upper = result.expect("test operation must succeed").footprint();
    assert_eq!(lower.index_bytes(), 576);
    assert_eq!(upper.index_bytes(), 1_088);
}

#[test]
fn table_matches_plan() {
    let result = plan::Plan::<mode::Direct>::new(4096);
    let plan = result.expect("test operation must succeed");
    let bounds = plan.footprint();
    let seed = Seed::try_acquire().expect("test entropy");
    let (table, usage) = Tracker::usage(|| {
        plan.try_table(plan.capacity(), seed)
            .expect("test operation must succeed")
    });
    assert_eq!(table.capacity(), 4096);
    assert_eq!(usage, (bounds.allocations(), bounds.resident()));
}

#[test]
fn ring_keeps_wrapping() {
    let result = plan::Plan::<mode::Direct>::new(128);
    let plan = result.expect("test operation must succeed");
    let mut table = (plan.try_table(plan.capacity(), Seed::try_acquire().expect("test entropy")))
        .expect("test operation must succeed");
    let value = b"0123456789ab";
    for _ in 0..6 {
        assert!(table.edit().insert(b"x-rings", value) == table::edit::Change::Inserted);
    }

    let field = (table.field(0)).expect("test value must be present");
    assert!(!field.name().span().parts().1.is_empty());
    assert!(field.name().span().is_equal(b"x-rings"));
    assert!(field.value().is_equal(value));
    assert_eq!(table.view().search().get(b"x-rings", value).exact, Some(0));
}

#[test]
fn seed_keeps_table_hashes() {
    let result = plan::Plan::<mode::Direct>::new(128);
    let plan = result.expect("test operation must succeed");
    let mut first = (plan.try_table(plan.capacity(), Seed::try_acquire().expect("test entropy")))
        .expect("test operation must succeed");
    let seed = first.seed().clone();
    let probe = first.probe(Entry::new(b"x-segment", b"payload"));
    assert_eq!(
        seed.probe(Entry::new(b"x-segment", b"payload"))
            .digest()
            .words(),
        probe.digest().words()
    );
    assert_eq!(
        first.seed().needle(b"x-segment").hash(),
        probe.digest().name()
    );

    assert!(first.edit().insert(b"x-segment", b"payload") == table::edit::Change::Inserted);
    assert_eq!(first.entry_count(), 1);
}

#[test]
fn staged_ring_commits() {
    let result = plan::Plan::<mode::Staged>::new(96);
    let plan = result.expect("test operation must succeed");
    let mut table = (plan.try_table(plan.capacity())).expect("test operation must succeed");
    let bytes = b"0123456789abcdefghijklm";
    let (_, usage) = Tracker::usage(|| {
        for _ in 0..5 {
            let name = table.try_begin();
            let mut name = name.expect("test operation must succeed");
            assert!(name.push(b"x-ring") == Ok(()));
            let value = name.finish();
            let mut value = value.ok().expect("test operation must succeed");
            assert!(value.push(bytes) == Ok(()));
            value.commit();
        }
    });
    assert_eq!(usage, (0, 0));

    let field = (table.field(0)).expect("test value must be present");
    assert!(!field.value().parts().1.is_empty());
    assert!(field.name().span().is_equal(b"x-ring"));
    assert!(field.value().is_equal(bytes));
}

#[test]
fn insert_evict_share_owner() {
    let result = plan::Plan::<mode::Staged>::new(96);
    let plan = result.expect("test operation must succeed");
    let mut table = (plan.try_table(plan.capacity())).expect("test operation must succeed");

    table.try_insert(b":method", b"GET").expect("dynamic entry");
    table.try_insert(b"x-one", b"1").expect("dynamic entry");
    assert_eq!(table.entry_count(), 2);
    let newest = (table.field(0)).expect("test value must be present");
    assert!(newest.name().span().is_equal(b"x-one"));
    assert!(newest.value().is_equal(b"1"));
    assert!((newest.name().proof()) == (block::Name::REGULAR));

    table
        .try_insert(b":path", b"/bounded")
        .expect("dynamic entry");
    assert!(table.current_size() <= table.max_size());
    let newest = (table.field(0)).expect("test value must be present");
    assert!(newest.name().span().is_equal(b":path"));
    assert!((newest.name().proof().head()) == (head::Name::Path));
    let kept = (table.field(1)).expect("test value must be present");
    assert!(kept.name().span().is_equal(b"x-one"));
    assert!(kept.value().is_equal(b"1"));
}

#[test]
fn refs_stay_in_arena() {
    let result = plan::Plan::<mode::Staged>::new(128);
    let plan = result.expect("test operation must succeed");
    let mut table = (plan.try_table(128)).expect("test operation must succeed");
    table
        .try_insert(b"content-type", b"application/json")
        .expect("dynamic entry");

    let name = table.try_begin();
    let name = name.expect("test operation must succeed");
    let value = name.copy(0);
    let mut value = value.ok().expect("test operation must succeed");
    assert!(value.push(b"text/plain") == Ok(()));
    value.commit();
    let field = (table.field(0)).expect("test value must be present");
    assert!(field.name().span().is_equal(b"content-type"));
    assert!(field.value().is_equal(b"text/plain"));

    assert!(table.edit().duplicate(0) == Ok(()));
    let copy = (table.field(0)).expect("test value must be present");
    assert!(copy.name().span().is_equal(b"content-type"));
    assert!(copy.value().is_equal(b"text/plain"));
}

#[test]
fn ring_matches_model() {
    for capacity in [0, 31, 32, 33, 63, 64, 95, 96, 127, 128, 255, 4096, 8192] {
        let result = plan::Plan::<mode::Direct>::new(capacity);
        let plan = result.expect("test operation must succeed");
        let mut table = (plan
            .try_table(plan.capacity(), Seed::try_acquire().expect("test entropy")))
        .expect("test operation must succeed");
        let mut model = Vec::<(Vec<u8>, Vec<u8>)>::new();
        for _ in 0..capacity / ENTRY_OVERHEAD {
            assert!(table.edit().insert(b"", b"") == table::edit::Change::Inserted);
            model.push((Vec::new(), Vec::new()));
        }
        assert_eq!(
            table.view().search().get(b"", b"").exact,
            (capacity >= ENTRY_OVERHEAD).then_some(0)
        );
        let mut limit = capacity;
        let mut random = Random::new(capacity as u64 + 1);
        for turn in 0..512 {
            if turn % 17 == 0 {
                limit = random.advance() % (capacity + 1);
                table.edit().set_limit(limit);
                model.trim(limit, 0);
            } else {
                let name = random.bytes(NonZero::new(23).expect("name width"));
                let value = random.bytes(NonZero::new(47).expect("value width"));
                let incoming = ENTRY_OVERHEAD + name.len() + value.len();
                let outcome = table.edit().insert(&name, &value);
                if incoming > limit {
                    assert!(
                        outcome
                            == table::edit::Change::Cleared(table::edit::Error::Capacity {
                                required: incoming,
                                limit,
                            })
                    );
                    model.clear();
                } else {
                    assert!(outcome == table::edit::Change::Inserted);
                    model.trim(limit, incoming);
                    model.insert(0, (name, value));
                }
            }
            assert_eq!(table.entry_count(), model.len());
            assert_eq!(table.current_size(), model.size());
            assert_eq!(table.max_size(), limit);
            for (index, (name, value)) in model.iter().enumerate() {
                let field = (table.field(index)).expect("test value must be present");
                assert!(field.name().span().is_equal(name));
                assert!(field.value().is_equal(value));
                let exact = model
                    .iter()
                    .position(|item| item.0 == *name && item.1 == *value);
                let named = model.iter().position(|item| item.0 == *name);
                assert_eq!(table.view().search().get(name, value).exact, exact);
                assert_eq!(table.view().search().get(name, b"absent").name, named);
            }
        }
    }
}

#[test]
fn reserved_ring_is_stable() {
    let result = plan::Plan::<mode::Direct>::new(4096);
    let plan = result.expect("test operation must succeed");
    let mut table = (plan.try_table(plan.capacity(), Seed::try_acquire().expect("test entropy")))
        .expect("test operation must succeed");
    assert!(table.edit().insert(b"x-steady", b"value") == table::edit::Change::Inserted);
    let (_, usage) = Tracker::usage(|| {
        for turn in 0u64..1024 {
            let value = turn.to_le_bytes();
            assert!(table.edit().insert(b"x-churn", &value) == table::edit::Change::Inserted);
            assert_eq!(table.view().search().get(b"x-churn", &value).exact, Some(0));
        }
    });
    assert_eq!(usage, (0, 0));
}
