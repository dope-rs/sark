pub mod common;

use common::field::dynamics::{Acquire as _, INVALID, Insert as _, NAME, SPACE, VALID};
use o3::buffer::write::Sink;
use sark_http::{
    field::metadata::{block, map::record::Entry},
    payload::compress::dynamic::{
        hash::Seed,
        insert::stages::{self, Write},
        mode, plan,
        table::{
            self,
            delta::{self, buffer::Storage},
        },
    },
};

#[test]
fn delta_searches_future() {
    let result = plan::Plan::<mode::Direct>::new(68);
    let plan = result.expect("test operation must succeed");
    let mut base = (plan.try_table(plan.capacity(), Seed::try_acquire().expect("test entropy")))
        .expect("test operation must succeed");
    assert!(base.edit().insert(b"a", b"1") == table::edit::Change::Inserted);
    assert!(base.edit().insert(b"b", b"2") == table::edit::Change::Inserted);
    let mut storage = (Storage::try_new(68, 2)).expect("test operation must succeed");
    let mut draft = delta::Draft::new(delta::Borrow::new(&mut base, &mut storage));

    for (name, value) in [(b"c".as_slice(), b"3".as_slice()), (b"d", b"4")] {
        let mut query = draft.query(Entry::new(name, value));
        assert!(query.try_insert() == Ok(()));
    }
    for (name, value, relative) in [(b"d", b"4", 0), (b"c", b"3", 1)] {
        let query = draft.query(Entry::new(name, value));
        assert_eq!(query.exact_index(), Some(relative));
    }
    {
        let query = draft.query(Entry::new(b"b", b"2"));
        assert_eq!(query.exact_index(), None);
    }
    let mut query = draft.query(Entry::new(b"e", b"5"));
    assert!(query.try_insert() == Err(delta::Failure::Entries { len: 2, limit: 2 }));

    draft.finish().apply();
    assert!(
        (base.field(0))
            .expect("test value must be present")
            .name()
            .span()
            .is_equal(b"d")
    );
    assert!(
        (base.field(1))
            .expect("test value must be present")
            .name()
            .span()
            .is_equal(b"c")
    );
}

#[test]
fn reuses_rollback_indices() {
    let plan = plan::Plan::<mode::Direct>::new(128).expect("table plan");
    let mut table = plan
        .try_table(128, Seed::try_acquire().expect("test entropy"))
        .expect("table storage");
    let mut pending = Storage::try_new(128, 2).expect("journal storage");
    let mut draft = delta::Draft::new(delta::Borrow::new(&mut table, &mut pending));
    assert!(
        draft
            .query(Entry::new(b"x-old", b"one"))
            .try_insert()
            .is_ok()
    );
    draft.rollback();

    delta::Draft::new(delta::Borrow::new(&mut table, &mut pending)).rollback();
    let mut draft = delta::Draft::new(delta::Borrow::new(&mut table, &mut pending));
    assert_eq!(
        draft.query(Entry::new(b"x-old", b"one")).exact_index(),
        None
    );
    assert!(
        draft
            .query(Entry::new(b"x-new", b"two"))
            .try_insert()
            .is_ok()
    );
    draft.finish().apply();

    let mut draft = delta::Draft::new(delta::Borrow::new(&mut table, &mut pending));
    assert_eq!(
        draft.query(Entry::new(b"x-new", b"two")).exact_index(),
        Some(0)
    );
    draft.rollback();
}

#[test]
fn rejects_before_publish() {
    let plan = plan::Plan::<mode::Direct>::new(128).expect("table plan");
    let mut table = plan
        .try_table(128, Seed::try_acquire().expect("test entropy"))
        .expect("table");
    let mut pending = Storage::try_new(2, 2).expect("journal");
    let mut draft = delta::Draft::new(delta::Borrow::new(&mut table, &mut pending));
    assert_eq!(
        draft.query(Entry::new(b"a", &[0; 97])).try_insert(),
        Err(delta::Failure::Capacity {
            required: 130,
            limit: 128
        })
    );
    assert_eq!(
        draft.query(Entry::new(b"a", b"bc")).try_insert(),
        Err(delta::Failure::Bytes {
            required: 3,
            limit: 2
        })
    );
    assert_eq!(draft.query(Entry::new(b"a", b"bc")).exact_index(), None);
    assert!(draft.query(Entry::new(b"a", b"b")).try_insert().is_ok());
    draft.finish().apply();
    assert_eq!(table.entry_count(), 1);
    assert!(
        table
            .field(0)
            .expect("committed field")
            .value()
            .is_equal(b"b")
    );
}

#[test]
fn preserves_validation() {
    let plan = plan::Plan::<mode::Direct>::new(128).expect("plan");
    let mut table = plan
        .try_table(128, Seed::try_acquire().expect("test entropy"))
        .expect("table");
    table.edit().insert(NAME, INVALID);
    assert_eq!(
        table.field(0).expect("entry").validation().validation(),
        block::Validation::Malformed
    );

    let mut pending = Storage::try_new(128, 2).expect("journal");
    let mut draft = delta::Draft::new(delta::Borrow::new(&mut table, &mut pending));
    assert!(draft.query(Entry::new(NAME, VALID)).try_insert().is_ok());
    assert!(draft.query(Entry::new(NAME, INVALID)).try_insert().is_ok());
    draft.finish().apply();
    assert_eq!(
        table.field(0).expect("entry").validation().validation(),
        block::Validation::Malformed
    );
    assert_eq!(
        table.field(1).expect("entry").validation().validation(),
        block::Validation::Valid
    );
}

#[test]
fn revalidates_retained() -> Result<(), &'static str> {
    let plan = plan::Plan::<mode::Staged>::new(128).expect("plan");
    let ready = stages::Ready::new(plan.try_table(128).expect("table"));
    let stages::Begin::Name(mut name) = ready.begin() else {
        return Err("table must admit an insertion name");
    };
    assert!(name.push(NAME).is_ok());
    let mut value = name.finish().ok().expect("value");
    assert!(value.push(INVALID).is_ok());
    assert_eq!(value.validation(), block::Validation::Malformed);
    assert!(value.truncate(NAME.len() + VALID.len()).is_ok());
    assert_eq!(value.validation(), block::Validation::Valid);
    assert!(value.push(SPACE).is_ok());
    assert_eq!(value.validation(), block::Validation::Malformed);
    assert!(value.push(VALID).is_ok());
    assert_eq!(value.validation(), block::Validation::Valid);
    let mut ready = value.commit();
    assert_eq!(
        ready.field(0).expect("entry").validation().validation(),
        block::Validation::Valid
    );
    assert!(ready.edit().duplicate(0).is_ok());
    assert_eq!(
        ready.field(0).expect("entry").validation().validation(),
        block::Validation::Valid
    );
    Ok(())
}

#[test]
fn validates_copied_name() {
    let plan = plan::Plan::<mode::Staged>::new(128).expect("plan");
    let mut table = plan.try_table(128).expect("table");
    table.try_insert(NAME, INVALID).expect("dynamic entry");
    assert!(table.edit().duplicate(0).is_ok());
    assert_eq!(
        table.field(0).expect("entry").validation().validation(),
        block::Validation::Malformed
    );
    let name = table.try_begin().expect("name");
    let mut value = name.copy(0).ok().expect("copy");
    assert!(value.push(VALID).is_ok());
    value.commit();
    assert_eq!(
        table.field(0).expect("entry").validation().validation(),
        block::Validation::Valid
    );
}

#[test]
fn preserves_name_classes() {
    let plan = plan::Plan::<mode::Staged>::new(128).expect("plan");
    let mut table = plan.try_table(128).expect("table");
    for compact in 0..18 {
        let proof = block::Name::from_compact(compact).expect("name proof");
        let head = proof.head();
        let wire = if proof == block::Name::REGULAR {
            NAME
        } else {
            head.wire()
        };
        for bytes in [VALID, INVALID] {
            let proof = block::Name::classify(wire);
            table.try_insert(wire, bytes).expect("dynamic entry");
            let field = table.field(0).expect("entry");
            assert!(field.name().proof() == proof);
            let expected = if bytes == INVALID || proof.is_forbidden() {
                block::Validation::Malformed
            } else {
                block::Validation::Valid
            };
            assert_eq!(field.validation().validation(), expected);
        }
    }
}

#[test]
fn rollback_keeps_name_scan() {
    let plan = plan::Plan::<mode::Staged>::new(128).expect("plan");
    let mut table = plan.try_table(128).expect("table");
    let mut name = table.try_begin().expect("name");
    name.push(b"con").expect("prefix");
    let mut writer = name.writer();
    writer.write_slice(b"-other").expect("discarded bytes");
    writer.rollback();
    name.push(b"nection").expect("suffix");
    name.finish().ok().expect("value").commit();
    let field = table.field(0).expect("entry");
    assert!(field.name().span().is_equal(b"connection"));
    assert_eq!(
        field.validation().validation(),
        block::Validation::Malformed
    );
}
