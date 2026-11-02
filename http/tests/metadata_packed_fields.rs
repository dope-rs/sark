pub mod common;

use std::num::NonZero;

use common::field::{
    blocks::Decode as _,
    indexing::{self, Invalid as _},
};
use dope_test::alloc::Tracker;
use o3::buffer::{pool, write::Sink as _};
use sark_http::{
    field::{
        self, Source as _,
        metadata::{
            map::{
                self,
                record::{Entry, Span},
            },
            packed::{
                self,
                prefix::{self, Prefix},
            },
            section::storage,
        },
    },
    payload::response::headers::{
        self, Error, Fields, Name, Source,
        head::{Head, Template},
        set::Set,
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn generated_is_atomic() {
    let mut fields = map::Fields::new();
    (fields.push(b"x-stable", b"retained"))
        .as_ref()
        .expect("test operation must succeed");
    let stable = fields.as_bytes().to_vec();

    for written in 0..=1 {
        let error = fields.push_encoded(b"x-generated", 4, |writer| {
            writer.write_slice(&b"short"[..written])?;
            writer.write_slice(&b"short"[written..])
        });
        std::assert_matches!(
            error,
            Err(map::Error::Write(error))
                if error.into_parts() == (NonZero::new(5 - written).expect("nonempty write"), 4 - written)
        );
        assert_eq!(fields.as_bytes(), stable);
    }

    assert_eq!(
        fields.push_encoded(b"x-generated", 6, |writer| writer.write_slice(b"short")),
        Err(map::Error::ValueLengthMismatch {
            expected: 6,
            actual: 5,
        })
    );
    assert_eq!(fields.as_bytes(), stable);

    fields
        .push_encoded(b"x-generated", 5, |writer| {
            let error = writer.write_slice(b"longer").expect_err("rejected write");
            assert_eq!(error.into_parts(), (NonZero::new(6).expect("nonempty"), 5));
            writer.write_slice(b"exact")
        })
        .expect("callback recovered before publication");
    assert_eq!(fields.get(b"x-generated"), Some(b"exact".as_slice()));
}

#[test]
fn nested_keep_owners() {
    let mut first = map::Fields::new();
    first
        .push(b"x-stable", b"retained")
        .expect("existing field");
    let mut second = map::Fields::new();
    first
        .push_encoded(b"a", 1, |left| {
            second
                .push_encoded(b"other", 2, |right| {
                    left.write_slice(b"A")?;
                    right.write_slice(b"BC")
                })
                .expect("second field");
            Ok(())
        })
        .expect("first field");
    assert!(
        first
            .iter()
            .eq([Entry::new(b"x-stable", b"retained"), Entry::new(b"a", b"A"),])
    );
    assert!(second.iter().eq([Entry::new(b"other", b"BC")]));
}

#[test]
fn owners_share_format() {
    let source = [
        Entry::new(b"x-request-id", b"42"),
        Entry::new(b"trace-bin", &[0, 1, 2, 255]),
    ];
    let mut built = map::Fields::new();
    for field in source {
        (built.push(field.name, field.value))
            .as_ref()
            .expect("test operation must succeed");
    }
    let decoded = packed::Decoded::try_decode(&source).expect("metadata fixture");

    assert_eq!(built.as_bytes(), decoded.as_bytes());
    assert!(decoded.iter().collect::<Vec<_>>() == source);
    assert!(decoded == packed::Decoded::try_decode(&source).expect("same fields"));
    for (field, range) in decoded.ranges() {
        assert!(core::ptr::eq(field.value, &decoded.as_bytes()[range]));
    }
}

#[test]
fn span_projections() {
    let bytes = indexing::FIELDS[0].name;
    for split in 0..=bytes.len() {
        let span = Span::new(&bytes[..split], &bytes[split..]);
        for start in 0..=bytes.len() + 1 {
            for end in 0..=bytes.len() + 1 {
                let expected = bytes.get(start..end);
                let actual = span.slice(start..end);
                assert_eq!(
                    actual.map(|value| value.iter().copied().collect::<Vec<_>>()),
                    expected.map(<[u8]>::to_vec)
                );
                if let Some(value) = expected {
                    assert_eq!(span.starts_with(value), bytes.starts_with(value));
                    assert_eq!(span.ends_with(value), bytes.ends_with(value));
                }
            }
        }
        assert!(span.slice(0..usize::MAX).is_none());
        assert!(span.slice(usize::MAX..usize::MAX).is_none());
    }
}

#[test]
fn rejects_unknown_indexing() {
    let raw = pool::Span::try_invalid(NonZero::new(Prefix::LEN).expect("nonempty packed prefix"))
        .expect("metadata fixture");
    assert!(matches!(
        packed::Decoded::try_from(raw),
        Err(storage::Fault::Packed {
            offset: 0,
            cause: prefix::Error::Indexing { value: 2 },
        })
    ));
    let tagged = pool::Span::try_invalid(
        NonZero::new(storage::Tag::PREFIX_LEN).expect("nonempty tagged prefix"),
    )
    .expect("metadata fixture");
    assert!(matches!(
        storage::Finished::<storage::Fields>::try_from(tagged),
        Err(storage::Fault::Indexing {
            offset: 0,
            value: 2
        })
    ));
}

#[test]
fn retains_never_index() {
    let [entry, encoded] = indexing::FIELDS;
    let mut fields = map::Fields::new();
    fields.push_entry(entry).expect("entry");
    fields
        .push_with(encoded.name, encoded.value.len(), encoded.indexing, |out| {
            out.write_slice(encoded.value)
        })
        .expect("encoded value");
    let entries = fields.iter().collect::<Vec<_>>();
    let decoded = packed::Decoded::try_decode(&entries).expect("metadata fixture");
    let owned = decoded.try_to_owned().expect("owned metadata fixture");
    let entries = owned
        .iter()
        .map(|owned| {
            let entry = owned.as_ref();
            headers::Entry::new(
                Name::try_bytes(entry.name).expect("field name"),
                field::Value::try_bytes(entry.value).expect("field value"),
            )
            .with_index(entry.indexing)
        })
        .collect::<Vec<_>>();
    let template = Template::new(&entries);
    let mut output = map::Fields::new();
    let mut store = indexing::Store {
        fields: &mut output,
        result: Ok(()),
    };
    Fields::new(Head::new(&template, Set::<0>::new())).emit(&mut store);
    store.result.expect("stored template fields");
    assert!(output.iter().eq(indexing::FIELDS));
}

#[test]
fn failed_copy_keeps_source() {
    let decoded = packed::Decoded::try_decode(&indexing::FIELDS).expect("packed fields");
    for allowed in [1, 4] {
        let (result, usage) =
            Tracker::activity(|| Tracker::fail_after(allowed, || decoded.try_to_owned()));
        assert!(matches!(result, Err(o3::alloc::Error::Exhausted)));
        assert_eq!(usage.allocations, allowed);
        assert_eq!(usage.deallocations, allowed);
        assert_eq!(usage.allocated_bytes, usage.deallocated_bytes);
        assert!(usage.allocated_bytes >= std::mem::size_of::<map::Owned>());
        assert!(decoded.iter().eq(indexing::FIELDS));
    }
}

#[test]
fn group_admission() {
    for (result, error) in indexing::MISMATCHES {
        assert_eq!(result, Err(error));
    }
    assert_eq!(indexing::MANAGED, Err(Error::Managed));
    assert_eq!(
        indexing::PSEUDO,
        Err(Error::Syntax(field::Error::InvalidByte {
            index: 0,
            byte: b':',
        }))
    );
}

#[test]
fn group_keeps_indexing() {
    for project in [false, true] {
        let mut output = map::Fields::new();
        let mut store = indexing::Store {
            fields: &mut output,
            result: Ok(()),
        };
        indexing::EMPTY.emit(&mut store);
        if project {
            Fields::new(indexing::GROUP).emit(&mut store);
        } else {
            Source::emit(&indexing::GROUP, &mut store);
        }
        store.result.expect("stored group fields");
        assert!(output.iter().eq(indexing::FIELDS));
    }
}
