pub mod common;

use std::cell::Cell;

use common::sequence::{Borrowed, Containers, Document, Rows, Slice, TryRow};
use sark_json::writing::{EncodeError, TryEncode};

#[test]
fn encodes_owner_views() {
    let name = String::from("borrowed\"name");
    let names = [name.as_str()];
    let value = Document::<Rows<'_>> {
        rows: Rows {
            next: Cell::new(&names),
            second: &names,
        },
    };
    assert_eq!(
        value.try_encode().expect("borrowed owner views"),
        br#"{"rows":[{"name":"borrowed\"name"}]}"#,
    );
}

#[test]
fn borrowed_containers() {
    let name = String::from("a");
    let vector = vec![TryRow {
        name: sark_json::Text::from(name.as_str()),
    }];
    let value = Containers {
        array: [TryRow {
            name: sark_json::Text::from(name.as_str()),
        }],
        vector: vec![TryRow {
            name: sark_json::Text::from(name.as_str()),
        }],
        borrowed: &vector,
    };
    assert_eq!(
        value.try_encode().expect("reference items"),
        br#"{"array":[{"name":"a"}],"vector":[{"name":"a"}],"borrowed":[{"name":"a"}]}"#,
    );
    assert_eq!(
        Slice { rows: &vector }.try_encode().expect("generic slice"),
        br#"{"rows":[{"name":"a"}]}"#,
    );

    let names = [name.as_str()];
    let rows = Rows {
        next: Cell::new(&names),
        second: &names,
    };
    assert_eq!(
        Borrowed { rows: &rows }
            .try_encode()
            .expect("borrowed owner"),
        br#"{"rows":[{"name":"a"}]}"#,
    );
}

#[test]
fn rejects_changed_sequence() {
    let names = ["a", "b"];
    for (first, second) in [(1, 2), (2, 1)] {
        let value = Document::<Rows<'_>> {
            rows: Rows {
                next: Cell::new(&names[..first]),
                second: &names[..second],
            },
        };
        let mut output = b"prefix".to_vec();
        let error = value.try_append(&mut output).expect_err("changed sequence");
        assert!(matches!(
            (first < second, error),
            (true, EncodeError::Capacity(_)) | (false, EncodeError::Length(_))
        ));
        assert_eq!(output, b"prefix");
    }
}
