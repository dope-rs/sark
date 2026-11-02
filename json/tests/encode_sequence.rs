pub mod common;

use std::cell::Cell;

use common::sequence::{Borrowed, Bounded, Containers, Document, Rows, Slice, TryRow};
use sark_http::payload::response::dispatch::IntoOutput;
use sark_json::writing::{Body, EncodeError, TryEncode};

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

#[test]
fn bounded_skips_exact_size() {
    let sizes = Cell::new(0);
    let bounds = Cell::new(0);
    let writes = Cell::new(0);
    let response = Body::new(Bounded {
        sizes: &sizes,
        bounds: &bounds,
        writes: &writes,
    })
    .try_response()
    .map_err(|rejected| rejected.discard())
    .expect("upper bound");
    let (_, _, plan) = response.into_output().into_parts();
    let output = plan.into_shared().expect("bounded body");
    assert_eq!(output.as_ref(), b"true");
    assert_eq!((sizes.get(), bounds.get(), writes.get()), (0, 1, 1));
}
