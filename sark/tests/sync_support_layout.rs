use std::mem;

pub mod common;

use std::cell::Cell;

use o3::buffer::{resident::bound, storage};
use sark_core::decode;

use crate::common::sync::{Router, State, StaticRouter};

#[test]
fn router_bounds_are_exact() {
    fn bound<'a, D: decode::Decode>(_: &'a D) -> bound::Limit {
        <D::Egress<'a> as bound::Bound>::LIMIT
    }

    let state = State {
        request_body: Cell::new(0),
        request_head: Cell::new(0),
        invalid_path: Cell::new(false),
        header_error: Cell::new(None),
        dispatches: Cell::new(0),
        large_body: storage::Shared::new(),
    };
    let fixed = StaticRouter::new();
    let mixed = Router::new(&state);
    assert_eq!(mem::size_of::<StaticRouter>(), 0);
    assert_eq!(mem::size_of_val(&mixed), mem::size_of::<usize>());
    assert!((bound(&fixed)) == (bound::Limit::AtMost(0)));
    assert!((bound(&mixed)) == (bound::Limit::Unbounded));
}
