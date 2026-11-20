use std::mem;

pub mod common;

use o3::buffer::{
    resident::bound::{Bound, Limit},
    storage,
};
use sark_core::decode::Decode;

use crate::common::sync::{Router, State, StaticRouter};

#[test]
fn router_bounds_are_exact() {
    fn bound<'a, D: Decode>(_: &'a D) -> Limit<<D::Egress<'a> as Bound>::Finite> {
        <D::Egress<'a> as Bound>::LIMIT
    }

    let state = State::new(storage::Shared::new());
    let fixed = StaticRouter::new();
    let mixed = Router::new(&state);
    assert_eq!(mem::size_of::<StaticRouter>(), 0);
    assert_eq!(mem::size_of_val(&mixed), mem::size_of::<usize>());
    assert!(matches!(bound(&fixed), Limit::AtMost(0, _)));
    assert!(matches!(bound(&mixed), Limit::Unbounded));
}
