use std::mem;

use o3::buffer::hold;
use sark_http::owner::{Change, Effect, cause::Transport};

#[test]
fn projects_effect_facts() {
    let charge = hold::Charge::from_commit(11, 64, 1);
    let effect = Effect::new(7_u64, charge, Change::TransportReleased(Transport::Acked));
    assert_eq!(*effect.key(), 7);
    assert!(effect.charge() == charge);
    assert!((effect.change()) == (Change::TransportReleased(Transport::Acked)));
    assert!(
        effect.map_key(|key| key + 5)
            == Effect::new(12, charge, Change::TransportReleased(Transport::Acked),)
    );
    assert_eq!(mem::size_of::<Effect<u64>>(), 40);
    assert_eq!(mem::size_of::<Effect<[u8; 16]>>(), 48);
}
