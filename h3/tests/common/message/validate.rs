use dope_quic::wire::stream::Id;
use sark_h3::conn::{
    Connection,
    event::{Event, Rejection},
};
use sark_http::field::metadata::section::rules;

use crate::common::conn::next_event;

/// Static request pseudo-fields followed by a literal uppercase field name.
pub const UPPERCASE: &[u8] = b"\x01\x17\x00\x00\xd1\xd7\x50\x0cexample.test\xc1\x21X\x01v";
pub const INVALID_PATH: &[u8] = b"x";

pub fn expect_malformed<P: rules::Policy>(conn: &mut Connection<'_, P>, id: Id) {
    assert!(matches!(
        next_event(conn).expect("poll request"),
        Some(Event::Rejected {
            stream_id,
            error: Rejection::Malformed { kind: rules::Kind::Request },
        }) if stream_id == id.into()
    ));
}
