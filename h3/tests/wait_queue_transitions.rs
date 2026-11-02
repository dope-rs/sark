use crate::common::{
    conn::Events,
    message::{Frame as _, promise::Authority},
};

pub mod common;

use dope_quic::{errors::Code, wire::stream::Id};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection,
        event::{Event, Role},
        io::Receive,
        options,
    },
    frame::TYPE_HEADERS,
};

#[test]
fn waiter_removal() {
    let mut config = options::Config::default();
    config.ingress.event_capacity = 1;
    let mut conn = Connection::with_config(Role::Server, config).expect("connection");
    let first = Id::ZERO;
    let second = Id::new(4).expect("stream");
    assert!(
        Receive::new(&mut conn)
            .stop(Id::new(8).expect("stream"), None, Code::from_u16(0))
            .is_ok()
    );
    let block = Authority.try_request().expect("request");
    for id in [first, second] {
        let wire = (&block).try_frame(TYPE_HEADERS).expect("frame");
        Receive::new(&mut conn)
            .owned(id, wire, Boundary::End)
            .expect("park");
    }
    let _ = conn.events().poll_token().expect("clear event");
    assert!(
        Receive::new(&mut conn)
            .reset(first, None, Code::from_u16(0))
            .is_ok()
    );
    let _ = conn.events().poll_token().expect("clear reset");
    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("ready"),
        Some(Event::Headers { message, .. }) if message.stream_id() == second.into())
    );
}
