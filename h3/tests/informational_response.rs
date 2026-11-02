use sark_h3::{conn::Error, frame};

use crate::common::{
    conn::{Events, build::Fields as _},
    field::entry,
    message::{Frame as _, promise::Authority},
    stream,
};

pub mod common;

use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::{Boundary, Section};
use sark_h3::{
    conn::{
        Connection,
        event::{Event, Role},
        io::{Receive, Send},
    },
    frame::TYPE_HEADERS,
};

#[test]
fn informational_final() {
    let mut conn = Connection::with_role(Role::Client).expect("connection");
    Send::new(&mut conn)
        .headers(Id::ZERO, Authority, Boundary::End)
        .expect("originating request");
    for (index, status) in stream::RESPONSES.into_iter().enumerate() {
        let block = [entry(b":status", status).expect("field fixture")]
            .try_block()
            .expect("response");
        let wire = (&block).try_frame(TYPE_HEADERS).expect("frame");
        let boundary = if index == 2 {
            Boundary::End
        } else {
            Boundary::More
        };
        Receive::new(&mut conn)
            .owned(Id::ZERO, wire, boundary)
            .expect("receive");
        assert!(
            matches!(Events::new(&mut conn).poll_token().expect("event"),
            Some(Event::Headers { message, section: Section::Initial, .. }) if message.boundary() == boundary)
        );
    }
}

#[test]
fn informational_edges() {
    for boundary in [Boundary::End, Boundary::More] {
        let mut conn = Connection::with_role(Role::Client).expect("connection");
        Send::new(&mut conn)
            .headers(Id::ZERO, Authority, Boundary::End)
            .expect("originating request");
        let block = [entry(b":status", stream::RESPONSES[0]).expect("field fixture")]
            .try_block()
            .expect("response");
        let wire = (&block).try_frame(TYPE_HEADERS).expect("frame");
        let received = Receive::new(&mut conn).owned(Id::ZERO, wire, boundary);
        if matches!(boundary, Boundary::End) {
            assert!(matches!(
                received,
                Err(Error::MessageMissing { stream_id: 0 })
            ));
        } else {
            received.expect("informational");
            let _ = conn.events().poll_token().expect("headers");
            let wire = [].try_frame(frame::TYPE_DATA).expect("data");
            assert!(matches!(
                Receive::new(&mut conn).owned(Id::ZERO, wire, Boundary::More),
                Err(Error::FrameUnexpected(_))
            ));
        }
    }
}
