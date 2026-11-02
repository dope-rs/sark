use sark_http::field::literal;

use crate::common::{
    conn::{Events, build::Fields as _},
    field::entry,
    message::{
        Frame as _,
        promise::{Authority, Push as _},
    },
    stream::Stream as _,
};

pub mod common;

use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    VarInt,
    conn::{
        Connection,
        event::{Completion, Event, Role},
        io::{Receive, Send},
    },
    frame,
};

#[test]
fn push_fin_after_headers() {
    let mut conn = Connection::with_push::<Authority>(Role::Client).expect("connection");
    conn.allow_push(Role::Client, 0).expect("push grant");
    Send::new(&mut conn)
        .headers(Id::ZERO, Authority, Boundary::End)
        .expect("originating request");
    Receive::new(&mut conn)
        .owned(
            Id::ZERO,
            VarInt::ZERO
                .try_promise(&Authority.try_request().expect("promised request"))
                .expect("promise"),
            Boundary::More,
        )
        .expect("promise before push stream");
    assert!(matches!(
        Events::new(&mut conn).poll_token().expect("promise event"),
        Some(Event::PushPromise { push_id: 0, .. })
    ));
    let id = Id::new(3).expect("push stream");
    let block = [literal!(entry(b":status", b"200"))]
        .try_block()
        .expect("headers");
    let headers = (&block).try_frame(frame::TYPE_HEADERS).expect("frame");
    Receive::new(&mut conn)
        .owned(
            id,
            VarInt::ZERO.try_stream(&headers).expect("push wire"),
            Boundary::More,
        )
        .expect("push headers");
    assert!(matches!(
        Events::new(&mut conn).poll_token().expect("headers"),
        Some(Event::PushHeaders { .. })
    ));
    Receive::new(&mut conn)
        .borrowed(id, &[], Boundary::End)
        .expect("push fin");
    let finished = Events::new(&mut conn)
        .poll_token()
        .expect("completion")
        .expect("finished");
    assert!(matches!(finished.completion(), Completion::End(stream) if stream == id.into()));
}
