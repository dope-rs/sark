pub mod common;

use std::{cell::Cell, str};

use common::{
    body::replies,
    fields::DATE,
    tasks::{Request, Streaming},
};
use o3::buffer::write::Slice;
use sark_core::body::credit;
use sark_h1::{
    Input,
    route::{self, stream},
    session,
    wire::{
        Failure, head, output,
        parsing::{self, framing, raw},
        response::Context,
    },
};
use sark_http::{
    h1::head::{
        line::Version,
        request::Coding,
        response::{Date, Server},
    },
    owner::handoff,
    payload::response::{fixed, status::Code},
};

#[test]
fn fixed_connections() {
    let policy = route::Policy::new(Coding::Plain, Date::Include, Server::Include);
    for &(request, connection) in replies::CONNECTIONS {
        let parsed = parsing::Head::parse(Input::Borrowed(request), u8::MAX)
            .expect("line")
            .expect("head");
        let response = head::Parsed::<()>::parse::<Request>(&parsed, 128)
            .ok()
            .flatten()
            .expect("admitted head")
            .response();
        let mut bytes = [0; 256];
        let mut writer = Slice::new(&mut bytes);
        let result = output::Egress::new(writer.reborrow(), DATE, response).route(
            fixed::Direct::new(Code::OK, (), replies::BYTES),
            handoff::Slot::ready(None),
            &mut (),
            Coding::Plain,
            policy,
        );
        let output::Outcome::SendCopied { copy, .. } = result else {
            panic!("fixed response must be copied");
        };
        let wire = writer.into_mut();
        let (head, body) = wire.split_at(wire.len() - copy.written);
        let head = str::from_utf8(head).expect("head");
        let status = if request.starts_with(b"GET / HTTP/1.0\r\n") {
            "HTTP/1.0 200 OK\r\n"
        } else {
            "HTTP/1.1 200 OK\r\n"
        };
        assert!(head.starts_with(status));
        assert_eq!(
            head.lines().find(|line| line.starts_with("Connection:")),
            connection
        );
        assert!(!head.contains("Transfer-Encoding:"));
        assert_eq!(body, replies::BYTES);
    }
}

#[test]
fn stream_ten() {
    for split in [false, true] {
        let mut request = replies::STREAM_TEN.to_vec();
        if !split {
            request.push(b'x');
        }
        let parsed = parsing::Head::parse(Input::Borrowed(&request), u8::MAX)
            .expect("line")
            .expect("head");
        let head = framing::Head::<Streaming>::parse(raw::Bound::empty(&parsed))
            .ok()
            .flatten()
            .expect("framing");
        let mut write = [0xa5; 256];
        let mut conn: session::Conn = session::Conn::default();
        let state = Cell::new(0);
        let admitted = Cell::new(false);
        let outcome = stream::Endpoint::new(
            output::Egress::new(
                o3::buffer::write::Slice::new(&mut write).reborrow(),
                DATE,
                parsed.response(),
            ),
            &mut conn,
        )
        .dispatch::<Streaming, _, _, _, _>(
            session::Permit::new(),
            head,
            &state,
            || Some(|(), _: &mut (), _: Context| admitted.set(true)),
            credit::Disabled::new(),
        );
        let outcome = match outcome {
            session::RouteOutcome::Outcome(outcome) => outcome,
            session::RouteOutcome::AwaitBody { pending, .. } => {
                let response = pending.response();
                assert_eq!(response.version(), Version::Ten);
                pending.finish(
                    session::Complete {
                        body_len: 1,
                        consumed: 1,
                    },
                    session::Permit::new(),
                    stream::Execution::new(
                        output::Egress::new(
                            o3::buffer::write::Slice::new(&mut write).reborrow(),
                            DATE,
                            response,
                        ),
                        &state,
                        &mut (),
                        || Some(|(), _: &mut (), _: Context| admitted.set(true)),
                    ),
                    credit::Disabled::new(),
                )
            }
            session::RouteOutcome::AwaitHead { pending, .. } => match pending {},
            session::RouteOutcome::AwaitReject { pending, .. } => match pending {},
        };
        assert!(matches!(
            outcome,
            session::Outcome::Failed(Failure::Output(output::Error::StreamingVersion(
                Version::Ten
            )))
        ));
        assert!(!admitted.get());
        assert!(write.iter().all(|byte| *byte == 0xa5));
    }
}
