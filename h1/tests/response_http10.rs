pub mod common;

use std::{cell::Cell, str};

use common::{
    body::replies,
    fields::DATE,
    tasks::{self, Request, Streaming},
};
use o3::buffer::write::Slice;
use sark_core::body::credit;
use sark_h1::{
    Input, route, session,
    wire::{
        Failure, head, output,
        parsing::{framing, raw},
        response::Context,
    },
};
use sark_http::{
    h1::head::{
        line::Version,
        request::Coding,
        response::{Date, Server},
    },
    payload::response::{Fixed, status::Code},
};

#[test]
fn fixed_connections() {
    let policy = route::Policy::new(route::Mode::Plain, Date::Include, Server::Include);
    for &(request, connection) in replies::CONNECTIONS {
        let parsed = common::head::parsed(Input::Borrowed(request), u8::MAX);
        let response = head::Parsed::<()>::parse::<Request, false>(&parsed, 128)
            .ok()
            .flatten()
            .expect("admitted head")
            .response()
            .with_coding(Coding::Plain);
        let mut bytes = [0; 256];
        let mut writer = Slice::new(&mut bytes);
        let result = output::Egress::new(writer.reborrow(), DATE, response).try_route(
            Fixed::new(Code::OK, (), replies::BYTES),
            &mut (),
            policy,
        );
        let Ok(output::Complete::Copy { copy }) = result else {
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
fn absent_stream_completes_with_zero_length() {
    for (request, expected) in [
        (
            b"GET / HTTP/1.0\r\nHost: local\r\n\r\n",
            b"HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n",
        ),
        (
            b"GET / HTTP/1.1\r\nHost: local\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        ),
    ] {
        let parsed = common::head::parsed(Input::Borrowed(request), u8::MAX);
        let Ok(Some(head)) =
            framing::Head::<Streaming<false>>::parse::<false>(raw::Bound::empty(&parsed))
        else {
            panic!("complete GET request");
        };
        let mut conn: session::Conn = session::Conn::default();
        let mut bytes = [0; 128];
        let mut writer = Slice::new(&mut bytes);
        let outcome = conn.dispatch_stream::<Streaming<false>, _, _, _, _>(
            session::Permit::new(),
            head,
            &Cell::new(0),
            writer.reborrow(),
            DATE,
            || Some(|(), _: &mut (), _: Context| panic!("absent producer installed")),
            credit::Disabled::new(),
        );
        assert!(matches!(
            outcome,
            session::RouteOutcome::Outcome(session::Outcome::Complete {
                response: output::Complete::Inline,
                ..
            })
        ));
        assert_eq!(writer.into_mut(), expected);
    }
}

#[test]
fn stream_ten() {
    let request = replies::STREAM_TEN;
    for input in [request, &request[..request.len() - 1]] {
        let mut write = [0xa5; 256];
        let admitted = Cell::new(false);
        let outcome = tasks::try_dispatch_stream(
            input,
            &mut Slice::new(&mut write),
            &Cell::new(0),
            Some(&admitted),
        );
        assert!(matches!(
            outcome,
            Ok(Some(session::Outcome::Failed(Failure::Output(
                output::Error::StreamingVersion(Version::Ten)
            ))))
        ));
        assert!(!admitted.get());
        assert!(write.iter().all(|byte| *byte == 0xa5));
    }
}
