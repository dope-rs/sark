pub mod common;

use std::{cell::Cell, str};

use common::{
    body::replies,
    fields::DATE,
    tasks::{Request, Streaming},
};
use sark_core::body::credit;
use sark_h1::{
    Input,
    route::{self, stream},
    session,
    wire::{
        Failure, cache, head, output,
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
    payload::response::{Method, fixed, status::Code},
};

#[test]
fn fixed_connections() {
    for version in [Version::Ten, Version::Eleven] {
        for connection in [head::Connection::Close, head::Connection::KeepAlive] {
            let mut write = [0; 256];
            let response = Context::new(Method::Other, version);
            let outcome = output::Egress::new(&mut write, DATE, response)
                .plain(fixed::Direct::new(Code::OK, (), replies::BYTES), connection);
            let (written, len) = match outcome {
                output::Outcome::SendCopied { written, copy, .. } => Some((written, copy.written)),
                _ => None,
            }
            .expect("fixed response");
            let head = str::from_utf8(&write[..written - len]).expect("head");
            assert!(head.starts_with(match version {
                Version::Ten => "HTTP/1.0 200 OK\r\n",
                Version::Eleven => "HTTP/1.1 200 OK\r\n",
            }));
            assert_eq!(
                head.contains("Connection: keep-alive\r\n"),
                version == Version::Ten && matches!(connection, head::Connection::KeepAlive)
            );
            assert_eq!(
                head.contains("Connection: close\r\n"),
                version == Version::Eleven && matches!(connection, head::Connection::Close)
            );
            assert!(!head.contains("Transfer-Encoding:"));
            assert_eq!(&write[written - len..written], replies::BYTES);
        }
    }
}

#[test]
fn cached_connections() {
    let mut entry = None;
    let policy = route::Policy::new(
        route::Cache::Store,
        Coding::Plain,
        Date::Include,
        Server::Include,
    );
    for &(request, connection) in replies::CONNECTIONS {
        let parsed = parsing::Head::parse(Input::Borrowed(request), u8::MAX)
            .expect("line")
            .expect("head");
        let response = head::Parsed::<()>::parse::<Request>(&parsed, 128)
            .ok()
            .flatten()
            .expect("admitted head")
            .response();
        let mut cold = [0; 256];
        assert!(
            output::Egress::new(&mut cold, DATE, response)
                .cached(&mut cache::Cache::new(&mut entry), route::Cache::Store)
                .is_none()
        );
        let outcome = output::Egress::new(&mut cold, DATE, response).route(
            fixed::Direct::new(Code::OK, (), replies::BYTES),
            handoff::Slot::ready(None),
            cache::Cache::new(&mut entry),
            &mut (),
            Coding::Plain,
            policy,
        );
        let (written, len) = match outcome {
            output::Outcome::SendCopied { written, copy, .. } => Some((written, copy.written)),
            _ => None,
        }
        .expect("cached response");
        let head = str::from_utf8(&cold[..written - len]).expect("head");
        assert_eq!(
            head.lines().find(|line| line.starts_with("Connection:")),
            connection
        );
        assert_eq!(&cold[written - len..written], replies::BYTES);
        let mut hot = [0; 256];
        assert!(matches!(
            output::Egress::new(&mut hot, DATE, response)
                .cached(&mut cache::Cache::new(&mut entry), route::Cache::Store),
            Some(output::Outcome::SendCopied { .. })
        ));
        assert_eq!(hot, cold);
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
            output::Egress::new(&mut write, DATE, parsed.response()),
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
                        output::Egress::new(&mut write, DATE, response),
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
