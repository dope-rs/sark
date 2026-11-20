pub mod common;

use std::{cell::Cell, str};

use common::{
    body::replies::{self, Capture, Compressor, Reply, Shape},
    fields::DATE,
    tasks::Streaming,
};
use o3::buffer::write::{Sink as _, Slice};
use sark_core::body::credit;
use sark_h1::{
    Input,
    route::stream,
    session,
    wire::{
        Failure, head, output,
        parsing::{framing, raw},
        response::{self, Context},
    },
};
use sark_http::{
    h1::head::{line::Version, request::Coding},
    owner::handoff,
    payload::response::{Method, fixed, status::Code},
};

#[test]
fn bounded_overflow_preserves_output() {
    let calls = Cell::new(0);
    let mut bytes = [0; 128];
    let mut writer = Slice::new(&mut bytes);
    writer.write_slice(b"prior").unwrap();
    let outcome = output::Egress::new(writer.reborrow(), DATE, common::fields::RESPONSE).plain(
        Reply {
            status: Code::OK,
            shape: Shape::Bounded,
            calls: &calls,
        },
        head::Connection::KeepAlive,
    );
    assert!(matches!(
        outcome,
        output::Outcome::Failed(Failure::Output(output::Error::Wire(
            response::Error::PlanOverflow {
                current: 108,
                added: usize::MAX,
            }
        )))
    ));
    assert_eq!(calls.get(), 1);
    assert_eq!(writer.into_mut(), b"prior");
}

#[test]
fn omits_body() {
    for &(method, status) in replies::CASES {
        for shape in [Shape::Fixed, Shape::Static, Shape::Exact, Shape::Bounded] {
            let calls = Cell::new(0);
            let mut gzip = Compressor { calls: 0 };
            let mut write = [0; 256];
            let mut writer = Slice::new(&mut write);
            let outcome = output::Egress::new(
                writer.reborrow(),
                DATE,
                Context::new(method, Version::Eleven),
            )
            .route(
                Reply {
                    status,
                    shape,
                    calls: &calls,
                },
                handoff::Slot::ready(None),
                &mut gzip,
                Coding::Gzip,
                replies::POLICY,
            );
            let written = match outcome {
                output::Outcome::Send { .. } => Some(writer.len()),
                _ => None,
            }
            .expect("head-only response");
            let head = str::from_utf8(&write[..written]).expect("response head");
            assert_eq!(head.find("\r\n\r\n"), Some(written - 4));
            assert_eq!(
                head.contains("Content-Length: 0\r\n"),
                status == Code::RESET_CONTENT
            );
            assert!(!head.contains("Transfer-Encoding:"));
            assert_eq!(calls.get(), 0);
            assert_eq!(gzip.calls, 0);
        }
    }
}

#[test]
fn omits_stream() {
    for &request in replies::HEADS {
        let parsed = common::head::parsed(Input::Borrowed(request), u8::MAX);
        let head = framing::Head::<Streaming>::parse(raw::Bound::empty(&parsed))
            .ok()
            .flatten()
            .expect("framing");
        let mut conn: session::Conn = session::Conn::default();
        let mut write = [0; 256];
        let mut writer = Slice::new(&mut write);
        let calls = Cell::new(0);
        let observed_calls = &calls;
        let state = Cell::new(1);
        let drops = Cell::new(0);
        let capture = Capture {
            state: &state,
            drops: &drops,
        };
        let result = stream::Endpoint::new(
            output::Egress::new(writer.reborrow(), DATE, parsed.response()),
            &mut conn,
        )
        .dispatch::<Streaming, _, _, _, _>(
            session::Permit::new(),
            head,
            &state,
            move || {
                let capture = capture;
                observed_calls.set(observed_calls.get() + 1);
                Some(move |(), _: &mut (), _: Context| capture.state.set(0))
            },
            credit::Disabled::new(),
        );
        let written = match result {
            session::RouteOutcome::Outcome(session::Outcome::Complete {
                response: output::Complete::Inline,
                ..
            }) => Some(writer.len()),
            _ => None,
        }
        .expect("head-only stream");
        let head = str::from_utf8(&write[..written]).expect("response head");
        assert!(!head.contains("Transfer-Encoding:"));
        assert_eq!(head.find("\r\n\r\n"), Some(written - 4));
        assert_eq!(calls.get(), 0);
        assert_eq!((state.get(), drops.get()), (0, 1));
    }
}

#[test]
fn phase_error() {
    for (method, status, expected) in [
        (
            Method::Other,
            Code::EARLY_HINTS,
            output::Error::Informational(Code::EARLY_HINTS),
        ),
        (Method::Connect, Code::OK, output::Error::Tunnel(Code::OK)),
        (
            Method::Connect,
            Code::NO_CONTENT,
            output::Error::Tunnel(Code::NO_CONTENT),
        ),
    ] {
        let mut write = [0xa5; 256];
        let result = output::Egress::new(
            o3::buffer::write::Slice::new(&mut write).reborrow(),
            DATE,
            Context::new(method, Version::Eleven),
        )
        .plain(
            fixed::Direct::new(status, (), replies::BYTES),
            head::Connection::KeepAlive,
        );
        assert!(
            matches!(result, output::Outcome::Failed(Failure::Output(actual)) if actual == expected)
        );
        assert!(write.iter().all(|byte| *byte == 0xa5));
    }
}
