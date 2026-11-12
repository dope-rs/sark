pub mod common;

use std::{cell::Cell, str};

use common::{
    body::replies::{self, Capture, Compressor, Reply, Shape},
    fields::{DATE, RESPONSE},
    tasks::Streaming,
};
use o3::buffer::write::Slice;
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
    h1::head::{line::Version, request::Coding},
    owner::handoff,
    payload::response::{Method, fixed, status::Code},
};

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
                cache::Cache::empty(),
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
fn cache_match() {
    let mut entry = None;
    let mut write = [0; 256];
    let policy = route::Policy {
        cache: route::Cache::Store,
        ..replies::POLICY
    };
    let outcome = output::Egress::new(
        o3::buffer::write::Slice::new(&mut write).reborrow(),
        DATE,
        RESPONSE,
    )
    .route(
        fixed::Direct::new(Code::OK, (), replies::BYTES),
        handoff::Slot::ready(None),
        cache::Cache::new(&mut entry),
        &mut (),
        Coding::Plain,
        policy,
    );
    assert!(
        matches!(outcome, output::Outcome::SendCopied { copy, .. } if copy.written == replies::BYTES.len())
    );
    for context in [
        Context::new(Method::Head, Version::Eleven),
        Context::new(Method::Other, Version::Ten),
    ] {
        write.fill(0xa5);
        assert!(
            output::Egress::new(
                o3::buffer::write::Slice::new(&mut write).reborrow(),
                DATE,
                context
            )
            .cached(&mut cache::Cache::new(&mut entry), route::Cache::Store)
            .is_none()
        );
        assert!(write.iter().all(|byte| *byte == 0xa5));
    }
    let calls = Cell::new(0);
    let context = Context::new(Method::Head, Version::Eleven);
    let cold = output::Egress::new(
        o3::buffer::write::Slice::new(&mut write).reborrow(),
        DATE,
        context,
    )
    .route(
        Reply {
            status: Code::OK,
            shape: Shape::Exact,
            calls: &calls,
        },
        handoff::Slot::ready(None),
        cache::Cache::new(&mut entry),
        &mut (),
        Coding::Plain,
        policy,
    );
    assert!(matches!(
        cold,
        output::Outcome::SendCopied { copy, .. } if copy.written == 0
    ));
    let hot = output::Egress::new(
        o3::buffer::write::Slice::new(&mut write).reborrow(),
        DATE,
        context,
    )
    .cached(&mut cache::Cache::new(&mut entry), route::Cache::Store);
    assert!(matches!(
        hot,
        Some(output::Outcome::SendCopied { copy, .. }) if copy.written == 0
    ));
    assert_eq!(calls.get(), 0);
}

#[test]
fn omits_stream() {
    for &request in replies::HEADS {
        let parsed = parsing::Head::parse(Input::Borrowed(request), u8::MAX)
            .expect("line")
            .expect("head");
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
