use sark_h3::{conn::Error, qpack::encoder::Encoder};

pub mod common;

use dope_quic::wire::stream::Id;
use o3::{buffer::pool, mem::Rejected};
use sark_core::{
    body,
    decode::{Decode, request, state, state::Homogeneous as _},
    manifold::stream::Boundary,
};
use sark_h3::{
    conn::{
        Connection,
        event::{Completion, Event, Role},
        io::{Receive, Send, Step},
    },
    link::{Flush, Transport},
    manifold::encoder,
    stream::Target,
};
use sark_http::field::{
    self, Entry, Pseudo, Value,
    head::Name,
    metadata::{
        block,
        map::record,
        section::{
            rules,
            state::Decoder,
            storage::{Scratch, Tag},
        },
        strings::Encoding,
    },
};

use crate::common::h3::{
    Capture, Framing, H3PlainRouter, H3Router, H3RouterRequestFamily, Routing, Selected, Wire,
};

#[test]
fn retained_range_mismatch() {
    let retained = b"/missingh3";
    let mut backing = pool::Span::try_new(1).expect("seal storage");
    let mut scratch = Scratch::default();
    for (end, expected) in [
        (retained.len(), block::Validation::Valid),
        (retained.len() + 1, block::Validation::Malformed),
    ] {
        let [mut method, mut path, mut protocol] = [(); 3].map(|()| {
            Decoder::h3(
                rules::Kind::Request,
                block::Connect::Disabled,
                <H3Router as Decode>::Plan::default(),
            )
        });
        method.store(
            Name::Path,
            rules::Store::Tagged(Tag::PATH),
            0..end,
            retained,
        );
        for decoder in [&mut method, &mut path, &mut protocol] {
            decoder.inspect::<rules::RejectPush>(
                rules::Inspect::Method,
                Name::Method,
                record::Entry::new(b":method", b"GET").into(),
                retained,
                &[],
            );
        }
        path.store(
            Name::Path,
            rules::Store::Tagged(Tag::PATH),
            0..end,
            retained,
        );
        protocol.store(
            Name::Path,
            rules::Store::Tagged(Tag::PATH),
            0..b"/missing".len(),
            retained,
        );
        protocol.store(
            Name::Protocol,
            rules::Store::Tagged(Tag::PROTOCOL),
            b"/missing".len()..end,
            retained,
        );
        for mut decoder in [method, path, protocol] {
            assert_eq!(
                decoder.seal(&mut backing, &mut scratch).expect("seal"),
                expected,
            );
        }
    }
}

#[test]
fn retains_used_authority() {
    let with = H3Router::new();
    let plain = H3PlainRouter::new();

    assert_eq!(
        with.try_retained()
            .expect("H3 admission")
            .expect("retained header"),
        plain
            .try_retained()
            .expect("H3 admission")
            .expect("retained header")
            + Tag::PREFIX_LEN
            + b"x".len(),
        "authority retention must cost exactly one tagged value and no raw field name",
    );
}

#[test]
fn routes_response() {
    let app = H3Router::new();

    let mut client =
        Connection::with_role(Role::Client).expect("default H3 client configuration must be valid");
    Send::new(&mut client)
        .headers(
            Id::ZERO,
            [
                const { Entry::pseudo(Pseudo::Method, field::literal!(Value::try_bytes(b"GET"))) },
                const { Entry::pseudo(Pseudo::Scheme, field::literal!(Value::try_bytes(b"https"))) },
                const { Entry::pseudo(Pseudo::Path, field::literal!(Value::try_bytes(b"/json"))) },
                const { Entry::new(field::literal!(field::Lower::try_bytes(b"host")), field::literal!(Value::try_bytes(b"x"))) },
                const { Entry::new(field::literal!(field::Lower::try_bytes(b"x-ignored")), field::literal!(Value::try_bytes(b"discard-me"))) },
                const { Entry::new(field::literal!(field::Lower::try_bytes(b"x-name")), field::literal!(Value::try_bytes(b"alice"))) },
                const { Entry::new(field::literal!(field::Lower::try_bytes(b"content-length")), field::literal!(Value::try_bytes(b"0"))) },
            ],
            Boundary::End,
        )
        .expect("fixture request headers must fit the client queue");

    let mut wire = Wire::default();
    assert!(
        wire.pump_writes(&mut client) == Ok(Flush::Drained),
        "client writes must be accepted by fixture transport"
    );

    let mut server = app.try_conn().expect("H3 connection");
    let Capture(request, boundary) = wire.sent.remove(&0).expect("client must emit stream zero");
    Receive::new(&mut server)
        .owned(Id::ZERO, request, boundary)
        .expect("server must accept the valid client stream");

    let mut pending = None;
    let mut routed = false;
    loop {
        let step = server
            .events()
            .poll_token()
            .expect("polling an initialized H3 server must succeed");
        let ev = match step {
            Step::Item(event) => event,
            Step::More => continue,
            Step::Idle => break,
        };
        let done = ev.completion();
        if let Event::Headers { head, .. } = ev {
            pending = Some(
                head.try_request()
                    .map_err(|_| ())
                    .expect("admitted headers must retain their control owner"),
            );
        }
        if let Completion::End(stream) = done {
            assert!(stream == Id::ZERO.into());
            let head = pending.take().expect("headers before finish");
            let method = head.control().method();
            let prepared = app.try_head(head).expect("known route");
            let started = request::Head::try_start(prepared, 0, Boundary::End)
                .map_err(Rejected::discard)
                .expect("empty GET body")
                .into_state();
            let admitted = state::Admission::admit(started, &mut state::Pass)
                .continue_value()
                .expect("unguarded route");
            let request = match H3RouterRequestFamily::fold(admitted, Selected).into_request() {
                request::Body::Finished(request) => Some(request),
                request::Body::Open(_) => None,
            }
            .expect("GET body must be complete");
            let request = request
                .try_map_body(|done| body::Body::try_new(done, &[][..]))
                .ok()
                .expect("finished body")
                .ready();
            let (response, _) = request
                .try_project(&app, |app, request| {
                    app.try_dispatch(
                        request.into_inner(),
                        encoder::Encoder::new(&mut server, Target::new(Id::ZERO), method),
                    )
                    .map(|prepared| prepared.finish())
                    .map_err(|(error, target)| (error.narrow(), target))
                })
                .ok()
                .expect("matching route projection");
            response
                .ok()
                .expect("synchronous dispatch")
                .expect("atomic H3 response");
            routed = true;
        }
    }
    assert!(routed);

    let mut back = Wire::default();
    assert!(
        back.pump_writes(&mut server) == Ok(Flush::Drained),
        "server writes must be accepted by fixture transport"
    );
    let Capture(response, boundary) = back.sent.remove(&0).expect("server must emit stream zero");
    Receive::new(&mut client)
        .owned(Id::ZERO, response, boundary)
        .expect("client must accept the valid server stream");

    let mut saw_head = false;
    let mut saw_body = false;
    loop {
        let step = client
            .events()
            .poll_token()
            .expect("polling an initialized H3 client must succeed");
        let ev = match step {
            Step::Item(event) => event,
            Step::More => continue,
            Step::Idle => break,
        };
        match ev {
            Event::Headers { head, .. } => {
                let (fields, _, _) = head.into_parts();
                assert!(
                    fields
                        .iter()
                        .any(|f| f.name == b":status" && f.value == b"200")
                );
                assert!(
                    fields
                        .iter()
                        .any(|f| f.name == b"content-type" && f.value == b"text/plain")
                );
                saw_head = true;
            }
            Event::Data { data, .. } => {
                assert!(
                    data.segments()
                        .flatten()
                        .copied()
                        .eq(b"hello-h3".iter().copied())
                );
                saw_body = true;
            }
            _ => {}
        }
    }
    assert!(saw_head && saw_body);
}

#[test]
fn decodes_huffman_plan() {
    let app = H3Router::new();
    let mut qpack = Encoder::new();
    qpack.set_encoding(Encoding::Huffman);
    let wire = qpack
        .try_frame(Id::ZERO, [
            const { Entry::pseudo(Pseudo::Path, field::literal!(Value::try_bytes(b"/json"))) },
            const { Entry::pseudo(Pseudo::Scheme, field::literal!(Value::try_bytes(b"https"))) },
            const { Entry::pseudo(Pseudo::Authority, field::literal!(Value::try_bytes(b"x"))) },
            const { Entry::pseudo(Pseudo::Method, field::literal!(Value::try_bytes(b"GET"))) },
            const {
                Entry::new(
                    field::literal!(field::Lower::try_bytes(b"x-ignored")),
                    field::literal!(Value::try_bytes(b"discarded huffman value")),
                )
            },
            const {
                Entry::new(
                    field::literal!(field::Lower::try_bytes(b"x-name")),
                    field::literal!(Value::try_bytes(b"alice")),
                )
            },
            const {
                Entry::new(
                    field::literal!(field::Lower::try_bytes(b"content-length")),
                    field::literal!(Value::try_bytes(b"0")),
                )
            },
        ])
        .expect("H3 frame");

    let mut server = app.try_conn().expect("H3 connection");
    Receive::new(&mut server)
        .owned(Id::ZERO, wire, Boundary::End)
        .expect("valid Huffman request must be admitted");
    let mut planned = None;
    loop {
        let step = server
            .events()
            .poll_token()
            .expect("polling an initialized H3 server must succeed");
        let ev = match step {
            Step::Item(event) => event,
            Step::More => continue,
            Step::Idle => break,
        };
        if let Event::Headers { head, .. } = ev {
            planned = Some(
                head.try_request()
                    .map_err(|_| ())
                    .expect("admitted headers must retain their control owner"),
            );
        }
    }
    let head = planned.expect("decoded request head");
    let method = head.control().method();
    let prepared = app.try_head(head).expect("known route");
    let started = request::Head::try_start(prepared, 0, Boundary::End)
        .map_err(Rejected::discard)
        .expect("empty GET body")
        .into_state();
    let admitted = state::Admission::admit(started, &mut state::Pass)
        .continue_value()
        .expect("unguarded route");
    let request = match H3RouterRequestFamily::fold(admitted, Selected).into_request() {
        request::Body::Finished(request) => Some(request),
        request::Body::Open(_) => None,
    }
    .expect("GET body must be complete");
    let request = request
        .try_map_body(|done| body::Body::try_new(done, &[][..]))
        .ok()
        .expect("finished body")
        .ready();
    let (response, _) = request
        .try_project(&app, |app, request| {
            app.try_dispatch(
                request.into_inner(),
                encoder::Encoder::new(&mut server, Target::new(Id::ZERO), method),
            )
            .map(|prepared| prepared.finish())
            .map_err(|(error, target)| (error.narrow(), target))
        })
        .ok()
        .expect("matching route projection");
    response
        .ok()
        .expect("synchronous dispatch")
        .expect("atomic H3 response");

    let mut sent = Wire::default();
    assert!(
        sent.pump_writes(&mut server) == Ok(Flush::Drained),
        "server writes must be accepted by fixture transport"
    );
    let capture = sent.sent.get(&0).expect("server must emit stream zero");
    let body = b"hello-h3";
    assert!(capture.0.windows(body.len()).any(|window| window == body));
}

#[test]
fn rejects_bad_authority() {
    let app = H3Router::new();
    let mut qpack = Encoder::new();
    let wire = qpack
        .try_frame(Id::ZERO, [
            const { Entry::pseudo(Pseudo::Method, field::literal!(Value::try_bytes(b"GET"))) },
            const { Entry::pseudo(Pseudo::Scheme, field::literal!(Value::try_bytes(b"https"))) },
            const {
                Entry::pseudo(
                    Pseudo::Authority,
                    field::literal!(Value::try_bytes(b"origin.example")),
                )
            },
            const { Entry::pseudo(Pseudo::Path, field::literal!(Value::try_bytes(b"/json"))) },
            const {
                Entry::new(
                    field::literal!(field::Lower::try_bytes(b"host")),
                    field::literal!(Value::try_bytes(b"other.example")),
                )
            },
            const {
                Entry::new(
                    field::literal!(field::Lower::try_bytes(b"x-name")),
                    field::literal!(Value::try_bytes(b"alice")),
                )
            },
        ])
        .expect("H3 frame");
    let mut server = app.try_conn().expect("H3 connection");
    std::assert_matches!(
        Receive::new(&mut server).owned(Id::ZERO, wire, Boundary::End),
        Err(Error::Malformed {
            stream_id: 0,
            kind: rules::Kind::Request,
        }),
    );
    assert!(matches!(
        server
            .events()
            .poll_token()
            .expect("polling a rejected H3 request must succeed"),
        Step::Idle
    ));
}
