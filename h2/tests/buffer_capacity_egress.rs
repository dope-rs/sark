use std::iter;

use o3::buffer::pool;
use sark_h2::{
    conn::{config::capacity, event::Event},
    frame::numbers::ErrorCode,
    stream::State,
};
use sark_http::field;

use crate::common::headers::entry;

pub mod common;

use sark_core::manifold::{
    server::Connection as _,
    stream::{Boundary, Control, Write as _},
};
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        Error,
        config::{Config, Settings},
        machine::Connection,
    },
    hpack::encode::fields,
    stream::Id,
};

use crate::common::capacity::{Upload, try_start};

#[test]
fn large_block_grows_output() {
    let mut client = Connection::<ClientRole>::try_new().expect("construct the default client");
    let before = client.output().len();
    let stream = client
        .streams()
        .headers()
        .request(
            [
                field::literal!(entry(b":method", b"POST")),
                field::literal!(entry(b":scheme", b"https")),
                field::literal!(entry(b":path", b"/proof.Admission/Commit")),
                field::literal!(entry(b":authority", b"localhost")),
                field::literal!(entry(b"content-type", b"application/grpc")),
                field::literal!(entry(b"te", b"trailers")),
                field::literal!(entry(
                    b"x-capacity-regression",
                    b"the-logical-egress-budget-must-not-be-confused-with-readable-ring-bytes",
                )),
            ],
            Boundary::More,
        )
        .expect("admit a header block larger than the client preface");
    assert!((stream) == (Id::FIRST_CLIENT));
    assert!(client.output().len() > before);
}

#[test]
fn continuation_retries() {
    use sark_core::manifold::client::io::{self, Prepared as _, Write as _};
    use sark_h2::frame::{
        self,
        layout::Header,
        numbers::{Flags, Type},
    };

    for (length, flags) in [
        (16_363, Flags::END_STREAM | Flags::END_HEADERS),
        (20 * 1024, Flags::END_STREAM),
    ] {
        let config = Config {
            local_settings: Settings {
                max_header_list_size: Some(32_768),
                ..Settings::DEFAULT
            },
            hpack_encoder: capacity::Encoder {
                header_list: 24 * 1024,
                ..Default::default()
            },
            outbound_capacity: 65_536,
            ..Config::default()
        };
        let mut client = Connection::<ClientRole>::with_config(config).expect("request client");
        let mut server = Connection::<ServerRole>::with_config(config).expect("request peer");
        let value = vec![b'~'; length];
        let id = client
            .streams()
            .headers()
            .request(
                [
                    field::literal!(entry(b":method", b"GET")),
                    field::literal!(entry(b":scheme", b"http")),
                    field::literal!(entry(b":path", b"/")),
                    field::literal!(entry(b":authority", b"localhost")),
                    entry(b"x-span", &value).expect("plain header value"),
                ],
                Boundary::End,
            )
            .expect("retain framed headers");
        let queued = client.outbound_len();
        let prefixes = pool::Pool::try_new(1, 256).expect("transfer prefix pool");
        let prepared = client
            .prepare(prefixes.cursor().expect("prefix slot"))
            .ok()
            .expect("prepare retained headers");
        let offered = prepared.try_bind().expect("bind retained headers");
        assert!(matches!(&offered, io::Offer::Body(_)));
        drop(offered);
        assert_eq!(prefixes.available(), 1);
        assert_eq!(client.outbound_len(), queued);

        let mut prefix = [0; 256];
        let (len, body) = match client.take(&mut prefix).expect("retry retained headers") {
            io::Output::Split {
                prefix_len: len,
                body,
            } => Some((len, body)),
            io::Output::Buffered(_) => None,
        }
        .expect("retained headers");
        assert_eq!(client.outbound_len(), 0);
        let first = Header::parse(body.as_slice()).expect("HEADERS frame");
        assert_eq!(
            (
                first.kind,
                first.length.into_usize(),
                first.flags.0,
                first.stream_id
            ),
            (Type::Headers, 16_384, flags, id)
        );
        let offset = frame::HEADER_LEN + first.length.into_usize();
        if flags == Flags::END_STREAM {
            let last = Header::parse(&body.as_slice()[offset..]).expect("CONTINUATION frame");
            assert_eq!(
                (last.kind, last.flags.0, last.stream_id),
                (Type::Continuation, Flags::END_HEADERS, id)
            );
            assert_eq!(
                offset + frame::HEADER_LEN + last.length.into_usize(),
                body.len()
            );
        } else {
            assert_eq!(offset, body.len());
        }

        server
            .ingress()
            .ingest(&prefix[..len])
            .expect("peer preface");
        server
            .ingress()
            .ingest_owned(body)
            .expect("decode retained headers");
        let (stream, boundary, fields) = iter::from_fn(|| server.ingress().poll_event())
            .find_map(|event| match event {
                Event::Headers {
                    stream_id: stream,
                    head,
                    boundary,
                    ..
                } => Some((stream, boundary, head.into_parts().0)),
                _ => None,
            })
            .expect("decoded request");
        assert_eq!(stream.id(), id);
        assert!(matches!(boundary, Boundary::End));
        assert_eq!(fields.get(b"x-span"), Some(value.as_slice()));
    }
}

#[test]
fn request_reuses_id() {
    let mut client = Connection::<ClientRole>::with_config(Config {
        outbound_capacity: 128,
        ..Config::default()
    })
    .expect("construct the constrained request client");
    client.transfer().drain(usize::MAX);
    for opaque in 0..7 {
        client
            .control()
            .ping([opaque; 8])
            .expect("fill request output with a ping");
    }
    let headers = [
        field::literal!(entry(b":method", b"GET")),
        field::literal!(entry(b":scheme", b"http")),
        field::literal!(entry(b":path", b"/")),
        field::literal!(entry(b":authority", b"x")),
    ];

    let error = client.streams().headers().request(headers, Boundary::End);
    assert!(
        error.as_ref().is_err_and(|error| matches!(error,
            Error::OutputAppend(cause)
                if cause.into_parts().0.get() > 9 && cause.into_parts().1 == 9
        )),
        "{error:?}"
    );
    assert_eq!(client.status().active_len(), 0);
    assert!(!client.status().has_stream(Id::FIRST_CLIENT));

    client.transfer().drain(usize::MAX);
    assert!(
        (client
            .streams()
            .headers()
            .request(headers, Boundary::End)
            .expect("reuse the first stream id after rejection"))
            == (Id::FIRST_CLIENT)
    );
    client.transfer().drain(usize::MAX);
    assert_eq!(
        client
            .streams()
            .headers()
            .request(headers, Boundary::End)
            .expect("advance the stream id after admission"),
        Id::new(3).expect("second client stream")
    );
}

#[test]
fn headers_ignore_headroom() {
    let (mut client, mut server, open) = Upload::new(Config {
        recv_window_target: 65_535,
        outbound_capacity: 64,
        ..Config::default()
    })
    .try_connect()
    .expect("capacity fixture");
    for opaque in 0..3 {
        server
            .control()
            .ping([opaque; 8])
            .expect("fill unrelated server output with a ping");
    }
    let open = server
        .status()
        .stream_key(open)
        .expect("current upload generation");
    server.streams().cancel(open, ErrorCode::Cancel);
    let out_len = server.output().len();
    assert_eq!(out_len, 64);

    let id = try_start(&mut client).expect("capacity fixture");
    server
        .ingress()
        .ingest(client.output().first())
        .expect("ingest headers without consuming egress headroom");
    assert_eq!(server.output().len(), out_len);
    assert!(
        matches!(server.ingress().poll_event(), Some(Event::Headers {
            stream_id: received,
            ..
        }) if received == id)
    );
}

#[test]
fn peer_limit_is_atomic() {
    let mut settings = Settings::DEFAULT;
    settings.max_header_list_size = Some(48);
    let mut client = Connection::<ClientRole>::with_config(Config {
        local_settings: settings,
        recv_window_target: 65_535,
        ..Config::default()
    })
    .expect("construct the limited peer client");
    let id = try_start(&mut client).expect("capacity fixture");
    let mut server = Connection::<ServerRole>::try_new().expect("construct the response server");
    server
        .ingress()
        .ingest(client.output().first())
        .expect("ingest the limited peer request");
    while server.ingress().poll_event().is_some() {}
    server.transfer().drain(usize::MAX);
    let state = server.status().stream_state(id);

    assert!(matches!(
        server.streams().headers().respond(
            id,
            [
                field::literal!(entry(b":status", b"200")),
                field::literal!(entry(b"x", b"v"))
            ],
            Boundary::End,
        ),
        Err(Error::HeaderEncode(fields::Error::List {
            used: 42,
            name: 1,
            value: 1,
            limit: 48,
        }))
    ));
    assert!(
        (server.status().stream_state(id)) == (state),
        "a rejected block must not commit the stream transition"
    );
    assert_eq!(server.output().len(), 0);

    server
        .streams()
        .headers()
        .respond(
            id,
            [field::literal!(entry(b":status", b"200"))],
            Boundary::End,
        )
        .expect("send a response within the peer header-list limit");
    assert!((server.status().stream_state(id)) == (Some(State::HalfClosedLocal)));
}

#[test]
fn local_limit_is_atomic() {
    let mut client = Connection::<ClientRole>::try_new().expect("construct the response client");
    let id = try_start(&mut client).expect("capacity fixture");
    let mut server = Connection::<ServerRole>::with_config(Config {
        outbound_capacity: 64,
        hpack_encoder: capacity::Encoder {
            header_list: 48,
            ..Default::default()
        },
        ..Config::default()
    })
    .expect("construct the locally bounded response server");
    server
        .ingress()
        .ingest(client.output().first())
        .expect("ingest the response request");
    while server.ingress().poll_event().is_some() {}
    server.transfer().drain(usize::MAX);
    let state = server.status().stream_state(id);
    let key = server.status().stream_key(id).expect("response stream");
    for opaque in 0..3 {
        server
            .control()
            .ping([opaque; 8])
            .expect("fill response output");
    }

    assert!(matches!(
        server.writer(key).headers(
            [
                field::literal!(entry(b":status", b"200")),
                field::literal!(entry(b"x", b"v"))
            ],
            Boundary::End,
        ),
        Err(Error::HeaderEncode(fields::Error::List {
            used: 42,
            name: 1,
            value: 1,
            limit: 48,
        }))
    ));
    assert!((server.status().stream_state(id)) == (state));
    assert_eq!(server.output().len(), 51);

    server.transfer().drain(usize::MAX);
    assert!(matches!(
        server
            .writer(key)
            .headers([field::literal!(entry(b":status", b"200"))], Boundary::End,),
        Ok(Control::Written)
    ));
}
