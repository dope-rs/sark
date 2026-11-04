pub mod common;
use common::upgrade::client::{
    ACCEPT, Admit as _, Error, INVALID_FIELDS, REJECTED_FIELD, RETAINED_FIELDS,
};
use dope_test::alloc::Tracker;
use sark_http::{
    h1::{
        codec::decode::{fields::Sink as _, response},
        field::Connection,
        head::ParseError,
    },
    payload::response::status,
};
use sark_ws::{upgrade, upgrade::response::Admission};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn list_fields_merge() {
    for fields in [
        b"Connection: Upgrade\r\nConnection: keep-alive\r\nUpgrade: h2c/1\r\nUpgrade: websocket\r\n"
            .as_slice(),
        b"Upgrade: websocket, h2c/1\r\nConnection: keep-alive\r\nConnection: Upgrade\r\n",
    ] {
        let mut head = b"HTTP/1.1 101 upgraded\r\n".to_vec();
        head.extend_from_slice(fields);
        head.extend_from_slice(b"Sec-WebSocket-Accept: ");
        head.extend_from_slice(ACCEPT);
        head.extend_from_slice(b"\r\n\r\n");

        assert!(matches!(head.try_admit(), Ok(accept) if accept == *ACCEPT));
    }
}

#[test]
fn rejects_versioned_token() {
    let head = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket/1\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
    assert!(matches!(
        head.try_admit(),
        Err(Error::Handshake(upgrade::Error::Protocol))
    ));
}

#[test]
fn exact_field_errors() {
    for (value, error) in INVALID_FIELDS {
        assert_eq!(
            Admission::default().push(b"Upgrade", value),
            Err(upgrade::Error::Field(error)),
        );
    }
}

#[test]
fn failed_field_keeps_state() {
    for (previous, error) in RETAINED_FIELDS {
        let mut fields = Admission::default();
        if let Some(value) = previous {
            fields.push(b"Upgrade", value).expect("initial upgrade");
        }
        assert_eq!(
            fields.push(b"Upgrade", REJECTED_FIELD),
            Err(upgrade::Error::Field(upgrade::FieldError::Slash {
                offset: 15,
            })),
        );
        assert_eq!(
            fields.finish(1, status::Code::SWITCHING_PROTOCOLS, Connection::default()),
            Err(error),
        );
    }
}

#[test]
fn opaque_obs_rejects_nul() {
    let mut valid = b"HTTP/1.1 101 opaque reason\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nX-Opaque: \xff\r\nSec-WebSocket-Accept: ".to_vec();
    valid.extend_from_slice(ACCEPT);
    valid.extend_from_slice(b"\r\n\r\n");
    assert!(matches!(valid.try_admit(), Ok(accept) if accept == *ACCEPT));

    let mut bad = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nX-Nul: safe\0shadow\r\nSec-WebSocket-Accept: ".to_vec();
    bad.extend_from_slice(ACCEPT);
    bad.extend_from_slice(b"\r\n\r\n");
    assert!(matches!(bad.try_admit(), Err(Error::Decode(_))));
}

#[test]
fn accept_is_unique() {
    let mut head = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ".to_vec();
    head.extend_from_slice(ACCEPT);
    head.extend_from_slice(b"\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n");
    assert!(matches!(
        head.try_admit(),
        Err(Error::Decode(response::Error::Head(ParseError::Sink(
            upgrade::Error::Duplicate(upgrade::Header::Accept),
        ))))
    ));
}

#[test]
fn close_blocks_upgrade() {
    let head = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: close, Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
    assert!(matches!(
        head.try_admit(),
        Err(Error::Handshake(upgrade::Error::Close))
    ));
}

#[test]
fn splits_keep_admission() {
    use sark_http::{
        h1::{
            codec::decode::fields::Sink,
            head::{Limit, response::State},
        },
        payload::response::Method,
    };
    let head = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c/1, websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
    State::with(
        Vec::new(),
        Method::Other,
        Limit::RESPONSE.with_bytes(16 * 1024),
        |mut source, mut progress| {
            let mut fields = Admission::default();
            for split in 0..head.len() {
                let (next, parts) = progress
                    .advance(&source, |field| fields.entry(field))
                    .expect("response fragment");
                assert!(parts.is_none(), "split {split}");
                progress = next;
                source
                    .try_extend(&head[split..split + 1])
                    .expect("append head");
            }
            let (_, head) = progress
                .advance(&source, |field| fields.entry(field))
                .expect("complete response");
            let head = head
                .expect("complete response")
                .admit()
                .expect("response framing");
            assert!(
                matches!(fields.finish(head.version(), head.status(), head.control().connection()),
                Ok(accept) if accept == *ACCEPT)
            );
        },
    );
}

#[test]
fn admission_has_no_alloc() {
    let head = b"HTTP/1.1 101 upgraded\r\nUpgrade: h2c/1, websocket\r\nUpgrade: custom/2\r\nConnection: keep-alive, Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";

    let (result, alloc) = Tracker::usage(|| head.try_admit());

    assert!(matches!(result, Ok(accept) if accept == *ACCEPT));
    assert_eq!(alloc, (0, 0));
}
