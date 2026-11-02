pub mod common;

use common::{
    hpack::{Fields, GET},
    protocol::{
        Session,
        content_length::{
            BODY, CONNECT_STATUSES, EMPTY_LENGTHS, EMPTY_RESPONSES, FORBIDDEN_LENGTHS, LENGTHS,
        },
        session::{Frame, Wire},
    },
    response::CONNECT,
};
use sark_core::manifold::stream::{Boundary, Section};
use sark_h2::{
    ClientRole, ServerRole,
    conn::{CLIENT_PREFACE, Error, config::Config, event::Event, machine::Connection},
    frame::{Block, numbers::ErrorCode},
    stream::Id,
};
use sark_http::field::metadata::map::record::Entry;

#[test]
fn requests_count_unpadded_data_once() {
    for (length, expected) in LENGTHS {
        for trailers in [false, true] {
            let mut conn = Connection::<ServerRole>::with_config(Config {
                event_capacity: 1,
                ..Config::default()
            })
            .expect("single-event server");
            conn.ingress().ingest(CLIENT_PREFACE).expect("peer preface");
            conn.ingress().poll_event().expect("preface event");
            conn.ingress()
                .ingest(common::INITIAL_SETTINGS)
                .expect("peer settings");
            conn.ingress().poll_event().expect("settings event");
            conn.transfer().drain(usize::MAX);
            let block = Fields::new(&[
                GET[0],
                GET[1],
                GET[2],
                GET[3],
                Entry::new(b"content-length", length),
            ])
            .try_encode()
            .expect("request block");
            conn.ingress()
                .ingest(
                    &Frame::new(1, &block)
                        .try_headers(Boundary::More, Block::End)
                        .expect("request headers"),
                )
                .expect("request ingress");
            let prefix = Frame::new(1, &BODY[..2])
                .try_padded(Boundary::More, 3)
                .expect("padded prefix");
            assert!(matches!(
                conn.ingress().ingest(&prefix),
                Err(Error::EventFull { used: 1, limit: 1 })
            ));
            assert!(matches!(
                conn.ingress().poll_event(),
                Some(Event::Headers { .. })
            ));
            conn.ingress().resume().expect("retry prefix once");
            assert!(matches!(
                conn.ingress().poll_event(),
                Some(Event::Data { data, boundary: Boundary::More, .. })
                    if data.contiguous() == Some(&BODY[..2])
            ));
            let boundary = if trailers {
                Boundary::More
            } else {
                Boundary::End
            };
            conn.ingress()
                .ingest(
                    &Frame::new(1, &BODY[2..])
                        .try_padded(boundary, 2)
                        .expect("padded suffix"),
                )
                .expect("suffix ingress");
            if trailers && expected >= BODY.len() as u64 {
                assert!(matches!(
                    conn.ingress().poll_event(),
                    Some(Event::Data { data, boundary: Boundary::More, .. })
                        if data.contiguous() == Some(&BODY[2..])
                ));
                conn.ingress()
                    .ingest(
                        &Frame::new(1, &[])
                            .try_headers(Boundary::End, Block::End)
                            .expect("trailer frame"),
                    )
                    .expect("trailer ingress");
            }
            let event = conn.ingress().poll_event().expect("body disposition");
            if expected != BODY.len() as u64 {
                assert!(matches!(
                    event,
                    Event::StreamReset {
                        error: ErrorCode::ProtocolError,
                        ..
                    }
                ));
                assert!(matches!(
                    Wire::new(conn.output().first())
                        .try_reset()
                        .expect("reset frame"),
                    Some((_, ErrorCode::ProtocolError))
                ));
            } else if trailers {
                assert!(matches!(
                    event,
                    Event::Headers {
                        section: Section::Trailing,
                        boundary: Boundary::End,
                        ..
                    }
                ));
            } else {
                assert!(matches!(
                    event,
                    Event::Data { data, boundary: Boundary::End, .. }
                        if data.contiguous() == Some(&BODY[2..])
                ));
            }
            assert!(conn.ingress().poll_event().is_none());
        }
    }
}

#[test]
fn initial_end_requires_empty_content() {
    for (length, empty) in EMPTY_LENGTHS {
        let mut server = Connection::<ServerRole>::try_new().expect("server");
        Session::new(&mut server)
            .try_prime()
            .expect("client preface");
        let request = Fields::new(&[
            GET[0],
            GET[1],
            GET[2],
            GET[3],
            Entry::new(b"content-length", length),
        ])
        .try_encode()
        .expect("request block");
        server
            .ingress()
            .ingest(
                &Frame::new(1, &request)
                    .try_headers(Boundary::End, Block::End)
                    .expect("request headers"),
            )
            .expect("request ingress");
        let mut conn = Connection::<ClientRole>::try_new().expect("client");
        Session::new(&mut conn).try_prime().expect("peer preface");
        let stream = Session::new(&mut conn).try_open().expect("request");
        let block = Fields::new(&[
            Entry::new(b":status", b"200"),
            Entry::new(b"content-length", length),
        ])
        .try_encode()
        .expect("response block");
        conn.ingress()
            .ingest(
                &Frame::new(stream.into_u32(), &block)
                    .try_headers(Boundary::End, Block::End)
                    .expect("response headers"),
            )
            .expect("response ingress");
        let request = server.ingress().poll_event();
        let response = conn.ingress().poll_event().expect("response disposition");
        if empty {
            assert!(matches!(
                request,
                Some(Event::Headers {
                    boundary: Boundary::End,
                    ..
                })
            ));
            assert!(matches!(
                response,
                Event::Headers {
                    boundary: Boundary::End,
                    ..
                }
            ));
        } else {
            assert!(request.is_none());
            assert_eq!(
                Wire::new(server.output().first())
                    .try_reset()
                    .expect("request reset"),
                Some((Id::FIRST_CLIENT, ErrorCode::ProtocolError)),
            );
            assert!(matches!(
                response,
                Event::StreamReset {
                    error: ErrorCode::ProtocolError,
                    ..
                }
            ));
        }
        assert!(server.ingress().poll_event().is_none());
        assert!(conn.ingress().poll_event().is_none());
    }
}

#[test]
fn empty_response_semantics_override_length_metadata() {
    for (request, response) in EMPTY_RESPONSES {
        for body in [None, Some(&[][..]), Some(BODY)] {
            let mut conn = Connection::<ClientRole>::try_new().expect("client");
            Session::new(&mut conn).try_prime().expect("peer preface");
            let stream = conn
                .streams()
                .headers()
                .request(request, Boundary::End)
                .expect("request");
            let block = Fields::new(response).try_encode().expect("response block");
            conn.ingress()
                .ingest(
                    &Frame::new(stream.into_u32(), &block)
                        .try_headers(
                            if body.is_some() {
                                Boundary::More
                            } else {
                                Boundary::End
                            },
                            Block::End,
                        )
                        .expect("response headers"),
                )
                .expect("response ingress");
            assert!(matches!(
                conn.ingress().poll_event(),
                Some(Event::Headers {
                    section: Section::Initial,
                    ..
                })
            ));
            if let Some(body) = body {
                conn.ingress()
                    .ingest(
                        &Frame::new(stream.into_u32(), body)
                            .try_padded(Boundary::End, 2)
                            .expect("terminal DATA"),
                    )
                    .expect("DATA ingress");
                let event = conn.ingress().poll_event().expect("body disposition");
                if body.is_empty() {
                    assert!(matches!(
                        event,
                        Event::Data { data, boundary: Boundary::End, .. }
                            if data.is_empty()
                    ));
                } else {
                    assert!(matches!(
                        event,
                        Event::StreamReset {
                            error: ErrorCode::ProtocolError,
                            ..
                        }
                    ));
                }
            }
            assert!(conn.ingress().poll_event().is_none());
        }
    }
}

#[test]
fn forbidden_response_lengths_reset_before_delivery() {
    for (request, status, length) in FORBIDDEN_LENGTHS {
        let mut conn = Connection::<ClientRole>::try_new().expect("client");
        Session::new(&mut conn).try_prime().expect("peer preface");
        let stream = conn
            .streams()
            .headers()
            .request(request, Boundary::End)
            .expect("request");
        let block = Fields::new(&[
            Entry::new(b":status", status),
            Entry::new(b"content-length", length),
        ])
        .try_encode()
        .expect("response block");
        conn.ingress()
            .ingest(
                &Frame::new(stream.into_u32(), &block)
                    .try_headers(Boundary::More, Block::End)
                    .expect("response headers"),
            )
            .expect("response ingress");
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::StreamReset {
                error: ErrorCode::ProtocolError,
                ..
            })
        ));
        assert!(conn.ingress().poll_event().is_none());
    }
}

#[test]
fn connect_success_retains_tunnel_data() {
    for status in CONNECT_STATUSES {
        for trailers in [false, true] {
            let mut conn = Connection::<ClientRole>::try_new().expect("client");
            Session::new(&mut conn).try_prime().expect("peer preface");
            let stream = conn
                .streams()
                .headers()
                .request(CONNECT, Boundary::More)
                .expect("CONNECT request");
            let block = Fields::new(&[
                Entry::new(b":status", status),
                Entry::new(b"content-length", b"42"),
            ])
            .try_encode()
            .expect("response block");
            conn.ingress()
                .ingest(
                    &Frame::new(stream.into_u32(), &block)
                        .try_headers(Boundary::More, Block::End)
                        .expect("successful CONNECT"),
                )
                .expect("response ingress");
            assert!(matches!(
                conn.ingress().poll_event(),
                Some(Event::Headers { .. })
            ));
            let boundary = if trailers {
                Boundary::More
            } else {
                Boundary::End
            };
            conn.ingress()
                .ingest(
                    &Frame::new(stream.into_u32(), BODY)
                        .try_padded(boundary, 2)
                        .expect("tunnel DATA"),
                )
                .expect("tunnel ingress");
            assert!(matches!(
                conn.ingress().poll_event(),
                Some(Event::Data { data, boundary: actual, .. })
                    if actual == boundary && data.contiguous() == Some(BODY)
            ));
            if trailers {
                conn.ingress()
                    .ingest(
                        &Frame::new(stream.into_u32(), &[])
                            .try_headers(Boundary::End, Block::End)
                            .expect("tunnel trailers"),
                    )
                    .expect("trailer ingress");
                assert!(matches!(
                    conn.ingress().poll_event(),
                    Some(Event::StreamReset {
                        error: ErrorCode::ProtocolError,
                        ..
                    })
                ));
            }
            assert!(conn.ingress().poll_event().is_none());
        }
    }
}

#[test]
fn connect_request_data_is_tunnel_before_response() {
    use common::headers::entry;
    use sark_http::field::literal;

    let mut client = Connection::<ClientRole>::try_new().expect("client");
    let mut server = Connection::<ServerRole>::try_new().expect("server");
    Session::new(&mut client)
        .try_prime()
        .expect("server preface");
    Session::new(&mut server)
        .try_prime()
        .expect("client preface");
    let id = client
        .streams()
        .headers()
        .request(
            [
                CONNECT[0],
                CONNECT[1],
                literal!(entry(b"content-length", b"0")),
            ],
            Boundary::More,
        )
        .expect("CONNECT request");
    server
        .ingress()
        .ingest(client.output().first())
        .expect("CONNECT ingress");
    assert!(matches!(
        server.ingress().poll_event(),
        Some(Event::Headers { .. })
    ));
    server
        .ingress()
        .ingest(
            &Frame::new(id.into_u32(), BODY)
                .try_data(Boundary::End)
                .expect("tunnel DATA"),
        )
        .expect("DATA before any response");
    assert!(matches!(
        server.ingress().poll_event(),
        Some(Event::Data { data, boundary: Boundary::End, .. })
            if data.contiguous() == Some(BODY)
    ));
    assert!(server.ingress().poll_event().is_none());
}

#[test]
fn connect_failure_response_keeps_exact_content_length() {
    for length in [b"3".as_slice(), b"4".as_slice()] {
        let mut conn = Connection::<ClientRole>::try_new().expect("client");
        Session::new(&mut conn).try_prime().expect("peer preface");
        let stream = conn
            .streams()
            .headers()
            .request(CONNECT, Boundary::More)
            .expect("CONNECT request");
        let block = Fields::new(&[
            Entry::new(b":status", b"400"),
            Entry::new(b"content-length", length),
        ])
        .try_encode()
        .expect("failure response");
        conn.ingress()
            .ingest(
                &Frame::new(stream.into_u32(), &block)
                    .try_headers(Boundary::More, Block::End)
                    .expect("response HEADERS"),
            )
            .expect("response ingress");
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::Headers { .. })
        ));
        conn.ingress()
            .ingest(
                &Frame::new(stream.into_u32(), BODY)
                    .try_data(Boundary::End)
                    .expect("response DATA"),
            )
            .expect("response body ingress");
        let event = conn
            .ingress()
            .poll_event()
            .expect("response body disposition");
        if length == b"4" {
            assert!(
                matches!(event, Event::Data { data, boundary: Boundary::End, .. } if data.contiguous() == Some(BODY))
            );
        } else {
            assert!(matches!(
                event,
                Event::StreamReset {
                    error: ErrorCode::ProtocolError,
                    ..
                }
            ));
        }
        assert!(conn.ingress().poll_event().is_none());
    }
}
