use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

use dope_test::Harness;
use sark_grpc::Status;
use sark_grpc::headers::HeaderBlock;
use sark_grpc::metadata::Metadata;
use sark_grpc::server::{Config, ConfigError, Handler, Limits, Request, Response, ValidatedLimits};
use sark_h2::{ClientRole, Conn, ErrorCode, StreamId, conn};

fn sid(raw: u32) -> StreamId {
    assert!(raw <= StreamId::MAX, "invalid test stream ID");
    StreamId::new(raw).expect("range checked above")
}

struct Nop;

fn connect(bind: std::net::SocketAddr) -> TcpStream {
    for _ in 0..200 {
        if let Ok(transport) = TcpStream::connect(bind) {
            return transport;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("listener did not start");
}

impl Handler for Nop {
    fn request(&mut self, _context: &mut (), _request: Request, response: &mut Response) {
        response.status = Status::ok();
    }
}

#[test]
fn invalid_limits_fail_before_listener_or_connection_construction() {
    let result = ValidatedLimits::new(Limits {
        max_in_flight: 0,
        ..Limits::default()
    });
    assert!(matches!(
        result,
        Err(ConfigError::ZeroCapacity("max_in_flight"))
    ));
}

#[test]
fn configured_capacity_is_built_at_accept() {
    let harness = Harness::bind().unwrap();
    let bind = harness.addr();
    let config = Config {
        bind,
        readiness: None,
        max_connections: 4,
        backlog: 16,
        grpc: Limits {
            max_in_flight: 1,
            ..Limits::default()
        },
    };
    harness
        .run_with_trigger(
            move |context, trigger| config.serve(Nop, context, Some(trigger)),
            |bind| {
                let mut transport = connect(bind);
                transport
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut client = Conn::<ClientRole>::new();
                let headers =
                    HeaderBlock::for_request(b"/svc/Method", None, &Metadata::new()).unwrap();
                assert_eq!(
                    client.start_request_fields(headers.iter(), true).unwrap(),
                    sid(1)
                );
                assert_eq!(
                    client.start_request_fields(headers.iter(), true).unwrap(),
                    sid(3)
                );
                transport.write_all(client.outbound()).unwrap();
                client.drain_outbound(client.outbound().len());

                let mut bytes = [0u8; 4096];
                let read = transport.read(&mut bytes).unwrap();
                client.ingest(&bytes[..read]).unwrap();
                assert_eq!(client.peer_settings().max_concurrent_streams, Some(1));
                let mut refused = false;
                while let Some(event) = client.poll_event() {
                    if let conn::Event::StreamReset { stream_id, error } = event {
                        refused |= stream_id == sid(3) && error == ErrorCode::RefusedStream;
                    }
                }
                assert!(refused);
            },
        )
        .unwrap();
}
