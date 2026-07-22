use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

use dope_extra::harness::Harness;
use sark_grpc::Status;
use sark_grpc::headers::HeaderBlock;
use sark_grpc::metadata::Metadata;
use sark_grpc::server::{self, Config, Handler, Limits, Request, Response};
use sark_h2::{ClientRole, Conn, ErrorCode, StreamId, conn};

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
    fn request(&mut self, _request: Request, response: &mut Response) {
        response.status = Status::ok();
    }
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
            move |context, trigger| server::serve(Nop, config, context, Some(trigger)),
            |bind| {
                let mut transport = connect(bind);
                transport
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut client = Conn::<ClientRole>::new();
                let headers =
                    HeaderBlock::for_request(b"/svc/Method", None, &Metadata::new()).unwrap();
                let fields = headers.as_h2();
                assert_eq!(client.start_request(&fields, true).unwrap(), StreamId(1));
                assert_eq!(client.start_request(&fields, true).unwrap(), StreamId(3));
                transport.write_all(client.outbound()).unwrap();
                client.drain_outbound(client.outbound().len());

                let mut bytes = [0u8; 4096];
                let read = transport.read(&mut bytes).unwrap();
                client.ingest(&bytes[..read]).unwrap();
                assert_eq!(client.peer_settings().max_concurrent_streams, Some(1));
                let mut refused = false;
                while let Some(event) = client.poll_event() {
                    if let conn::Event::StreamReset { stream_id, error } = event {
                        refused |= stream_id == StreamId(3) && error == ErrorCode::RefusedStream;
                    }
                }
                assert!(refused);
            },
        )
        .unwrap();
}
