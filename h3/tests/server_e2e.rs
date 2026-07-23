use std::time::Instant;

use dope_quic::{Conn, ConnHandle, Handler, ServerConn, conn, transport_params};
use ring::rand::{SecureRandom, SystemRandom};
use sark_core::http::Field;
use sark_h3::dope::{Server, Session};
use sark_h3::{Event, Role, StreamId};
use shin::sig::SigningKey;

const CID: [u8; 8] = [0x51, 0x99, 0x51, 0x99, 0x51, 0x99, 0x51, 0x99];

#[sark_gen::response(raw)]
struct Reply {
    status: http::StatusCode,
    body: &'static [u8],
}

#[sark_gen::request]
struct JsonReq {}

#[sark_gen::handler]
fn json_h(_req: JsonReq, _state: &sark::EmptyState) -> Reply {
    Reply {
        status: http::StatusCode::OK,
        body: b"hi-server",
    }
}

sark_gen::define_route! {
    SrvApp: sark::EmptyState => {
        GET "/json" => json_h,
    }
}

fn config() -> conn::Config {
    conn::Config {
        transport_params: transport_params::Params {
            max_idle_timeout_ms: 30_000,
            initial_max_data: 1 << 20,
            initial_max_stream_data_bidi_local: 1 << 20,
            initial_max_stream_data_bidi_remote: 1 << 20,
            initial_max_stream_data_uni: 1 << 20,
            initial_max_streams_bidi: 8,
            initial_max_streams_uni: 8,
            ..transport_params::Params::default()
        },
        ..Default::default()
    }
}

fn pair() -> (ServerConn, Conn) {
    let mut seed = [0u8; 32];
    SystemRandom::new().fill(&mut seed).unwrap();
    let signing = SigningKey::from_seed(&seed).unwrap();
    let server_pubkey = *signing.pubkey().unwrap();
    let mut server =
        Conn::new_server(CID.to_vec(), CID.to_vec(), CID.to_vec(), signing, config()).unwrap();
    let mut client = Conn::new_client(CID.to_vec(), CID.to_vec(), server_pubkey, config()).unwrap();
    let now = Instant::now();
    for _ in 0..3 {
        drain_client(&mut client, &mut server, now);
        drain_server(&mut server, &mut client, now);
    }
    assert!(client.is_established());
    assert!(server.is_established());
    (server, client)
}

fn drain_client(from: &mut Conn, into: &mut ServerConn, now: Instant) {
    for pkt in from.send_packets(now) {
        into.recv_packet(&pkt, now).expect("recv");
    }
}

fn drain_server(from: &mut ServerConn, into: &mut Conn, now: Instant) {
    for pkt in from.send_packets(now) {
        into.recv_packet(&pkt, now).expect("recv");
    }
}

fn pump_client(session: &mut Session, quic: &mut Conn) {
    while let Some(event) = quic.poll_stream_event() {
        session.quic_stream_event(quic, event).unwrap();
    }
}

#[test]
fn server_handler_routes_over_quic() {
    let (mut server_quic, mut client_quic) = pair();
    let handle = ConnHandle(0);

    let timer = sark::Timer::with_capacity(0);
    let app = SrvApp::new::<dope_net::wire::identity::Identity>(
        sark::EmptyState::REF,
        &timer,
        sark::app::Config {
            timer_capacity: 0,
            task_capacity: 0,
        },
    );
    let mut server = Server::new(app);
    server.established(&mut server_quic, handle);

    let mut client = Session::with_role(Role::Client);
    client.start_control_stream(&mut client_quic).unwrap();

    let now = Instant::now();
    drain_client(&mut client_quic, &mut server_quic, now);
    while let Some(event) = server_quic.poll_stream_event() {
        server.stream_event(&mut server_quic, handle, event);
    }
    drain_server(&mut server_quic, &mut client_quic, now);
    pump_client(&mut client, &mut client_quic);

    assert!(matches!(client.poll_event(), Some(Event::Settings(_))));

    let stream_id = client.open_request_stream(&mut client_quic).unwrap();
    client
        .h3_mut()
        .send_headers(
            StreamId::new(stream_id),
            [
                Field::new(b":method", b"GET"),
                Field::new(b":scheme", b"https"),
                Field::new(b":path", b"/json"),
            ],
            false,
        )
        .unwrap();
    client
        .h3_mut()
        .send_headers(
            StreamId::new(stream_id),
            [Field::new(b"x-request-finished", b"true")],
            true,
        )
        .unwrap();
    client.flush(&mut client_quic).unwrap();

    drain_client(&mut client_quic, &mut server_quic, now);
    while let Some(event) = server_quic.poll_stream_event() {
        server.stream_event(&mut server_quic, handle, event);
    }
    drain_server(&mut server_quic, &mut client_quic, now);
    pump_client(&mut client, &mut client_quic);

    let mut status_ok = false;
    let mut body_ok = false;
    while let Some(event) = client.poll_event() {
        match event {
            Event::Headers { fields, .. }
                if fields
                    .iter()
                    .any(|f| f.name == b":status" && f.value == b"200") =>
            {
                status_ok = true;
            }
            Event::Data { data, .. } if data == b"hi-server" => {
                body_ok = true;
            }
            _ => {}
        }
    }
    assert!(
        status_ok,
        "client must receive :status 200 from routed handler"
    );
    assert!(body_ok, "client must receive handler body");
}
