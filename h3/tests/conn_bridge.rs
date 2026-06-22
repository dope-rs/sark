use std::time::Instant;

use dope_quic::{Conn, ConnConfig, transport_params};
use ring::rand::{SecureRandom, SystemRandom};
use sark_core::http::Field;
use sark_h3::dope::Session;
use sark_h3::{Event, Role, StreamId};
use shin::sig::SigningKey;

const CID: [u8; 8] = [0x13, 0x37, 0x13, 0x37, 0x13, 0x37, 0x13, 0x37];

fn config() -> ConnConfig {
    ConnConfig {
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

fn pair() -> (Conn, Conn) {
    let mut seed = [0u8; 32];
    SystemRandom::new().fill(&mut seed).unwrap();
    let signing = SigningKey::from_seed(&seed).unwrap();
    let server_pubkey = *signing.pubkey();
    let mut server = Conn::new_server(CID.to_vec(), CID.to_vec(), CID.to_vec(), signing, config());
    let mut client = Conn::new_client(CID.to_vec(), CID.to_vec(), server_pubkey, config());
    let now = Instant::now();
    for _ in 0..3 {
        drain(&mut client, &mut server, now);
        drain(&mut server, &mut client, now);
    }
    assert!(client.is_established());
    assert!(server.is_established());
    (server, client)
}

fn drain(from: &mut Conn, into: &mut Conn, now: Instant) {
    for pkt in from.send_packets(now) {
        into.recv_packet(&pkt, now).expect("recv");
    }
}

fn pump_quic_events(session: &mut Session, quic: &mut Conn) {
    while let Some(event) = quic.poll_stream_event() {
        session.on_quic_stream_event(quic, event).unwrap();
    }
}

#[test]
fn settings_exchange_and_request_stream_round_trip_over_quic() {
    let (mut server_quic, mut client_quic) = pair();
    let mut server_h3 = Session::with_role(Role::Server);
    let mut client_h3 = Session::with_role(Role::Client);

    server_h3.start_control_stream(&mut server_quic).unwrap();
    client_h3.start_control_stream(&mut client_quic).unwrap();

    let t1 = Instant::now();
    drain(&mut client_quic, &mut server_quic, t1);
    pump_quic_events(&mut server_h3, &mut server_quic);
    drain(&mut server_quic, &mut client_quic, t1);
    pump_quic_events(&mut client_h3, &mut client_quic);

    assert!(matches!(server_h3.poll_event(), Some(Event::Settings(_))));
    assert!(matches!(client_h3.poll_event(), Some(Event::Settings(_))));

    let stream_id = client_h3.open_request_stream(&mut client_quic).unwrap();
    client_h3
        .h3_mut()
        .send_headers(
            StreamId::new(stream_id),
            [
                Field::new(b":method", b"POST"),
                Field::new(b":scheme", b"https"),
                Field::new(b":path", b"/pkg.Service/Method"),
            ],
            false,
        )
        .unwrap();
    client_h3
        .h3_mut()
        .send_data(StreamId::new(stream_id), b"hello", true)
        .unwrap();
    client_h3.flush(&mut client_quic);

    let t2 = Instant::now();
    drain(&mut client_quic, &mut server_quic, t2);
    pump_quic_events(&mut server_h3, &mut server_quic);

    assert!(matches!(
        server_h3.poll_event(),
        Some(Event::Headers {
            stream_id: StreamId(0),
            ..
        })
    ));
    assert_eq!(
        server_h3.poll_event(),
        Some(Event::Data {
            stream_id: StreamId(0),
            data: b"hello".to_vec()
        })
    );
    assert_eq!(
        server_h3.poll_event(),
        Some(Event::Finished {
            stream_id: StreamId(0)
        })
    );
}
