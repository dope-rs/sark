use sark_grpc::{
    Code,
    client::{
        call::bidi::Session as _,
        config::{Limits, Output},
        io::ingress::Session as _,
        session::Session,
    },
    metadata::Metadata,
    protocol::section,
};
use sark_h2::{
    ServerRole, client,
    conn::{config, machine::Connection},
};

#[test]
fn enforces_output_slots() {
    let limits = Limits::default()
        .try_calls(2)
        .expect("two concurrent calls fit the client limits")
        .try_bytes(64)
        .expect("64 message bytes fit the client limits")
        .try_output(Output::try_new(1).expect("one output slot is nonzero"))
        .expect("one output slot fits the client limits");
    Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let server = Connection::<ServerRole>::with_config(config::Config {
                local_settings: config::Settings {
                    initial_window_size: 0,
                    ..config::Settings::DEFAULT
                },
                ..config::Config::default()
            })
            .expect("zero initial stream credit is valid server configuration");
            client
                .ingest(server.output().first())
                .expect("client accepts the server settings");

            let mut first = client
                .open(b"/svc/Method", None, &Metadata::new())
                .expect("first stream fits the two-call limit");
            client
                .send_raw(&mut first, b"first")
                .expect("first message reserves the sole output slot");
            let mut second = client
                .open(b"/svc/Method", None, &Metadata::new())
                .expect("second stream fits the two-call limit");
            assert!(matches!(
                client.send_raw(&mut second, b"second"),
                Err(status) if status.code() == Code::ResourceExhausted
            ));
        },
    )
    .expect("client session completes after the expected capacity rejection");
}
