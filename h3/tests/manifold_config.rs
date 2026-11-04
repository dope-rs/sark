use dope_quic::{
    config, conn::life::options::Options, proto::cid, runtime::endpoint::config::Config,
};
use sark_h3::manifold::server::{self, ConfigError, startup};

#[test]
fn rejects_fixed_layout() {
    let Ok(overflow) = usize::try_from(u64::from(u32::MAX) + 1) else {
        return;
    };
    let result = startup::Plan::<0>::try_new(
        "127.0.0.1:0".parse().expect("loopback bind address"),
        config::raw::Identity::from_seed(&[0x6a; 32])
            .expect("fixture signing key")
            .server()
            .expect("fixture TLS config"),
        Options::default(),
        Config {
            max_conns: 1,
            outgoing_capacity: 1,
            outgoing_bytes_capacity: 1200,
            packet_buffer_slots: 1,
            packet_buffer_bytes: 1200,
            cid_rotation: cid::Rotation::disabled(),
            opening_lifetime: None,
        },
        server::Config {
            max_connection_body_chunks: overflow,
            ..server::Config::default()
        },
    );

    assert!(
        matches!(result, Err(startup::PrepareError::Application(ConfigError::Chunks(error))) if error.requested() == overflow)
    );
}
