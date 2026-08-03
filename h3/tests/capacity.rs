use sark_h3::{
    Config, ConfigError, Conn, ConnError, Role, STREAM_TYPE_CONTROL, StreamId, ValidatedConfig,
};

fn config() -> Config {
    Config {
        stream_capacity: 1,
        event_capacity: 1,
        write_capacity: 1,
        ..Config::default()
    }
}

#[test]
fn config_rejects_unusable_or_unrepresentable_bounds() {
    for (name, broken) in [
        (
            "frame",
            Config {
                max_frame_size: 0,
                ..Config::default()
            },
        ),
        (
            "stream",
            Config {
                stream_capacity: 0,
                ..Config::default()
            },
        ),
        (
            "event",
            Config {
                event_capacity: 0,
                ..Config::default()
            },
        ),
        (
            "write",
            Config {
                write_capacity: 0,
                ..Config::default()
            },
        ),
    ] {
        assert!(matches!(
            ValidatedConfig::new(broken),
            Err(ConfigError::ZeroCapacity(actual)) if actual == name
        ));
    }

    let mut invalid_setting = Config::default();
    invalid_setting.local_settings.qpack_max_table_capacity = u64::MAX;
    assert!(matches!(
        ValidatedConfig::new(invalid_setting),
        Err(ConfigError::InvalidSetting("qpack table capacity"))
    ));
    assert!(matches!(
        ValidatedConfig::new(Config {
            stream_capacity: usize::MAX,
            ..Config::default()
        }),
        Err(ConfigError::CapacityOverflow("stream"))
    ));
}

#[test]
fn validated_config_is_reusable_without_revalidation() {
    let config = ValidatedConfig::new(config()).unwrap();
    let _client = Conn::from_config(Role::Client, config.clone());
    let _server = Conn::from_config(Role::Server, config);
}

#[test]
fn bounded_queues_report_overload_and_reuse_slots() {
    let mut events = Conn::with_config(Role::Server, config()).unwrap();
    events.ingest_stopped(StreamId::new(0), 1).unwrap();
    assert_eq!(
        events.ingest_stopped(StreamId::new(4), 1),
        Err(ConnError::Overload)
    );
    assert!(events.poll_event().is_some());
    events.ingest_stopped(StreamId::new(4), 1).unwrap();

    let mut writes = Conn::with_config(Role::Server, config()).unwrap();
    writes.send_data(StreamId::new(0), b"a", false).unwrap();
    assert_eq!(
        writes.send_data(StreamId::new(0), b"b", false),
        Err(ConnError::Overload)
    );
    assert!(writes.poll_write().is_some());
    writes.send_data(StreamId::new(0), b"c", false).unwrap();

    let mut critical = Conn::with_config(Role::Server, config()).unwrap();
    critical.start_control_stream(StreamId::new(3)).unwrap();
    assert_eq!(
        critical.start_qpack_encoder_stream(StreamId::new(7)),
        Err(ConnError::Overload)
    );
    assert!(critical.poll_write().is_some());
    critical
        .start_qpack_encoder_stream(StreamId::new(7))
        .unwrap();
}

#[test]
fn bounded_stream_table_reports_overload_and_reuses_slots() {
    let mut conn = Conn::with_config(Role::Server, config()).unwrap();
    conn.ingest_stream(StreamId::new(0), &[], false).unwrap();
    assert_eq!(
        conn.ingest_stream(StreamId::new(4), &[], false),
        Err(ConnError::Overload)
    );
    conn.ingest_reset(StreamId::new(0), 0).unwrap();
    let _ = conn.poll_event();
    conn.ingest_stream(StreamId::new(4), &[], false).unwrap();
}

#[test]
fn critical_stream_state_is_singleton_and_cannot_be_recycled() {
    let mut conn = Conn::with_config(Role::Server, config()).unwrap();
    let mut stream_type = Vec::new();
    STREAM_TYPE_CONTROL.encode(&mut stream_type);
    conn.ingest_stream(StreamId::new(2), &stream_type, false)
        .unwrap();
    assert_eq!(
        conn.ingest_reset(StreamId::new(2), 0),
        Err(ConnError::ClosedCriticalStream)
    );
    assert_eq!(
        conn.ingest_stream(StreamId::new(6), &stream_type, false),
        Err(ConnError::StreamCreation)
    );
}
