use sark_h2::{
    ServerRole,
    conn::{
        config,
        config::{Config, Settings, capacity},
        machine::Connection,
    },
    frame::numbers,
};

#[test]
fn payload_bounds() {
    use config::payload::Error;

    assert!(matches!(capacity::Payload::new(0, 1), Err(Error::ZeroData)));
    assert!(matches!(
        capacity::Payload::new(1, 0),
        Err(Error::ZeroHeaders)
    ));
    assert!(matches!(
        capacity::Payload::new(usize::MAX, 1),
        Err(Error::SlotOverflow {
            data: usize::MAX,
            headers: 1,
        })
    ));
    assert!(matches!(
        capacity::Payload::new(usize::MAX - 1, 1),
        Err(Error::IngressOverflow { slots: usize::MAX })
    ));
    let data = usize::MAX / 2 + 1;
    assert!(matches!(
        capacity::Payload::new(data, 1),
        Err(Error::SegmentOverflow {
            data: actual,
            ingress,
        }) if actual == data && ingress == data + 2
    ));
    let base = (capacity::Payload::new(1, 1)).expect("positive payload capacities must fit");
    assert!(matches!(
        base.with_arena(2),
        Err(Error::ArenaTooSmall {
            required: 3,
            actual: 2,
        })
    ));
    let full =
        (base.with_arena(usize::MAX)).expect("the maximum representable arena must be valid");
    assert!(matches!(
        full.extend(1),
        Err(Error::RetentionOverflow {
            segments: usize::MAX,
            extra: 1,
        })
    ));
}

#[test]
fn config_checks_bounds() {
    assert!(matches!(
        config::Validated::<ServerRole>::new(Config {
            event_capacity: 0,
            ..Config::default()
        }),
        Err(config::Error::ZeroCapacity(config::Field::Event))
    ));
    assert!(matches!(
        config::Validated::<ServerRole>::new(Config {
            recv_window_target: 0x8000_0000,
            ..Config::default()
        }),
        Err(config::Error::ReceiveWindowOverflow {
            actual: 0x8000_0000,
            max: 0x7fff_ffff,
        })
    ));
    assert!(matches!(
        config::Validated::<ServerRole>::new(Config {
            local_settings: Settings {
                initial_window_size: 0x8000_0000,
                ..Settings::DEFAULT
            },
            ..Config::default()
        }),
        Err(config::Error::InitialWindowOverflow {
            actual: 0x8000_0000,
            max: 0x7fff_ffff,
        })
    ));
    assert!((numbers::Limit::new(16_383)).is_none());
    assert!(matches!(
        config::Validated::<ServerRole>::new(Config {
            hpack_encoder: capacity::Encoder {
                header_list: usize::MAX,
                ..capacity::Encoder::default()
            },
            ..Config::default()
        }),
        Err(config::Error::HpackBlockOverflow { list: usize::MAX })
    ));
    let list = usize::MAX - 12;
    assert!(matches!(
        config::Validated::<ServerRole>::new(Config {
            hpack_encoder: capacity::Encoder {
                header_list: list,
                ..capacity::Encoder::default()
            },
            ..Config::default()
        }),
        Err(config::Error::HpackWireOverflow { list: actual }) if actual == list
    ));
    assert!(matches!(
        config::Validated::<ServerRole>::new(Config {
            outbound_capacity: 1,
            ..Config::default()
        }),
        Err(config::Error::OutboundTooSmall { .. })
    ));
    assert!(matches!(
        config::Validated::<ServerRole>::new(Config {
            inbound_capacity: usize::MAX,
            ..Config::default()
        }),
        Err(config::Error::IngressCount(_))
    ));
    let payload = (capacity::Payload::default().with_arena(u32::MAX as usize + 1))
        .expect("the arena must represent a count wider than the retained limit");
    assert!(matches!(
        config::Validated::<ServerRole>::new(Config {
            payload_capacity: payload,
            ..Config::default()
        }),
        Err(config::Error::RetentionCount(_))
    ));
    assert!(matches!(
        config::Validated::<ServerRole>::new(Config {
            outbound_capacity: usize::MAX,
            ..Config::default()
        }),
        Err(config::Error::OutboundCapacityOverflow {
            logical,
            source,
        }) if logical.get() == usize::MAX
            && source.capacity == logical && source.reserved == 13
    ));
}

#[test]
fn config_proof_reuses() {
    let config = (config::Validated::<ServerRole>::new(Config::default()))
        .expect("the default server configuration must be valid");
    let first = (Connection::<ServerRole>::from_plan(config))
        .expect("the first proof copy must construct a connection");
    let second = (Connection::<ServerRole>::from_plan(config))
        .expect("the second proof copy must construct a connection");
    assert_eq!(
        (first.status().active_len(), second.status().active_len()),
        (0, 0)
    );
}
