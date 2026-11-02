use crate::common::message::capacity::Capacity as _;
pub mod common;

use sark_h3::conn::options::{Arithmetic, Bound, Config, Error, Resource, plan};

#[test]
fn rejects_invalid_bounds() {
    assert_zero(
        Resource::Frame,
        Config::edited(|config| config.protocol.max_frame_bytes = 0),
    );
    assert_zero(
        Resource::Stream,
        Config::edited(|config| config.streams.capacity = 0),
    );
    assert_zero(
        Resource::Event,
        Config::edited(|c| c.ingress.event_capacity = 0),
    );
    assert_zero(
        Resource::Write,
        Config::edited(|c| c.egress.write_capacity = 0),
    );
    assert_zero(
        Resource::QpackSection,
        Config::edited(|c| c.qpack.encoder.section_capacity = 0),
    );
    assert_zero(
        Resource::OutboundByte,
        Config::edited(|c| c.egress.wire_bytes = 0),
    );
    assert_zero(
        Resource::OutboundResidentByte,
        Config::edited(|c| c.egress.resident_bytes = 0),
    );
    assert_zero(
        Resource::InboundByte,
        Config::edited(|c| c.ingress.retained_bytes = 0),
    );
    assert_zero(
        Resource::InboundSegment,
        Config::edited(|c| c.ingress.retained_segments = 0),
    );

    for resource in [
        Resource::Write,
        Resource::OutboundByte,
        Resource::OutboundResidentByte,
    ] {
        let config = Config::edited(|config| match resource {
            Resource::Write => config.egress.write_capacity = 1,
            Resource::OutboundByte => config.egress.wire_bytes = 1,
            Resource::OutboundResidentByte => config.egress.resident_bytes = 1,
            _ => panic!("fixture egress resource"),
        });
        assert!(matches!(
            plan::Config::new(config),
            Err(Error::Below { resource: actual, actual: 1, minimum: 2 }) if actual == resource
        ));
    }

    assert!(matches!(
        plan::Config::new(Config::edited(
            |c| c.ingress.field_section_bytes = usize::MAX
        )),
        Err(Error::Capacity {
            resource: Resource::FieldSection,
            requested: usize::MAX,
            error: Bound::Wire
        })
    ));
    assert!(matches!(
        plan::Config::new(Config::edited(|c| c.streams.capacity = usize::MAX)),
        Err(Error::Capacity { resource: Resource::Stream, requested: usize::MAX, error: Bound::Depot(error) })
            if error.requested() == usize::MAX
    ));
    for (resource, size) in [
        (Resource::QpackEncoderTable, usize::MAX),
        (Resource::QpackDecoderTable, (1_usize << 62) - 1),
    ] {
        let config = Config::edited(|c| match resource {
            Resource::QpackEncoderTable => c.qpack.encoder.table_bytes = size,
            Resource::QpackDecoderTable => c.qpack.decoder.table_bytes = size,
            _ => panic!("fixture table resource"),
        });
        assert!(matches!(
            plan::Config::new(config),
            Err(Error::Table { resource: found, requested: want,
                error: sark_http::payload::compress::dynamic::plan::Error::Capacity { actual, maximum } })
                if found == resource && want == size && actual == size && maximum < actual
        ));
    }
    assert!(matches!(
        plan::Config::new(Config::edited(|c| c.ingress.retained_segments = usize::MAX)),
        Err(Error::Arithmetic {
            resource: Resource::InboundSegment,
            operation: Arithmetic::Mul,
            left: usize::MAX,
            right: 2
        })
    ));

    let bad = Config::edited(|config| {
        config.streams.capacity = 1;
        config.qpack.decoder.blocked_streams = 2;
    });
    assert!(matches!(
        plan::Config::new(bad),
        Err(Error::Exceeds {
            resource: Resource::BlockedStream,
            limit: Resource::Stream,
            actual: 2,
            maximum: 1,
        })
    ));
}

fn assert_zero(resource: Resource, config: Config) {
    assert!(matches!(
        plan::Config::new(config),
        Err(Error::Zero(actual)) if actual == resource
    ));
}
