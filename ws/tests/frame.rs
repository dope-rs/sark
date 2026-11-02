use o3::buffer::Residency as _;
use sark_ws::frame;

#[test]
fn rejects_invalid_plans() {
    for opcode in [0x3, 0x7, 0xb, 0xf, 0x10, 0x82, 0xff] {
        assert_eq!(
            frame::Plan::new(opcode, true, 0, false).err(),
            Some(frame::PlanError::Opcode { actual: opcode })
        );
    }
    for opcode in [0x8, 0x9, 0xa] {
        for (fin, length) in [(false, 0), (true, 126)] {
            assert_eq!(
                frame::Plan::new(opcode, fin, length, true).err(),
                Some(frame::PlanError::Control { fin, length })
            );
        }
    }
    for masked in [false, true] {
        assert_eq!(
            frame::Plan::new(0x2, true, usize::MAX, masked).err(),
            Some(frame::PlanError::Length {
                payload: usize::MAX,
                masked,
            })
        );
    }
}

#[test]
fn encodes_admitted_flags() {
    for (opcode, fin, first) in [
        (0x0, false, 0x00),
        (0x0, true, 0x80),
        (0x1, false, 0x01),
        (0x1, true, 0x81),
        (0x2, false, 0x02),
        (0x2, true, 0x82),
        (0x8, true, 0x88),
        (0x9, true, 0x89),
        (0xa, true, 0x8a),
    ] {
        let plan = frame::Plan::new(opcode, fin, 125, false).expect("admitted frame shape");
        let mut wire = [0; 127];
        plan.bind(&[0x5a; 125], None)
            .expect("matching payload")
            .bind(&mut wire)
            .expect("matching capacity")
            .encode();
        assert_eq!(wire[..2], [first, 125]);
    }
}

#[test]
fn parses_boundaries() {
    let cases: &[(usize, &[u8])] = &[
        (0, &[0x82, 0]),
        (125, &[0x82, 125]),
        (126, &[0x82, 126, 0, 126]),
        (65_535, &[0x82, 126, 0xff, 0xff]),
        (65_536, &[0x82, 127, 0, 0, 0, 0, 0, 1, 0, 0]),
        (16 * 1024 * 1024, &[0x82, 127, 0, 0, 0, 0, 1, 0, 0, 0]),
    ];
    for &(len, wire) in cases {
        let head = frame::Head::parse(wire, 0, len)
            .expect("valid frame fixture")
            .expect("complete frame fixture");
        assert_eq!(head.payload_len(), len);
        assert_eq!(head.start().get(), wire.len());
    }
}

#[test]
fn rejects_noncanonical() {
    assert_eq!(
        frame::Head::parse(&[0x82, 126, 0, 125], 0, usize::MAX)
            .err()
            .expect("short extended length must be rejected"),
        frame::Error::NonCanonical {
            marker: 126,
            length: 125,
        }
    );

    let mut wire = vec![0x82, 127];
    wire.extend_from_slice(&65_535u64.to_be_bytes());
    assert_eq!(
        frame::Head::parse(&wire, 0, usize::MAX)
            .err()
            .expect("noncanonical wide length must be rejected"),
        frame::Error::NonCanonical {
            marker: 127,
            length: 65_535,
        }
    );
}

#[test]
fn outputs_match() {
    let data = b"causal-control-causal-control-causal-control!";
    let mask = [0x11, 0x22, 0x33, 0x44];
    let plan = frame::Plan::new(0x9, true, data.len(), true).expect("wire length");
    let mut direct = vec![0xa5; plan.wire_len() + 3];
    assert_eq!(
        plan.bind(data, Some(mask))
            .expect("matching payload and mask")
            .bind(&mut direct)
            .expect("roomy destination")
            .encode(),
        plan.wire_len()
    );

    let owned = plan
        .bind(data, Some(mask))
        .expect("matching payload and mask")
        .try_owned()
        .expect("exact owned frame");
    assert_eq!(owned.as_ref(), &direct[..plan.wire_len()]);
    assert_eq!(&direct[plan.wire_len()..], &[0xa5; 3]);
    assert_eq!(
        owned.as_ref(),
        &[
            0x89, 0xad, 0x11, 0x22, 0x33, 0x44, 0x72, 0x43, 0x46, 0x37, 0x70, 0x4e, 0x1e, 0x27,
            0x7e, 0x4c, 0x47, 0x36, 0x7e, 0x4e, 0x1e, 0x27, 0x70, 0x57, 0x40, 0x25, 0x7d, 0x0f,
            0x50, 0x2b, 0x7f, 0x56, 0x41, 0x2b, 0x7d, 0x0f, 0x50, 0x25, 0x64, 0x51, 0x52, 0x28,
            0x3c, 0x41, 0x5c, 0x2a, 0x65, 0x50, 0x5c, 0x28, 0x30,
        ]
    );
    assert_eq!(owned.len(), plan.wire_len());
    assert_eq!(owned.resident(), plan.wire_len());
}

#[test]
fn bind_rejects_drift() {
    let plan = frame::Plan::new(0xa, true, 2, false).expect("wire length");
    assert!(matches!(
        plan.bind(b"x", None),
        Err(frame::BindError::Payload {
            expected: 2,
            actual: 1,
        })
    ));
    assert!(matches!(
        plan.bind(b"ok", Some([0; 4])),
        Err(frame::BindError::Mask {
            expected: false,
            actual: true,
        })
    ));
    let frame = plan.bind(b"ok", None).expect("matching frame");
    let mut short = [0xa5; 3];
    assert!(matches!(
        frame.bind(&mut short),
        Err(frame::BindError::Capacity {
            required: 4,
            available: 3,
        })
    ));
    assert_eq!(short, [0xa5; 3]);
}

#[test]
fn owned_masked_extended() {
    let data = vec![0x5a; 65_537];
    let plan = frame::Plan::new(0x2, true, data.len(), true).expect("admitted");
    let frame = plan.bind(&data, Some([1, 2, 3, 4])).expect("admitted");
    let mut direct = vec![0; plan.wire_len()];
    frame.bind(&mut direct).expect("admitted").encode();
    let owned = frame.try_owned().expect("admitted");
    assert_eq!(owned.as_ref(), direct);
    assert_eq!(owned.resident(), plan.wire_len());
}
