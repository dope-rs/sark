pub mod common;

use common::discard::DATE;
use dope_test::alloc::Tracker;
use sark_h1::{pump, route::Driver, server::deadline, session::Conn, wire::head};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn resumes_drained_tail() {
    use crate::common::discard::{HEAD, Router, TAIL};
    for retained in [false, true] {
        let router = Router::new(&());
        let mut driver = Driver::try_new(&router, *DATE).expect("route driver");
        let mut conn = Conn::default();
        let mut write = [0; 2048];
        let cycle = driver.ingest(&mut conn, HEAD, &mut write, head::Connection::KeepAlive);
        assert_eq!(cycle.discard_remaining, 4);
        assert!(matches!(cycle.progress.head(), deadline::Head::Body));
        let prefix = match cycle.state {
            Ok(pump::Egress::Write {
                prefix,
                body: pump::Body::None,
            }) => Some(prefix),
            _ => None,
        }
        .expect("response");
        assert!(matches!(
            pump::Drain::complete(&mut conn),
            pump::Completion::None
        ));

        let input = if retained {
            let cycle = driver.retain(&mut conn, TAIL);
            assert!(matches!(cycle.state, Ok(pump::Egress::Wake)));
            assert!(matches!(cycle.progress.head(), deadline::Head::Pending));
            &[][..]
        } else {
            TAIL
        };
        let cycle = driver.ingest(&mut conn, input, &mut write, head::Connection::KeepAlive);
        assert!(matches!(cycle.progress.head(), deadline::Head::Ready));
        assert!(matches!(cycle.state,
            Ok(pump::Egress::Write { prefix: written, body: pump::Body::None }) if written == prefix
        ));
        assert!(write[..prefix].ends_with(b"\r\n\r\nok"));
        assert!(matches!(
            pump::Drain::complete(&mut conn),
            pump::Completion::None
        ));
    }
}

#[test]
fn discarded_input_finishes() {
    use crate::common::discard::{HEAD, Router};
    let router = Router::new(&());
    let mut driver = Driver::try_new(&router, *DATE).expect("route driver");
    let mut conn = Conn::default();
    let mut write = [0; 2048];
    for input in [HEAD, b"bo"] {
        let cycle = driver.ingest(&mut conn, input, &mut write, head::Connection::KeepAlive);
        assert!(matches!(cycle.progress.head(), deadline::Head::Body));
    }
    let cycle = driver.ingest(&mut conn, b"dy", &mut write, head::Connection::KeepAlive);
    assert_eq!(cycle.discard_remaining, 0);
    assert!(matches!(cycle.progress.head(), deadline::Head::Ready));
}

#[test]
fn discarded_method_body_policy() {
    use sark_http::h1::codec::canned;

    use crate::common::discard::{BODY_POLICY, Router, SUCCESS};

    let router = Router::new(&());
    let mut driver = Driver::try_new(&router, *DATE).expect("route driver");
    for &(input, expected) in BODY_POLICY {
        let mut conn = Conn::default();
        let mut write = [0; 512];
        let cycle = driver.ingest(&mut conn, input, &mut write, head::Connection::KeepAlive);
        assert_eq!(cycle.discard_remaining, 0);
        if expected == 200 {
            assert!(matches!(cycle.state,
                Ok(pump::Egress::Write { prefix, body: pump::Body::None })
                    if write[..prefix].starts_with(SUCCESS)
            ));
        } else {
            let expected = match expected {
                400 => canned::BAD_REQUEST,
                413 => canned::PAYLOAD_TOO_LARGE,
                _ => panic!("body policy rejection"),
            };
            assert!(matches!(cycle.connection, head::Connection::Close));
            assert!(matches!(cycle.state,
                Ok(pump::Egress::Write { prefix: 0, body: pump::Body::Static(reply) })
                    if reply.as_slice() == expected.as_slice()
            ));
        }
    }
}

#[test]
fn rejects_after_admission() {
    use sark_h1::{pump, route::Driver, session::Conn, wire::head};
    use sark_http::h1::codec::canned;

    use crate::common::discard::{MISMATCHED_LENGTH, MISSING_HEAD, Router};

    for split in [8, MISSING_HEAD.len()] {
        for (tail, expected) in [
            (&b"\r\n"[..], canned::NOT_FOUND),
            (MISMATCHED_LENGTH, canned::BAD_REQUEST),
        ] {
            let router = Router::new(&());
            let mut driver = Driver::try_new(&router, *DATE).expect("driver");
            let mut conn = Conn::default();
            let mut write = [0; 512];
            let mut input = MISSING_HEAD.to_vec();
            for part in input.chunks(split) {
                let first = driver.ingest(&mut conn, part, &mut write, head::Connection::KeepAlive);
                assert!(matches!(first.state, Ok(pump::Egress::None)));
            }
            input.fill(b'!');
            let last = driver.ingest(&mut conn, tail, &mut write, head::Connection::KeepAlive);
            assert!(matches!(last.state,
                Ok(pump::Egress::Write { prefix: 0, body: pump::Body::Static(reply) })
                    if reply.as_slice() == expected.as_slice()
            ));
        }
    }
}

#[test]
fn preserves_head_lease() {
    use crate::common::discard::{HEAD, MISSING_HEAD, PREFIX, Router};
    let router = Router::new(&());
    let mut driver = Driver::try_new(&router, *DATE).expect("driver");
    for partial in [PREFIX, &HEAD[..HEAD.len() - 2], MISSING_HEAD] {
        let mut conn = Conn::default();
        let mut write = [0; 2048];
        let cycle = driver.ingest(&mut conn, partial, &mut write, head::Connection::KeepAlive);
        let mut progress = cycle.progress;
        assert!(matches!(progress.head(), deadline::Head::Pending));
        let cycle = driver.ingest(&mut conn, &[], &mut write, head::Connection::KeepAlive);
        assert!(matches!(cycle.progress.head(), deadline::Head::Unchanged));
        progress.merge(cycle.progress);
        let cycle = driver.retain(&mut conn, &[]);
        assert!(matches!(cycle.progress.head(), deadline::Head::Unchanged));
        progress.merge(cycle.progress);
        assert!(matches!(progress.head(), deadline::Head::Pending));
    }
}

#[test]
fn fragmented_line_pipeline() {
    use crate::common::discard::{PIPELINE, PREFIX, Router, SUCCESS};
    let router = Router::new(&());
    let mut driver = Driver::try_new(&router, *DATE).expect("driver");
    let mut conn = Conn::default();
    let mut write = [0; 2048];
    let mut input = [0];
    for byte in PREFIX {
        input[0] = *byte;
        let cycle = driver.ingest(&mut conn, &input, &mut write, head::Connection::KeepAlive);
        assert!(matches!(cycle.state, Ok(pump::Egress::None)));
    }
    let cycle = driver.ingest(
        &mut conn,
        &PIPELINE[PREFIX.len()..],
        &mut write,
        head::Connection::KeepAlive,
    );
    let prefix = match cycle.state {
        Ok(pump::Egress::Write {
            prefix,
            body: pump::Body::None,
        }) => Some(prefix),
        _ => None,
    }
    .expect("responses");
    assert_eq!(
        write[..prefix]
            .windows(SUCCESS.len())
            .filter(|part| *part == SUCCESS)
            .count(),
        2
    );
    assert!(matches!(
        pump::Drain::complete(&mut conn),
        pump::Completion::None
    ));
}

#[test]
fn retains_upload_pipeline() {
    use crate::common::discard::{PIPELINE, Request, Router, SUCCESS};

    let input = Request::pipeline().expect("request pipeline");
    let router = Router::new(&());
    let mut driver = Driver::try_new(&router, *DATE).expect("driver");
    for buffered in [false, true] {
        let mut conn = Conn::default();
        if buffered {
            let cycle = driver.retain(&mut conn, &PIPELINE[..PIPELINE.len() / 2]);
            assert!(matches!(cycle.state, Ok(pump::Egress::Wake)));
            let mut write = [0; 1024];
            let cycle = driver.ingest(&mut conn, &input, &mut write, head::Connection::KeepAlive);
            assert!(matches!(cycle.state,
                Ok(pump::Egress::Write { prefix, body: pump::Body::None })
                    if write[..prefix].starts_with(SUCCESS)
                        && write[..prefix].ends_with(b"\r\n\r\nok")
            ));
            assert!(matches!(
                pump::Drain::complete(&mut conn),
                pump::Completion::Resume
            ));
        } else {
            for part in input.chunks(64 * 1024) {
                let cycle = driver.retain(&mut conn, part);
                assert!(matches!(cycle.state, Ok(pump::Egress::Wake)));
            }
        }
        let mut write = [0; 2048];
        let cycle = driver.ingest(&mut conn, &[], &mut write, head::Connection::KeepAlive);
        assert_eq!(cycle.discard_remaining, 0);
        let prefix = match cycle.state {
            Ok(pump::Egress::Write {
                prefix,
                body: pump::Body::None,
            }) => Some(prefix),
            _ => None,
        }
        .expect("responses");
        assert_eq!(
            write[..prefix]
                .windows(SUCCESS.len())
                .filter(|part| *part == SUCCESS)
                .count(),
            2
        );
        assert!(write[..prefix].ends_with(b"\r\n\r\nok"));
        assert!(matches!(
            pump::Drain::complete(&mut conn),
            pump::Completion::None
        ));
    }
}

#[test]
fn rejects_before_alloc() {
    use sark_h1::route::Driver;
    use sark_http::h1::codec::canned;

    use crate::common::discard::Router;

    let router = Router::new(&());
    let driver = Driver::try_new(&router, *DATE).expect("driver");
    let limit = 4 * 1024 * 1024;
    let input = vec![0; limit + 1];
    let mut conn = Conn::default();
    let (cycle, allocs) =
        Tracker::count(|| Tracker::fail_after(0, || driver.retain(&mut conn, &input)));
    assert_eq!(allocs, 0);
    assert!(matches!(
        cycle.state,
        Err(pump::Reject::Capacity { operation: pump::Operation::Append, cause })
            if cause.into_parts().0.get() == limit + 1 && cause.into_parts().1 == limit
    ));

    conn.defer(canned::BAD_REQUEST);
    let (cycle, allocs) =
        Tracker::count(|| Tracker::fail_after(0, || driver.retain(&mut conn, &input)));
    assert_eq!(allocs, 0);
    assert!(matches!(cycle.state, Ok(pump::Egress::None)));
    assert!(
        matches!(pump::Drain::progress(&mut conn), pump::Next::Close(body)
            if body.as_slice() == canned::BAD_REQUEST.as_slice()
        )
    );
}
