pub mod common;

use std::{cell::Cell, convert::Infallible, pin::Pin};

use common::session::{Frame, Reject};
use dope_test::alloc::Tracker;
use o3::buffer::storage::range;
use sark_h1::{
    pump,
    session::{self, Conn},
    switch,
    wire::{Failure, InputError, head, output},
};
use sark_http::h1::codec::canned;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn http_stays_closed() {
    let mut conn: session::Conn<'_, (), Infallible, switch::Disabled> = session::Conn::default();
    let calls = Cell::new(0);
    let mut handler = Reject::new(&calls, None);
    let (cycle, allocs) = Tracker::count(|| handler.close(&mut conn));
    assert_eq!(allocs, 0);
    assert!(matches!(cycle.connection, head::Connection::Close));
    assert!(matches!(
        cycle.state,
        Ok(pump::Egress::Write { prefix: 0, body: pump::Body::Static(reason) })
            if reason.as_slice() == canned::BAD_REQUEST.as_slice()
    ));
    assert_eq!(calls.get(), 1);
    assert!(handler.reported.is_empty());
    assert!(matches!(pump::Drain::progress(&mut conn), pump::Next::None));
    assert!(matches!(pump::Drain::progress(&mut conn), pump::Next::None));
}

#[test]
fn switching_stays_closed() {
    let mut conn: session::Conn<'_, (), Infallible, switch::Enabled<Infallible>> =
        session::Conn::default();
    let calls = Cell::new(0);
    let mut handler = Reject::new(&calls, None);
    let cycle = handler.close(&mut conn);
    assert!(matches!(cycle.connection, head::Connection::Close));
    assert!(matches!(
        cycle.state,
        Ok(pump::Egress::Write { prefix: 0, body: pump::Body::Static(reason) })
            if reason.as_slice() == canned::BAD_REQUEST.as_slice()
    ));
    assert_eq!(calls.get(), 1);
    assert!(handler.reported.is_empty());
    assert!(matches!(pump::Drain::progress(&mut conn), pump::Next::None));
    assert!(matches!(
        pump::Drain::complete(&mut conn),
        pump::Completion::None
    ));
    assert!(matches!(conn.mode(), switch::Mode::Http));
}

#[test]
fn preserves_cause_once() {
    let calls = Cell::new(0);
    let error = range::Error::new(3..7, 4);
    let mut handler = Reject::new(&calls, Some(Failure::Input(InputError::Range(error))));
    let mut conn: Conn<'_, (), Infallible> = Conn::default();
    let cycle = pump::Ingress::drive(
        &mut conn,
        Pin::new(&mut handler),
        b"request",
        &mut [0; 2048],
        head::Connection::KeepAlive,
    );
    assert_eq!(handler.reported, [Some(error)]);
    assert!(matches!(cycle.state,
        Ok(pump::Egress::Write { prefix: 0, body: pump::Body::Static(body) })
            if body.as_slice() == canned::BAD_REQUEST.as_slice()
    ));
}

#[test]
fn pump_ingress_allocations() {
    for buffered in [false, true] {
        let mut conn = Conn::default();
        let mut disp = Frame::default();
        let mut write = [0u8; 2048];
        let input = b"xy";
        let input = if buffered {
            let (_, usage) = Tracker::activity(|| pump::Ingress::retain(&mut conn, input));
            assert!(usage.allocated_bytes <= input.len() + 64);
            &[][..]
        } else {
            input.as_slice()
        };
        let (cycle, alloc) = Tracker::count(|| {
            pump::Ingress::drive(
                &mut conn,
                Pin::new(&mut disp),
                input,
                &mut write,
                head::Connection::KeepAlive,
            )
        });

        assert_eq!(alloc, 0);
        assert!(matches!(
            cycle.state,
            Ok(pump::Egress::Write {
                prefix: 2,
                body: pump::Body::None
            })
        ));
        assert!(matches!(cycle.connection, head::Connection::KeepAlive));
        assert!(matches!(cycle.tail, pump::Tail::Empty));
        assert_eq!(&write[..2], b"xy");
    }
}

#[test]
fn rejects_terminal_cursor() {
    for next in [
        session::Outcome::Complete {
            permit: session::Permit::new(),
            consumption: session::Consumption::Buffered(session::Buffered::new(usize::MAX)),
            response: output::Complete::Inline { written: 0 },
            connection: head::Connection::KeepAlive,
        },
        session::Outcome::Park {
            consumed: usize::MAX,
            close: head::Connection::KeepAlive,
        },
        session::Outcome::Streamed {
            consumed: usize::MAX,
            written: 0,
            close: head::Connection::KeepAlive,
        },
    ] {
        let mut conn: Conn<'_, (), Infallible> = Conn::default();
        let mut disp = Frame {
            next: Some(next),
            reported: Vec::with_capacity(1),
            ..Frame::default()
        };
        let (cycle, allocs) = Tracker::count(|| {
            pump::Ingress::drive(
                &mut conn,
                Pin::new(&mut disp),
                b"xy",
                &mut [0; 2048],
                head::Connection::KeepAlive,
            )
        });
        assert_eq!(allocs, 0);
        assert_eq!(disp.calls, 2);
        assert_eq!(disp.reported, [Some(range::Error::new(0..usize::MAX, 1))]);
        assert!(matches!(cycle.connection, head::Connection::KeepAlive));
        assert!(matches!(
            cycle.state,
            Ok(pump::Egress::Write {
                prefix: 1,
                body: pump::Body::None
            })
        ));
        assert!(
            matches!(pump::Drain::progress(&mut conn), pump::Next::Close(body)
                if body.as_slice() == canned::BAD_REQUEST.as_slice()
            )
        );
        assert!(matches!(pump::Drain::progress(&mut conn), pump::Next::None));
    }
}

#[test]
fn rejects_write_overrun() {
    let mut conn: Conn<'_, (), Infallible> = Conn::default();
    let mut disp = Frame {
        next: Some(session::Outcome::Complete {
            permit: session::Permit::new(),
            consumption: session::Consumption::Buffered(session::Buffered::new(1)),
            response: output::Complete::Inline { written: 2048 },
            connection: head::Connection::KeepAlive,
        }),
        ..Frame::default()
    };
    let (cycle, allocs) = Tracker::count(|| {
        pump::Ingress::drive(
            &mut conn,
            Pin::new(&mut disp),
            b"xy",
            &mut [0; 2048],
            head::Connection::KeepAlive,
        )
    });
    assert_eq!(allocs, 0);
    assert!(matches!(cycle.state, Err(pump::Reject::Capacity {
        operation: pump::Operation::Write,
        cause,
    }) if cause.into_parts() == (std::num::NonZero::new(2048).expect("length"), 2047)));
}
