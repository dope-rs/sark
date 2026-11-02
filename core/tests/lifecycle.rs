pub mod common;
use core::{fmt, mem};

use common::life::{Events, NativeStream};
use o3::buffer::hold;
use sark_core::{
    body::credit::Evidence as _,
    life::{
        self, audit, exchange,
        mode::{Disabled, Observed},
        sequence,
        sequence::observed::Session,
    },
};

#[test]
fn keeps_exchange_tag() {
    let mut mode = Observed::new(Events {
        events: 0,
        audits: 0,
        diagnostic: |cause: &dyn fmt::Debug| eprintln!("lifecycle diagnostic: {cause:?}"),
    });
    let mut session = Session::default();
    let first = sequence::Mode::open(&mut mode, &mut session, 0_u8);
    life::Mode::body(&mut mode, first).head();
    let charge = hold::Charge::from_owner(41, 64);
    let tag = sequence::Mode::tag(&mode, first, charge);
    assert_eq!(
        mem::size_of_val(&tag),
        mem::size_of::<u64>() + mem::size_of::<hold::Charge>(),
    );

    let second = sequence::Mode::open(&mut mode, &mut session, 0_u8);
    life::Mode::body(&mut mode, second).head();
    life::Mode::<u8, u64>::audit(&mut mode, 0, audit::Snapshot::default);

    assert!(sequence::Mode::transport(&mode, 0, tag) == Some((first, charge)));
    assert!((sequence::Mode::current(&mode, &session, 0)) == (Some(second)));
    assert_eq!(mode.observer().events, 4);
    assert_eq!(mode.observer().audits, 1);
}

#[test]
fn erases_disabled_tag() {
    type Tag = <Disabled as sequence::Mode<u8>>::TransportTag;
    type Session = <Disabled as sequence::Mode<u8>>::Session;

    assert_eq!(mem::size_of::<Disabled>(), mem::size_of::<Observed<()>>());
    assert_eq!(mem::align_of::<Disabled>(), mem::align_of::<Observed<()>>());

    let mode = Disabled;
    let exchange = exchange::Id::new(7, 11);
    sequence::Mode::tag(&mode, exchange, hold::Charge::from_owner(41, 64));

    assert_eq!(mem::size_of::<Tag>(), 0);
    assert_eq!(mem::size_of::<Session>(), 0);
    assert!(sequence::Mode::transport(&mode, exchange.connection(), ()).is_none());
}

#[test]
fn keeps_stream_type() {
    let exchange = exchange::Id::new(7_u16, NativeStream(13));

    assert_eq!(exchange.connection(), 7);
    assert!((exchange.stream()) == (NativeStream(13)));
    assert_eq!(
        mem::size_of_val(&exchange),
        mem::size_of::<(u16, NativeStream)>(),
    );
}

#[test]
fn audit_fails_closed() {
    let required = audit::Capability::REQUEST_BOOK_ENTRIES
        .union(audit::Capability::PRE_ADMISSION_BYTES)
        .union(audit::Capability::LIVE_TASKS)
        .union(audit::Capability::H3_RESPONSE_OWNER_BYTES);

    assert!(!audit::Snapshot::default().is_zero(required));
    assert!(
        audit::Snapshot::default()
            .request(0, 0, 0, 0)
            .live_tasks(0)
            .h3_bytes(0)
            .is_zero(required),
    );
    assert!(
        !audit::Snapshot::default()
            .request(0, 1, 0, 0)
            .live_tasks(0)
            .h3_bytes(0)
            .is_zero(required),
    );
}

#[test]
fn round_trips_audit() {
    let snapshot = audit::Snapshot::default()
        .request(1, 2, 3, 4)
        .owner(5, 6, 7)
        .task(8, 9, 10)
        .streams(11, 12, 13)
        .response(14, 15, 16);
    let view = snapshot.projection();
    let (caps, values) = view.into_parts();
    let restored = audit::Projection::from_parts(caps, values).restore();

    assert!((restored) == (snapshot));
}

#[test]
fn audit_outside_event() {
    assert!(mem::size_of::<life::Event<u64, u64>>() < mem::size_of::<audit::Snapshot>(),);
}
