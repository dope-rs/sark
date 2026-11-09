use std::{alloc, time::Duration};

use dope::{
    core::driver::{
        route::{KeyTag, table::conn},
        settings::{self, claim::Needs},
    },
    manifold::timing::{Throughput, interval},
};
use sark_h1::server::{date, listener};

#[test]
fn date_keeps_two_owners() {
    assert_eq!(
        alloc::Layout::new::<date::Root<'static, 'static, KeyTag<0>>>(),
        alloc::Layout::new::<(interval::Interval<'static, KeyTag<0>>, &'static date::Clock)>(),
    );
}

#[test]
fn plan_resolves_domain() {
    let bind = "127.0.0.1:0".parse().expect("loopback bind address");
    let server = listener::Config::<KeyTag<0>, Throughput>::tcp(
        bind,
        conn::Capacity::fixed::<1024>(),
        Duration::from_secs(10),
    )
    .plan::<KeyTag<1>>();
    let baseline = server.config();
    let needs = Needs::from_tree(&server).expect("listener claims");
    let layout = server.prepare(()).expect("H1 receive plan").layout();

    assert!(layout.receive() == baseline.receive());
    assert!((layout.domains()) == (settings::Domain::ZERO));
    assert_eq!(layout.file_slots().accept().count().raw(), 1024);
    assert!(layout.file_slots().outbound() == baseline.file_slots().outbound());
    assert_eq!(layout.schedule().ready().count().raw(), 1025);
    assert_eq!(layout.schedule().timers().count().raw(), 1025);

    let receive = settings::Receive::fixed::<2, 1>();
    let layout = needs
        .apply_to(baseline.with_receive(receive))
        .expect("caller receive pool");
    assert!(layout.receive() == receive);
}
