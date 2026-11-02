use std::{alloc, time::Duration};

use dope::{
    core::driver::{
        route::{KeyTag, table::conn},
        settings,
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
    let result = "127.0.0.1:0".parse();
    let bind = result.expect("loopback bind address");
    let server = listener::Config::<KeyTag<0>, Throughput>::tcp(
        bind,
        conn::Capacity::fixed::<1024>(),
        Duration::from_secs(10),
    );
    let result = server.clone().plan::<KeyTag<1>>().prepare(());
    let prepared = result.expect("exact H1 receive plan");
    let layout = prepared.layout();

    assert!((layout.receive()) == (settings::Config::for_profile::<Throughput>().receive()));
    assert!((layout.domains()) == (settings::Domain::ZERO));
    assert_eq!(layout.file_slots().accept().count().raw(), 1024);
    assert!(
        (layout.file_slots().outbound())
            == (settings::Config::for_profile::<Throughput>()
                .file_slots()
                .outbound())
    );
    assert_eq!(layout.schedule().ready().count().raw(), 1025);
    assert_eq!(layout.schedule().timers().count().raw(), 1025);
}
