use dope::core::driver::route::KeyTag;
use dope_test::alloc::Tracker;
use o3::depot::capacity::Count;
use sark_fiber::client::{
    config::{
        Config,
        report::{Class, Never, Policy},
    },
    dynamic,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn host_allocation_fails() {
    let rejected = Tracker::fail_after(0, || Config::try_new("127.0.0.1"));
    assert!(matches!(rejected, Err(o3::alloc::Exhausted)));
    let (config, count) = Tracker::count(|| Config::try_new("127.0.0.1"));
    config.expect("owned host");
    assert_eq!(count, 1);
}

#[test]
fn authority_moves_free() {
    let host = String::from("127.0.0.1:8080");
    let peer = String::from("[2001:db8::1]:8080");

    let (fixed, f_alloc) = Tracker::usage(|| {
        use sark_fiber::client::channel::{Plan, Topology};

        Plan::<KeyTag<0>>::new(Config::new(host).failures(usize::MAX), Topology::new(1, 1))
    });
    let (dynamic, d_alloc) = Tracker::usage(|| {
        use dynamic::channel::Plan;

        Plan::<KeyTag<0>>::new(Config::new(peer).failures(usize::MAX), 1, 1, 1)
    });

    (fixed)
        .as_ref()
        .expect("fixed client must accept its owned authority");
    (dynamic)
        .as_ref()
        .expect("dynamic client must accept its owned authority");
    assert_eq!(f_alloc, (0, 0));
    assert_eq!(d_alloc, (0, 0));
}

#[test]
fn never_allocates_reports() {
    use std::convert::Infallible;
    let capacity = <Never as Class>::capacity(usize::MAX, usize::MAX)
        .expect("disabled reporting requires no capacity");
    assert_eq!(capacity, Count::EMPTY);
    let (pool, alloc) = Tracker::usage(|| <Never as Policy<Infallible>>::try_pool(capacity));
    pool.expect("disabled report pool");
    assert_eq!(alloc, (0, 0));
}

#[test]
fn accepts_https_origin() {
    use sark_fiber::client::channel::{Plan, Topology};

    for origin in [
        "https://stgapi.betradar.com",
        "HtTpS://stgapi.betradar.com",
        "HTTP://stgapi.betradar.com",
    ] {
        Plan::<KeyTag<0>>::new(
            Config::try_new(origin).expect("HTTP host allocation"),
            Topology::new(1, 1),
        )
        .as_ref()
        .expect("HTTP scheme is case-insensitive");
    }
}

#[test]
fn origin_allocation_fails() {
    use o3::alloc;
    use sark_client::OriginError;
    use sark_fiber::client::dynamic::origin::Origin;
    use sark_http::uri::target::Target;

    let target = Target::parse("http://EXAMPLE.test:8080/").expect("origin fixture");
    let rejected = Tracker::fail_after(0, || Origin::from_uri(&target));
    assert!(matches!(
        rejected,
        Err(OriginError::Allocation(alloc::Exhausted))
    ));
    let (origin, count) = Tracker::count(|| Origin::from_uri(&target));
    let origin = origin.expect("origin allocation");
    assert_eq!(origin.authority(), "example.test:8080");
    assert_eq!(origin.host(), "example.test");
    assert_eq!(count, 1);
}

#[test]
fn origin_port_overflow() {
    use sark_client::OriginError;
    use sark_fiber::client::dynamic::origin::Origin;
    use sark_http::uri::{self, target::Target};

    let target = Target::parse("http://127.0.0.1:99999/").expect("decimal URI port");
    assert!(matches!(
        Origin::from_uri(&target),
        Err(OriginError::Port(uri::PortError))
    ));
}
