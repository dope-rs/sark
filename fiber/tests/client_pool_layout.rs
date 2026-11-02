use dope::core::driver::route::KeyTag;
use dope_fiber::reply::layout;
use sark_fiber::client::{
    channel::{Plan, Topology},
    config::{Config, Error},
};

#[test]
fn rejects_overflow() {
    let entries = Config::try_new("127.0.0.1")
        .expect("HTTP host allocation")
        .request_pool(1, 1)
        .inflight(2);
    assert!(matches!(
        Plan::<KeyTag<0>>::new(entries, Topology::new(usize::MAX, 1)),
        Err(Error::ResponseEntries)
    ));

    let items = Config::try_new("127.0.0.1")
        .expect("HTTP host allocation")
        .request_pool(1, 1)
        .inflight(1);
    assert!(matches!(
        Plan::<KeyTag<0>>::new(items, Topology::new(usize::MAX / 2 + 1, 1)),
        Err(Error::ResponseItems)
    ));

    let bytes = Config::try_new("127.0.0.1")
        .expect("HTTP host allocation")
        .request_pool(1, 1)
        .inflight(1)
        .queue_bytes(usize::MAX / 2 + 1);
    assert!(matches!(
        Plan::<KeyTag<0>>::new(bytes, Topology::new(2, 1)),
        Err(Error::ResponseBytes)
    ));
}

#[test]
fn preserves_exact_cause() {
    assert!(matches!(
        Plan::<KeyTag<0>>::new(
            Config::try_new("127.0.0.1").expect("HTTP host allocation"),
            Topology::new(0, 1)
        ),
        Err(Error::ResponseLayout(layout::Error::ZeroLanes))
    ));

    let config = Config::try_new("127.0.0.1")
        .expect("HTTP host allocation")
        .inflight(0);
    assert!(matches!(
        Plan::<KeyTag<0>>::new(config, Topology::new(1, 1)),
        Err(Error::ResponseLayout(layout::Error::ZeroEntries))
    ));
}
