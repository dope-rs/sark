use dope_test::alloc::Tracker;
use o3::alloc;
use sark_http::uri::target::{Origin, Target};
use sark_ws::client::{Config, Error, channel, headers};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn factory_uses_no_heap() {
    let target = Target::new(
        "[2001:db8::1]:8443",
        Origin::parse("/socket/%2F?token=%0D%0A").expect("valid request target"),
    )
    .expect("valid endpoint");
    let (config, allocs) = Tracker::usage(|| Config::from_target(target));
    assert_eq!(allocs, (0, 0));
    let agent = String::from("sark-test");
    let (config, allocs) = Tracker::usage(|| config.user_agent(agent));
    assert_eq!(allocs, (0, 0));
    let config = config.expect("valid user agent");

    let allocs = Tracker::usage(|| {
        channel::Port::factory(config, 1, 1).expect("valid client factory");
    })
    .1;

    assert_eq!(allocs, (0, 0));
}

#[test]
fn admit_before_allocating() {
    for name in ["X-Extra", "Host"] {
        let config = Config::new("localhost", "/").expect("valid endpoint");
        let owned = String::from(name);
        let value = String::from("value");
        let result = Tracker::fail_after(0, || config.with_header(owned, value));
        assert!(matches!(
            (name, result),
            (
                "Host",
                Err(Error::Header(headers::Error::Reserved { index: 0 }))
            ) | ("X-Extra", Err(Error::Allocation(alloc::Error::Exhausted)))
        ));
    }
}
