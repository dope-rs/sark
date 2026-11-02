pub mod common;

use std::{num, str};

use common::uri::{
    AUTHORITY_ONLY, BASE, IPV6, NAMES, PORTS, REJECTED_HOSTS, REJECTED_IPV6, RELATIVE, RESOLVED,
};
use dope_test::alloc::Tracker;
use sark_http::{
    field::metadata::map::record::Span,
    uri::{
        self, Authority, Scheme, path,
        target::{Absolute, Error, Origin, Plan, Relative, Target},
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn borrows_absolute_parts() {
    let parsed = (Absolute::parse("wss://[2001:db8::1]:9443/tickets?node=1"))
        .expect("test operation must succeed");
    assert_eq!(parsed.scheme(), "wss");
    assert_eq!(parsed.authority().hostname(), b"2001:db8::1");
    assert_eq!(parsed.authority().port(), Some(b"9443".as_slice()));
    let host = str::from_utf8(parsed.authority().hostname()).expect("source is UTF-8");
    let port = num::NonZero::<u16>::new(9443).expect("nonzero fixture port");
    let target = (Target::from_host(host, Some(port), parsed.origin()))
        .expect("test operation must succeed");
    assert_eq!(target.authority(), "[2001:db8::1]:9443");
    assert_eq!(target.host(), "2001:db8::1");
    assert_eq!(target.path_query(), "/tickets?node=1");
}

#[test]
fn canonicalizes_paths() {
    let root = (Absolute::parse("https://example.test")).expect("test operation must succeed");
    let target = (Target::from_host("example.test", None, root.origin()))
        .expect("test operation must succeed");
    assert_eq!(target.path(), "/");

    let query =
        (Absolute::parse("https://example.test?node=1")).expect("test operation must succeed");
    let target = (Target::from_host("example.test", None, query.origin()))
        .expect("test operation must succeed");
    assert_eq!(target.path_query(), "/?node=1");
}

#[test]
fn rejects_invalid_uris() {
    assert!(matches!(
        Absolute::parse("wss://example.test/tickets#fragment"),
        Err(Error::Origin(path::Error::Byte { at: 8, byte: b'#' }))
    ));
    assert!(matches!(
        Absolute::parse("wss://example.test#fragment"),
        Err(Error::Start { byte: Some(b'#') })
    ));
    assert!(matches!(
        Absolute::parse("wss://user@example.test/tickets"),
        Err(Error::UserInfo)
    ));
    assert_eq!(
        "/tickets".parse::<Target>(),
        Err(Error::Authority(uri::Error::InvalidHost))
    );
    assert_eq!(
        "https:///tickets".parse::<Target>(),
        Err(Error::Authority(uri::Error::Empty))
    );
}

#[test]
fn mutates_target_safely() {
    let target = Target::parse("https://example.test/one").expect("valid absolute target");
    let target = (target.retarget("EXAMPLE.test:443")).expect("test operation must succeed");
    let target = (target
        .with_origin((Origin::parse("/two?q=1")).expect("test operation must succeed")))
    .expect("test operation must succeed");
    let other = uri::Authority::parse(b"example.test").expect("valid comparison authority");
    assert!(target.parts().is_same(other, uri::Scheme::Https));
    assert_eq!(target.scheme_str(), Some("https"));
    assert_eq!(target.path(), "/two");
    assert_eq!(target.query(), Some("q=1"));
    assert_eq!(target.path_query(), "/two?q=1");
}

#[test]
fn measures_uri_allocations() {
    let (absolute, alloc) = Tracker::usage(|| {
        (Absolute::parse("wss://example.test/tickets?node=1")).expect("test operation must succeed")
    });
    assert_eq!(alloc, (0, 0));

    let expected = "example.test/tickets?node=1".len();
    let (target, alloc) = Tracker::usage(|| {
        (Target::from_host("example.test", None, absolute.origin()))
            .expect("test operation must succeed")
    });
    assert_eq!(alloc, (1, expected));

    let (_, alloc) = Tracker::usage(|| {
        assert_eq!(target.authority(), "example.test");
        assert_eq!(target.path_query(), "/tickets?node=1");
        let other = uri::Authority::parse(b"EXAMPLE.test:443").expect("valid comparison authority");
        assert!(target.parts().is_same(other, uri::Scheme::Https));
    });
    assert_eq!(alloc, (0, 0));
}

#[test]
fn owns_absolute_target() {
    let value = "HTTPS://[2001:db8::1]:0443/a%2Fb?node=1";
    let (target, alloc) = Tracker::usage(|| value.parse::<Target>().expect("absolute target"));
    assert_eq!(alloc, (1, value.len()));
    let (_, alloc) = Tracker::usage(|| {
        assert_eq!(target.as_ref(), value);
        assert_eq!(target.scheme_str(), Some("HTTPS"));
        assert_eq!(target.host_raw(), "[2001:db8::1]");
        assert_eq!(target.host(), "2001:db8::1");
        assert_eq!(target.parts().try_port_u16(), Ok(Some(443)));
        assert_eq!(target.path(), "/a%2Fb");
        assert_eq!(target.query(), Some("node=1"));
    });
    assert_eq!(alloc, (0, 0));
    assert_eq!(
        target,
        "https://[2001:DB8::1]:0443/a%2Fb?node=1"
            .parse::<Target>()
            .expect("equivalent target")
    );
    let relative = target
        .with_origin(Origin::parse("/next?").expect("origin form"))
        .expect("retained authority");
    assert_eq!(relative.as_ref(), "HTTPS://[2001:db8::1]:0443/next?");
    assert_eq!(relative.query(), Some(""));
}

#[test]
fn retains_optional_scheme() {
    let target = "example.test:080"
        .parse::<Target>()
        .expect("authority target");
    assert_eq!(target.scheme_str(), None);
    assert_eq!(target.authority(), "example.test:080");
    assert_eq!(target.parts().try_port_u16(), Ok(Some(80)));
    assert_eq!(target.path_query(), "/");
    let target = "https://example.test?node=1"
        .parse::<Target>()
        .expect("query target");
    assert_eq!(target.path(), "/");
    assert_eq!(target.path_query(), "/?node=1");
}

#[test]
fn consumes_owned_plan() {
    for (source, expected, absolute) in [
        ("[2001:db8::1]:8080", "http://[2001:db8::1]:8080/", false),
        (
            "https://Example.test?node=1",
            "https://Example.test/?node=1",
            true,
        ),
        (
            "wss://example.test/a%2Fb?node=1",
            "wss://example.test/a%2Fb?node=1",
            true,
        ),
    ] {
        for capacity in [source.len(), expected.len(), expected.len() + 16] {
            let mut owned = String::with_capacity(capacity);
            owned.push_str(source);
            let pointer = owned.as_ptr();
            let (plan, admit) = Tracker::usage(|| {
                if absolute {
                    Plan::parse(owned)
                } else {
                    Plan::http(owned)
                }
            });
            let plan = plan.expect("owned target admission");
            assert_eq!(admit, (0, 0));
            let (target, built) = Tracker::usage(|| plan.try_build());
            let target = target.expect("target materialization");
            let allocs = capacity != expected.len();
            assert_eq!(
                built,
                (usize::from(allocs), if allocs { expected.len() } else { 0 })
            );
            if !allocs {
                assert_eq!(target.as_ref().as_ptr(), pointer);
            }
            assert_eq!(target.as_ref(), expected);
            assert_eq!(
                target,
                expected.parse::<Target>().expect("canonical target")
            );
        }
    }
}

#[test]
fn normalizes_authorities() {
    let same = |left, right, scheme| {
        (Authority::parse(left))
            .expect("test operation must succeed")
            .is_same(
                (Authority::parse(right)).expect("test operation must succeed"),
                scheme,
            )
    };
    assert!(same(b"EXAMPLE.com:080", b"example.com", Scheme::Http));
    assert!(same(b"%65xample.com", b"example.com", Scheme::Https,));
    assert!(same(
        b"[2001:0db8::1]:0443",
        b"[2001:db8:0:0::1]",
        Scheme::Https,
    ));
    assert!(!same(b"example.com:81", b"example.com", Scheme::Http,));
    for &(left, right, equal) in NAMES {
        for (left, right) in [(left, right), (right, left)] {
            assert_eq!(same(left, right, Scheme::Https), equal);
            let parsed = Authority::parse(left).expect("name authority");
            for split in 0..=right.len() {
                let (first, second) = right.split_at(split);
                assert_eq!(
                    parsed.compare_span(Span::new(first, second), Scheme::Https),
                    Ok(equal)
                );
            }
        }
    }
}

#[test]
fn parses_ip_literals() {
    use core::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use sark_http::uri::Host;

    assert!((Authority::parse_ip(b"127.0.0.1")) == (Ok(IpAddr::V4(Ipv4Addr::LOCALHOST))));
    for &(source, words) in IPV6 {
        assert_eq!(
            Authority::parse_ip(source),
            Ok(IpAddr::V6(Ipv6Addr::from(words)))
        );
        let parsed = Authority::parse(source).expect("valid IPv6 authority");
        for split in 0..=source.len() {
            let (first, second) = source.split_at(split);
            assert_eq!(
                parsed.compare_span(Span::new(first, second), Scheme::Https),
                Ok(true)
            );
        }
    }
    let parsed = Authority::parse(b"[::]").expect("valid IPv6 authority");
    for &source in REJECTED_IPV6 {
        assert_eq!(Authority::parse_ip(source), Err(Host::Invalid));
        for split in 0..=source.len() {
            let (first, second) = source.split_at(split);
            assert_eq!(
                parsed.compare_span(Span::new(first, second), Scheme::Https),
                Err(uri::Error::InvalidHost)
            );
        }
    }
    assert!((Authority::parse_ip(b"example.com")) == (Err(Host::Name)));
    assert_eq!(Authority::parse_ip(b"v1.fe80"), Err(Host::Name));
    assert!((Authority::parse_ip(b"[v1.fe80]")) == (Err(Host::Future)));
    assert!((Authority::parse_ip(b"[::1")) == (Err(Host::Invalid)));
}

#[test]
fn port_range() {
    for (source, expected) in PORTS {
        let parsed = Authority::parse(source).expect("authority syntax");
        assert_eq!(parsed.try_port_u16(), Ok(expected));
    }
    let parsed = Authority::parse(common::uri::PORT_OVERFLOW).expect("authority syntax");
    assert_eq!(parsed.try_port_u16(), Err(uri::PortError));
}

#[test]
fn host_admission() {
    for &(host, port, expected) in REJECTED_HOSTS {
        let (result, usage) = Tracker::usage(|| Target::from_host(host, port, Origin::ROOT));
        assert_eq!(result.err(), Some(Error::Authority(expected)));
        assert_eq!(usage, (0, 0));
    }
}

#[test]
fn absolute_separator() {
    assert!(Target::parse(AUTHORITY_ONLY).is_err());
    assert!(AUTHORITY_ONLY.parse::<Target>().is_ok());
}

#[test]
fn resolves_once() {
    for &(base, relative, expected) in RESOLVED {
        let base = Target::parse(base).expect("base target");
        let relative = Relative::parse(relative).expect("relative target");
        let (resolved, usage) = Tracker::usage(|| relative.resolve(&base));
        let resolved = resolved.expect("resolved target");
        assert_eq!(usage, (1, expected.len()));
        assert_eq!(resolved.as_ref(), expected);
        let expected = Target::parse(expected).expect("expected target");
        assert_eq!(resolved.path(), expected.path());
        assert_eq!(resolved.query(), expected.query());
        assert_eq!(resolved.host_raw(), expected.host_raw());
        assert_eq!(resolved.scheme_str(), expected.scheme_str());
    }
}

#[test]
fn retains_alloc_failure() {
    let mut owned = String::with_capacity(32);
    owned.push_str("example.test");
    let capacity = owned.capacity();
    let plan = Plan::http(owned).expect("owned authority");
    let (rejected, activity) = Tracker::activity(|| Tracker::fail_after(0, || plan.try_build()));
    assert!(matches!(rejected, Err(o3::alloc::Exhausted)));
    assert_eq!(
        (
            activity.allocations,
            activity.deallocations,
            activity.deallocated_bytes
        ),
        (0, 1, capacity)
    );
    assert_eq!(
        Tracker::fail_after(0, || Target::parse(BASE)),
        Err(Error::Allocation(o3::alloc::Exhausted))
    );
    let base = Target::parse(BASE).expect("base target");
    assert_eq!(
        Tracker::fail_after(0, || base.try_clone()),
        Err(o3::alloc::Exhausted)
    );
    let relative = Relative::parse(RELATIVE).expect("relative target");
    assert_eq!(
        Tracker::fail_after(0, || relative.resolve(&base)),
        Err(Error::Allocation(o3::alloc::Exhausted))
    );
    let resolved = relative.resolve(&base).expect("normalized target");
    assert_eq!(resolved.path(), "/c");
    assert_eq!(resolved.query(), Some("new"));
}
