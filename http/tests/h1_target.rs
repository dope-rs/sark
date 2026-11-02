use sark_http::{
    h1::{field::expect, framing, target},
    request,
    request::span,
    uri,
    uri::path,
};

#[test]
fn host_validation() {
    for value in [
        b"example.com".as_slice(),
        b"example.com:443",
        b"[2001:db8::1]",
        b"[2001:db8::1]:8443",
        b"[v1.fe80::a]",
        b"xn--bcher-kva.example",
        b"name%2Eexample",
    ] {
        assert!(
            target::Host::parse(value) == Ok(target::Host::Authority),
            "{value:?}"
        );
    }
    assert!(target::Host::parse(b"\t ") == Ok(target::Host::Empty));
    for (value, cause) in [
        (b"user@example.com".as_slice(), uri::Error::InvalidHost),
        (b"example.com/path", uri::Error::InvalidHost),
        (b"example.com:bad", uri::Error::InvalidPort),
        (b"2001:db8::1", uri::Error::InvalidHost),
        (b"[not-ipv6]", uri::Error::InvalidHost),
        (b"bad%2", uri::Error::InvalidHost),
        (b"bad host", uri::Error::InvalidHost),
    ] {
        assert!(target::Host::parse(value) == Err(cause), "{value:?}");
    }
}

#[test]
fn request_plan() {
    let auth =
        (target::Request::default().try_host(b"example.com")).expect("test operation must succeed");
    assert!(auth.try_host(b"other.example") == Err(target::HostError::Duplicate));
    let plan = (auth.finish(b"GET", b"/items", b"HTTP/1.1")).expect("test operation must succeed");
    assert!((plan.target_form()) == (target::Form::Origin));
    assert!((plan.source()) == (target::Source::Host));

    assert!(
        target::Request::default().finish(b"GET", b"/", b"HTTP/1.1")
            == Err(target::Error::MissingHost)
    );
    let empty = (target::Request::default().try_host(b"")).expect("test operation must succeed");
    let plan = (empty.finish(b"GET", b"/", b"HTTP/1.1")).expect("test operation must succeed");
    assert!((plan.source()) == (target::Source::Empty));
    assert!(plan.require() == Err(target::Error::EmptyAuthority));

    let absolute = (target::Request::default().try_host(b"")).expect("test operation must succeed");
    let plan = (absolute.finish(b"GET", b"https://origin.example/items?q=1", b"HTTP/1.1"))
        .expect("test operation must succeed");
    assert!((plan.target_form()) == (target::Form::Absolute));
    assert!((plan.source()) == (target::Source::Target));

    let connect = (target::Request::default().try_host(b"proxy.example"))
        .expect("test operation must succeed");
    let plan = (connect.finish(b"CONNECT", b"origin.example:443", b"HTTP/1.1"))
        .expect("test operation must succeed");
    assert!((plan.target_form()) == (target::Form::Authority));
    assert!((plan.source()) == (target::Source::Target));
    assert!(
        connect.finish(b"CONNECT", b"origin.example", b"HTTP/1.1")
            == Err(target::Error::InvalidTarget)
    );
    assert!(
        connect.finish(b"CONNECT", b"origin.example:", b"HTTP/1.1")
            == Err(target::Error::InvalidTarget)
    );

    let asterisk =
        (target::Request::default().try_host(b"example.com")).expect("test operation must succeed");
    assert!(
        ((asterisk.finish(b"OPTIONS", b"*", b"HTTP/1.1"))
            .expect("test operation must succeed")
            .target_form())
            == (target::Form::Asterisk)
    );
    assert!(asterisk.finish(b"GET", b"*", b"HTTP/1.1") == Err(target::Error::InvalidTarget));

    assert!(
        (auth.finish(b"GET", b"/%zz", b"HTTP/1.1"))
            == (Err(target::Error::Path(path::Error::Escape {
                at: 1,
                first: Some(b'z'),
                second: Some(b'z'),
            })))
    );
    let plan = (auth.finish(b"GET", b"/ok?nested=/x?y", b"HTTP/1.1"))
        .expect("test operation must succeed");
    assert!((plan.target_form()) == (target::Form::Origin));
    assert!((plan.source()) == (target::Source::Host));
}

#[test]
fn target_projection() {
    let origin = b"/items/42?q=rust";
    let plan = (target::Target::parse(b"GET", origin)).expect("test operation must succeed");
    assert!((plan.form()) == (target::Form::Origin));
    assert_eq!(plan.route_path(origin), Some(b"/items/42".as_slice()));
    assert_eq!(
        plan.query_span().and_then(|span| span.get(origin)),
        Some(b"q=rust".as_slice())
    );
    assert!(plan.authority().is_none());

    let absolute = b"https://origin.example/items/42?q=rust";
    let plan = (target::Target::parse(b"GET", absolute)).expect("test operation must succeed");
    assert!((plan.form()) == (target::Form::Absolute));
    assert_eq!(plan.route_path(absolute), Some(b"/items/42".as_slice()));
    assert_eq!(
        plan.authority().and_then(|span| span.get(absolute)),
        Some(b"origin.example".as_slice())
    );
    assert_eq!(
        plan.query_span().and_then(|span| span.get(absolute)),
        Some(b"q=rust".as_slice())
    );

    let root = b"http://origin.example?ready=yes";
    let plan = (target::Target::parse(b"GET", root)).expect("test operation must succeed");
    assert_eq!(plan.route_path(root), Some(b"/".as_slice()));
    assert!(plan.path_span().is_empty());
    assert!((plan.form()) == (target::Form::Absolute));

    let shifted = (plan.shifted(17)).expect("test operation must succeed");
    assert!(
        (shifted.authority().expect("shifted authority span"))
            == ((span::Span::checked(24, 14)).expect("test operation must succeed"))
    );

    let connect = b"origin.example:443";
    let plan = (target::Target::parse(b"CONNECT", connect)).expect("test operation must succeed");
    assert!((plan.form()) == (target::Form::Authority));
    assert_eq!(plan.route_path(connect), Some(b"*".as_slice()));
    assert_eq!(
        plan.authority().and_then(|span| span.get(connect)),
        Some(connect.as_slice())
    );
}

#[test]
fn target_failures() {
    for method in [b"".as_slice(), b"BAD METHOD", b"G\xffT"] {
        assert_eq!(
            target::Target::parse(method, b"/%zz").err(),
            Some(target::Error::InvalidTarget),
        );
    }
    assert!(
        target::Target::parse(b"GET", b"https://bad host/items")
            == Err(target::Error::Authority(uri::Error::InvalidHost))
    );
    assert!(
        target::Target::parse(b"GET", b"https://example.com:bad/items")
            == Err(target::Error::Authority(uri::Error::InvalidPort))
    );

    let target = (target::Target::parse(b"GET", b"/x?q")).expect("test operation must succeed");
    let base = u32::MAX as usize;
    assert!(
        target.shifted(base)
            == Err(target::Error::Span(
                target::Operation::PathShift,
                span::Error::Shift {
                    start: 0,
                    len: 2,
                    base,
                },
            ))
    );
    let base = u32::MAX as usize - 2;
    assert!(
        target.shifted(base)
            == Err(target::Error::Span(
                target::Operation::QueryShift,
                span::Error::Shift {
                    start: 3,
                    len: 1,
                    base,
                },
            ))
    );

    let target = (target::Target::parse(b"CONNECT", b"origin.example:443"))
        .expect("test operation must succeed");
    let base = u32::MAX as usize - 1;
    assert!(
        target.shifted(base)
            == Err(target::Error::Span(
                target::Operation::AuthorityShift,
                span::Error::Shift {
                    start: 0,
                    len: 18,
                    base,
                },
            ))
    );
}

#[test]
fn request_admission() {
    let target = (target::Target::parse(b"POST", b"/items")).expect("test operation must succeed");
    let auth = (target::Request::default().try_host(b"origin.example"))
        .expect("test operation must succeed");
    let fields =
        (framing::Fields::default().try_length(b"7")).expect("test operation must succeed");
    let expect = expect::State::default().push(b"100-continue");

    let admit = (framing::Admission::admit(
        fields,
        auth,
        expect,
        target,
        b"HTTP/1.1",
        target::Policy::Require,
    ))
    .expect("test operation must succeed");
    assert!((admit.target()) == (target));
    assert!((admit.authority().source()) == (target::Source::Host));
    assert!((admit.body()) == (framing::RequestBody::Fixed(7)));
    assert!((admit.expectation()) == (expect::Decision::Continue));

    let start = 1usize << (u32::BITS - 3);
    let range = (span::Span::checked(start, 0)).expect("test operation must succeed");
    let host = (span::Span::checked(0, 14)).expect("test operation must succeed");
    assert!(
        admit.layout(range, Some(host)) == Err(target::Error::Layout(request::Error::OutOfBounds))
    );

    assert!(
        framing::Admission::admit(
            framing::Fields::default(),
            target::Request::default(),
            expect::State::default(),
            target,
            b"HTTP/1.1",
            target::Policy::Require,
        ) == Err(framing::RequestError::Authority(target::Error::MissingHost))
    );
}

#[test]
fn layout_projection() {
    let owner = b"/items/42?q=rustorigin.example";
    let len = b"/items/42?q=rust".len();
    let target =
        (target::Target::parse(b"GET", &owner[..len])).expect("test operation must succeed");
    let auth = (target::Request::default().try_host(b"origin.example"))
        .expect("test operation must succeed");
    let admit = (framing::Admission::admit(
        framing::Fields::default(),
        auth,
        expect::State::default(),
        target,
        b"HTTP/1.1",
        target::Policy::Require,
    ))
    .expect("test operation must succeed");
    let layout = (admit.layout(
        (span::Span::checked(0, len)).expect("test operation must succeed"),
        Some((span::Span::checked(len, 14)).expect("test operation must succeed")),
    ))
    .expect("test operation must succeed");
    let route = layout.route().expect("origin route span");
    assert_eq!(route.get(owner), Some(&b"/items/42"[..]));
    assert_eq!(
        owner.get(route.end() + 1..layout.target().end()),
        Some(&b"q=rust"[..])
    );
    assert_eq!(
        layout.authority().and_then(|span| span.get(owner)),
        Some(&b"origin.example"[..])
    );

    let root = b"http://origin.example?ready=yes";
    let target = (target::Target::parse(b"GET", root)).expect("test operation must succeed");
    let auth = (target::Request::default().try_host(b"")).expect("test operation must succeed");
    let layout = (framing::Admission::admit(
        framing::Fields::default(),
        auth,
        expect::State::default(),
        target,
        b"HTTP/1.1",
        target::Policy::Require,
    ))
    .expect("test operation must succeed")
    .layout(
        (span::Span::checked(0, root.len())).expect("test operation must succeed"),
        None,
    )
    .expect("test operation must succeed");
    assert_eq!(layout.target().get(root), Some(root.as_slice()));
    assert!((layout.target().form()) == (request::Form::Root));
    let route = layout.route().expect("root route span");
    assert!(route.is_empty());
    assert_eq!(
        root.get(route.start() + 1..layout.target().end()),
        Some(&b"ready=yes"[..])
    );
    assert_eq!(
        layout.authority().and_then(|span| span.get(root)),
        Some(&b"origin.example"[..])
    );

    let connect = b"origin.example:443";
    let target = (target::Target::parse(b"CONNECT", connect)).expect("test operation must succeed");
    let auth = (target::Request::default().try_host(b"")).expect("test operation must succeed");
    let layout = (framing::Admission::admit(
        framing::Fields::default(),
        auth,
        expect::State::default(),
        target,
        b"HTTP/1.1",
        target::Policy::Require,
    ))
    .expect("test operation must succeed")
    .layout(
        (span::Span::checked(0, connect.len())).expect("test operation must succeed"),
        None,
    )
    .expect("test operation must succeed");
    assert!((layout.target().form()) == (request::Form::Authority));
    assert!(layout.route().is_none());
    assert_eq!(
        layout.authority().and_then(|span| span.get(connect)),
        Some(&connect[..])
    );
}
