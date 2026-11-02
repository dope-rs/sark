use std::ptr;

use dope_test::alloc::Tracker;
use sark_client::{
    Error, Safety,
    protocol::redirect::{self, Action, Machine, State, Target},
};
use sark_http::{method::Method, payload::response::status, uri::target};

pub mod common;
use common::redirect::{
    ALIAS, BASE, INITIAL_TARGETS, METHOD_CHAIN, PAYLOAD, PRESERVED_METHODS, REWRITE_STATUSES,
    SAME_TARGETS, START,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn initial_borrow_is_free() {
    assert_eq!(
        size_of::<Machine<'static>>(),
        size_of::<State<'static>>() + size_of::<Method>() + size_of::<&'static [u8]>(),
        "transaction commitment must reuse redirect history rather than add state",
    );
    let base = "https://example.test/root"
        .parse::<target::Target>()
        .expect("absolute target");
    let path = String::from("/items//42?view=full");

    let (state, (calls, bytes)) = Tracker::usage(|| State::new(5, &base, &path));
    let state = state.expect("valid redirect state");

    assert_eq!((calls, bytes), (0, 0));
    assert_eq!(state.origin_form(), path);
    assert_eq!(state.origin_form().as_ptr(), path.as_ptr());
    assert!(ptr::eq(state.origin(), &base));

    let body = b"borrowed";
    let (machine, cost) = Tracker::usage(|| Machine::new(5, &base, &path, Method::POST, body));
    let machine = machine.expect("valid redirect machine");
    assert_eq!(cost, (0, 0));
    assert_eq!(machine.origin_form().as_ptr(), path.as_ptr());
    assert_eq!(machine.body().as_ptr(), body.as_ptr());
    assert!(ptr::eq(machine.origin(), &base));
}

#[test]
fn fragment_is_ignored() {
    let base = "https://example.test/dir/start?old=1"
        .parse::<target::Target>()
        .expect("absolute target");

    let relative = State::new(3, &base, "next?new=2#section").expect("relative redirect");
    assert_eq!(relative.origin_form(), "/dir/next?new=2");

    let fragment = State::new(3, &base, "#section").expect("fragment-only redirect");
    assert_eq!(fragment.origin_form(), "/dir/start?old=1");
}

#[test]
fn noncanonical_resolves() {
    let base = "https://example.test/root"
        .parse::<target::Target>()
        .expect("absolute target");
    let state = State::new(3, &base, "/a//b/../c").expect("normalized target");

    assert_eq!(state.origin_form(), "/a//c");
}

#[test]
fn absolute_dot_segments() {
    let base = "https://example.test/root"
        .parse::<target::Target>()
        .expect("absolute target");
    for location in [
        "https://example.test/a//b/../c?x=1",
        "//example.test/a//b/../c?x=1",
    ] {
        let state = State::new(0, &base, location).expect("resolved target");
        assert_eq!(state.origin_form(), "/a//c?x=1");
    }
}

#[test]
fn absolute_scheme() {
    let base = "http://example.test/start"
        .parse::<target::Target>()
        .expect("absolute target");
    let request = Machine::new(1, &base, "/start", Method::GET, &[]).expect("request");
    let failure = request
        .response(status::Code::FOUND, || Some(b"ftp://other.test/file"))
        .err()
        .expect("unsupported scheme");
    assert!(matches!(
        failure.cause(),
        Error::Origin(sark_client::OriginError::UnsupportedScheme)
    ));
    assert_eq!(failure.safety(), Safety::PossiblyCommitted);
    assert!(matches!(
        State::new(0, &base, "http:80"),
        Err(Error::Redirect(redirect::Error::Uri(
            target::Error::Separator { .. }
        )))
    ));
    for (location, path) in [
        ("next/part:tag", "/next/part:tag"),
        ("next?value:a", "/next?value:a"),
    ] {
        let state = State::new(0, &base, location).expect("relative target");
        assert_eq!(state.origin_form(), path);
    }
}

#[test]
fn initial_revisit_is_loop() {
    let base = target::Target::parse(BASE).expect("absolute target");
    for location in SAME_TARGETS {
        for initial in INITIAL_TARGETS {
            for (status, method) in PRESERVED_METHODS {
                let machine = Machine::new(3, &base, initial, method, PAYLOAD).expect("target");
                let error = machine
                    .response(status, || Some(location))
                    .err()
                    .expect("same method and target must be rejected");
                assert!(matches!(
                    error.cause(),
                    Error::Redirect(redirect::Error::Loop)
                ));
                assert_eq!(error.safety(), Safety::PossiblyCommitted);
            }
        }
    }
}

#[test]
fn same_uri_method_rewrite() {
    let base = target::Target::parse(BASE).expect("absolute target");
    for initial in INITIAL_TARGETS {
        for status in REWRITE_STATUSES {
            let machine =
                Machine::new(2, &base, initial, Method::POST, PAYLOAD).expect("initial request");
            let machine = match machine
                .response(status, || Some(ALIAS))
                .expect("same URI with changed method")
            {
                Action::Follow(machine) => Some(machine),
                Action::Complete => None,
            }
            .expect("redirect must be followed");
            assert_eq!(machine.method(), &Method::GET);
            assert!(machine.body().is_empty());
            assert_eq!(machine.origin_form(), START);
            assert_eq!(machine.safety(), Safety::PossiblyCommitted);

            let failure = machine
                .response(status, || Some(START.as_bytes()))
                .err()
                .expect("GET revisit must remain a loop");
            assert!(matches!(
                failure.cause(),
                Error::Redirect(redirect::Error::Loop)
            ));
            assert_eq!(failure.safety(), Safety::PossiblyCommitted);
        }
    }
}

#[test]
fn rewrite_retains_history() {
    let base = target::Target::parse(BASE).expect("absolute target");
    let mut machine =
        Machine::new(5, &base, START, Method::POST, PAYLOAD).expect("initial request");
    for (status, location) in METHOD_CHAIN {
        machine = match machine
            .response(status, || Some(location))
            .expect("each target is new for its method")
        {
            Action::Follow(next) => Some(next),
            Action::Complete => None,
        }
        .expect("redirect must be followed");
    }
    let failure = machine
        .response(status::Code::SEE_OTHER, || Some(ALIAS))
        .err()
        .expect("previous GET target must remain a loop");
    assert!(matches!(
        failure.cause(),
        Error::Redirect(redirect::Error::Loop)
    ));
    assert_eq!(failure.safety(), Safety::PossiblyCommitted);
}

#[test]
fn rewrite_keeps_limit() {
    let base = target::Target::parse(BASE).expect("absolute target");
    let mut machine =
        Machine::new(2, &base, START, Method::POST, PAYLOAD).expect("initial request");
    for (status, location) in METHOD_CHAIN.into_iter().take(2) {
        machine = match machine
            .response(status, || Some(location))
            .expect("admitted redirect")
        {
            Action::Follow(next) => Some(next),
            Action::Complete => None,
        }
        .expect("redirect must be followed");
    }
    let failure = machine
        .response(status::Code::FOUND, || Some(START.as_bytes()))
        .err()
        .expect("method rewrite must consume its hop");
    assert!(matches!(
        failure.cause(),
        Error::Redirect(redirect::Error::Limit)
    ));
    assert_eq!(failure.safety(), Safety::PossiblyCommitted);
}

#[test]
fn rejects_cross_origin() {
    let base = "http://a.example/start"
        .parse::<target::Target>()
        .expect("absolute target");
    for (initial, location) in [
        ("/start", "http://b.example/final"),
        ("/start", "https://a.example/final"),
        ("/start", "http://a.example:8080/final"),
        ("http://b.example/start", "http://a.example/final"),
    ] {
        for status in [status::Code::SEE_OTHER, status::Code::TEMPORARY_REDIRECT] {
            let machine =
                Machine::new(2, &base, initial, Method::POST, PAYLOAD).expect("redirect machine");
            let failure = machine
                .response(status, || Some(location.as_bytes()))
                .err()
                .expect("cross-origin redirect");
            assert!(matches!(
                failure.cause(),
                Error::Request(sark_client::RequestError::CrossOrigin)
            ));
            assert_eq!(failure.safety(), Safety::PossiblyCommitted);
        }
    }
}

#[test]
fn default_port_is_same() {
    let base = "http://example.test/start"
        .parse::<target::Target>()
        .expect("absolute target");
    for location in [
        "http://EXAMPLE.test:80/start?next=1",
        "http://EXAMPLE.test:00080/start?next=2",
    ] {
        let machine = Machine::new(2, &base, "/start", Method::GET, &[]).expect("redirect machine");
        let machine = match machine.response(status::Code::FOUND, || Some(location.as_bytes())) {
            Ok(Action::Follow(machine)) => Some(machine),
            _ => None,
        }
        .expect("absolute redirect");
        assert!(machine.is_base());
    }
}

#[test]
fn aliases_reuse_initial_origin() {
    let base = target::Target::parse("http://a.example/start").expect("base");
    let initial = "http://[::1]/start";
    let mut machine = Machine::new(3, &base, initial, Method::GET, &[]).expect("request");
    for location in ["http://[0:0:0:0:0:0:0:1]:80/start?next=1", "/final"] {
        machine = match machine
            .response(status::Code::FOUND, || Some(location.as_bytes()))
            .expect("same origin")
        {
            Action::Follow(next) => Some(next),
            Action::Complete => None,
        }
        .expect("redirect");
        assert_eq!(machine.origin().authority(), "[::1]");
        assert!(!machine.is_base());
    }
    assert_eq!(machine.origin_form(), "/final");
    let failure = machine
        .response(status::Code::FOUND, || {
            Some(b"http://[0:0:0:0:0:0:0:1]:00080/start")
        })
        .err()
        .expect("same IPv6 resource");
    assert!(matches!(
        failure.cause(),
        Error::Redirect(redirect::Error::Loop)
    ));
}

#[test]
fn absolute_tracks_origin() {
    let base = "http://a.example/start"
        .parse::<target::Target>()
        .expect("absolute target");
    for (target, same) in [
        ("http://A.example:80/final", true),
        ("http://b.example/final", false),
    ] {
        let (parsed, raw) = Tracker::usage(|| target.parse::<target::Target>());
        let parsed = parsed.expect("absolute URI");
        let (state, cost) = Tracker::usage(|| State::new(2, &base, target));
        let state = state.expect("absolute target");
        assert_eq!(state.origin(), &parsed);
        assert_eq!(state.is_base(), same);
        assert_eq!(cost, raw, "history storage starts with the first redirect");
    }
}

#[test]
fn other_status_completes() {
    let base = "http://example.test/start"
        .parse::<target::Target>()
        .expect("absolute target");
    for status in [200, 300, 304, 305, 306, 399, 404, 503] {
        let machine =
            Machine::new(2, &base, "/start", Method::GET, b"borrowed").expect("redirect machine");
        let status = (status::Code::from_u16(status)).expect("valid status");
        let mut read = false;
        let (action, cost) = Tracker::usage(|| {
            machine.response(status, || {
                read = true;
                None
            })
        });
        assert!(matches!(action, Ok(Action::Complete)));
        assert!(!read);
        assert_eq!(cost, (0, 0));
    }
}

#[test]
fn method_rewrite_contract() {
    let base = "http://example.test/start"
        .parse::<target::Target>()
        .expect("absolute target");
    let location = b"/next".as_slice();
    let custom = Method::from_bytes(b"CUSTOM-EXTENSION-METHOD").expect("extension method");
    let cases = [
        (301, Method::POST, "GET", false),
        (302, Method::POST, "GET", false),
        (301, Method::PUT, "PUT", true),
        (302, Method::HEAD, "HEAD", true),
        (303, Method::DELETE, "GET", false),
        (303, Method::GET, "GET", false),
        (303, Method::HEAD, "HEAD", true),
        (307, Method::GET, "GET", true),
        (307, Method::POST, "POST", true),
        (308, Method::PATCH, "PATCH", true),
        (307, custom, "CUSTOM-EXTENSION-METHOD", true),
    ];

    for (status, initial, expected, keeps) in cases {
        let body = if initial == Method::GET && status == 307 {
            &[][..]
        } else {
            &b"payload"[..]
        };
        let mut machine =
            Machine::new(3, &base, "/start", initial, body).expect("redirect machine");
        assert!(machine.preserves_body());
        let status = (status::Code::from_u16(status)).expect("valid redirect status");
        for (status, location) in [
            (status, location),
            (status::Code::TEMPORARY_REDIRECT, b"/later".as_slice()),
            (status::Code::PERMANENT_REDIRECT, b"/final".as_slice()),
        ] {
            machine = match machine.response(status, || Some(location)) {
                Ok(Action::Follow(machine)) => machine,
                _ => panic!("redirect status must produce a follow action"),
            };
            assert_eq!(machine.method().as_str(), expected);
            assert_eq!(machine.body(), if keeps { body } else { &[][..] });
            assert_eq!(machine.preserves_body(), keeps);
        }
    }
}

#[test]
fn history_marks_commitment() {
    let base = "http://example.test/start"
        .parse::<target::Target>()
        .expect("absolute target");
    let initial = Machine::new(
        2,
        &base,
        "http://example.test/start",
        Method::POST,
        b"effect",
    )
    .expect("absolute initial target");
    assert!(initial.safety() == Safety::BeforeSubmit);

    let location = b"/next".as_slice();
    let followed = match initial.response(status::Code::TEMPORARY_REDIRECT, || Some(location)) {
        Ok(Action::Follow(followed)) => Some(followed),
        _ => None,
    }
    .expect("307 must be followed");
    assert!(followed.safety() == Safety::PossiblyCommitted);

    let failure = (followed.response(status::Code::TEMPORARY_REDIRECT, || None))
        .err()
        .expect("invalid redirect after a response must fail");
    assert!(failure.safety() == Safety::PossiblyCommitted);
    assert!(matches!(failure.cause(), Error::Redirect(_)));
}
