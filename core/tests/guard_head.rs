pub mod common;

use common::requests::{GUARD_PATHS, ORIGIN_AUTHORITY};
use sark_core::{request, route::guard::Head};
use sark_http::{
    h1::target,
    method::Method,
    request::{Form, Layout, span::Span},
};

#[test]
fn authority_routing_has_no_guard_path() {
    let method = Method::CONNECT;
    let authority = ORIGIN_AUTHORITY;
    let target = target::Target::parse(method.as_bytes(), authority).expect("CONNECT target");
    let span = Span::checked(0, authority.len()).expect("authority span");
    let layout = Layout::try_new(Form::Authority, span, None, None).expect("authority layout");
    let retained = request::Head::<_>::bind(authority, layout).expect("retained head");

    for head in [
        Head::from_parts(
            &method,
            Form::Authority,
            target.route_path(authority),
            target.authority().and_then(|span| span.get(authority)),
        ),
        Head::from_parts(
            &method,
            retained.form(),
            Some(retained.path()),
            retained.authority(),
        ),
    ] {
        assert!(head.path().is_none());
        assert!(std::ptr::eq(
            head.authority().expect("borrowed authority"),
            authority,
        ));
    }
}

#[test]
fn asterisk_and_extended_connect_keep_their_guard_paths() {
    for (method, form, path) in GUARD_PATHS {
        let span = Span::checked(0, path.len()).expect("target span");
        let route = (form == Form::Extended).then_some(span);
        let layout = Layout::try_new(form, span, None, route).expect("request layout");
        let retained = request::Head::<_>::bind(path, layout).expect("retained head");
        let head = Head::from_parts(
            &method,
            retained.form(),
            Some(retained.path()),
            retained.authority(),
        );

        assert_eq!(head.path(), Some(path));
    }
}
