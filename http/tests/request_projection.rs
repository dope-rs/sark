use sark_http::request::{Form, Layout, Target, span::Span};

#[test]
fn compact_span_is_exact() {
    let span = (Span::checked(0, u32::MAX as usize)).expect("test operation must succeed");
    let target = (Target::from_span(Form::Path, span)).expect("test operation must succeed");
    let layout = Layout::new(target, None, Some(span));

    assert_eq!(layout.route().map(Span::len), Some(u32::MAX as usize));
    assert!((layout.route()) == (Some(span)));
}

#[test]
fn views_borrow_owner() {
    let owner = b"/items/42?full=1origin.example";
    let target = (Span::checked(0, 16)).expect("test operation must succeed");
    let auth = (Span::checked(16, 14)).expect("test operation must succeed");
    let target = (Target::from_span(Form::Path, target)).expect("test operation must succeed");
    let route = (Span::checked(0, 9)).expect("test operation must succeed");
    let layout = Layout::new(target, Some(auth), Some(route));
    let route = layout.route().and_then(|span| span.get(owner));
    let query = layout
        .route()
        .and_then(|span| owner.get(span.end() + 1..layout.target().end()));
    let auth = layout.authority().and_then(|span| span.get(owner));

    assert_eq!(route, Some(&owner[..9]));
    assert_eq!(query, Some(&owner[10..16]));
    assert_eq!(auth, Some(&owner[16..]));
    assert_eq!(route.map(<[u8]>::as_ptr), Some(owner.as_ptr()));
}
