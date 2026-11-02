use sark_http::{
    field::text::IntError,
    h1,
    h1::{
        field::{self, expect},
        framing,
    },
};

#[test]
fn length_bounds() {
    assert!(
        framing::Fields::default().try_length(b"")
            == Err(field::Error::InvalidContentLength(IntError::Empty))
    );

    assert!(
        framing::Fields::default().try_length(b"184467440737095516160")
            == Err(field::Error::InvalidContentLength(IntError::Overflow))
    );

    assert!(
        framing::Fields::default().try_length(b"42, 42")
            == Err(field::Error::InvalidContentLength(IntError::InvalidByte))
    );
}

#[test]
fn request_fields() {
    assert_eq!(h1::CONTINUE_100, b"HTTP/1.1 100 Continue\r\n\r\n");

    let expect = expect::State::default().push(b"100-Continue");
    assert!(expect.finish(framing::RequestBody::Empty) == Ok(expect::Decision::None));
    assert!(expect.finish(framing::RequestBody::Fixed(1)) == Ok(expect::Decision::Continue));
    assert!(expect.push(b"100-continue") == expect);
    assert!(expect.push(b" , ") == expect);

    let invalid = expect::State::default().push(b"100-continue, fancy");
    assert!(invalid.finish(framing::RequestBody::Fixed(1)) == Err(expect::Error::Unsupported));
    assert!(
        invalid
            .push(b"100-continue")
            .finish(framing::RequestBody::Fixed(1))
            == Err(expect::Error::Unsupported)
    );

    let conn = (field::Connection::default().try_push(b"keep-alive, Upgrade, CLOSE, x-extension"))
        .expect("test operation must succeed");
    assert!(conn.is_keep());
    assert!(conn.is_upgrade());
    assert!(conn.is_close());

    for (value, index, byte) in [
        (b"close, bad token".as_slice(), 10, b' '),
        (b",\t, \tbad token,", 8, b' '),
        (b", \t,\xff", 4, 0xff),
    ] {
        assert!(
            field::Connection::default().try_push(value) == Err(field::TokenError { index, byte })
        );
    }
}

#[test]
fn response_control() {
    let keep = field::KeepAlive::default();

    let fixed = framing::ResponseBody::Fixed(0);
    let h11 = framing::Control::new(1, 200, fixed, field::Connection::default(), keep);
    assert!(h11.is_reusable());
    assert_eq!(h11.timeout(), None);

    let h10 = framing::Control::new(0, 200, fixed, field::Connection::default(), keep);
    assert!(!h10.is_reusable());
    assert_eq!(h10.timeout(), None);

    let persist = (field::Connection::default().try_push(b"keep-alive"))
        .expect("test operation must succeed");
    assert!(framing::Control::new(0, 200, fixed, persist, keep).is_reusable());

    let close = (field::Connection::default().try_push(b"keep-alive, close"))
        .expect("test operation must succeed");
    assert!(!framing::Control::new(1, 200, fixed, close, keep).is_reusable());

    let upgrade =
        (field::Connection::default().try_push(b"upgrade")).expect("test operation must succeed");
    assert!(!framing::Control::new(1, 200, fixed, upgrade, keep).is_reusable());
    assert!(
        !framing::Control::new(1, 101, framing::ResponseBody::None, persist, keep).is_reusable()
    );
    assert!(
        !framing::Control::new(
            1,
            200,
            framing::ResponseBody::UntilEof,
            field::Connection::default(),
            keep,
        )
        .is_reusable()
    );
}

#[test]
fn transfer_order() {
    let coding =
        (field::Transfer::default().parse(b"gzip; level=9")).expect("test operation must succeed");
    let coding =
        (coding.parse(b"custom; note=\"a,b\", chunked")).expect("test operation must succeed");
    assert!(coding.has_encoding());
    assert!(coding.is_chunked());
    assert!((coding.framing()) == (framing::Transfer::Chunked));
    assert!(coding.parse(b"gzip") == Err(field::value::Error::InvalidOrdering));

    let invalid = field::Transfer::default();
    assert!(invalid.parse(b"chunked, gzip") == Err(field::value::Error::InvalidOrdering));
    assert!(!invalid.has_encoding(), "push must be transactional");
    assert!(invalid.parse(b"chunked; forbidden=yes") == Err(field::value::Error::InvalidByte));
    assert!(!invalid.has_encoding(), "push must be transactional");
}
