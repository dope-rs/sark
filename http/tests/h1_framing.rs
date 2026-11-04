use sark_http::{
    h1::{field, framing, switch},
    payload::response::Method,
};

#[test]
fn fields_transaction() {
    let fields =
        (framing::Fields::default().try_length(b" 42\t")).expect("test operation must succeed");
    assert_eq!(fields.length(), Some(42));
    assert!(fields.try_set(7) == Err(field::Error::DuplicateContentLength));
    assert_eq!(fields.length(), Some(42));

    let coding = (framing::Fields::default().try_coding(b"gzip; level=9"))
        .expect("test operation must succeed");
    let coding = (coding.try_coding(b"chunked")).expect("test operation must succeed");
    assert!(coding.has_coding());
    assert!(coding.is_chunked());
    assert!(coding.finish().request() == Ok(framing::RequestBody::Chunked));

    let invalid =
        (framing::Fields::default().try_coding(b"chunked")).expect("test operation must succeed");
    assert!(
        invalid.try_coding(b"gzip")
            == Err(field::Error::InvalidTransferEncoding(
                field::value::Error::InvalidOrdering
            ))
    );
    assert!(
        invalid.is_chunked(),
        "a rejected field must not mutate state"
    );
}

#[test]
fn fields_conflict() {
    let length =
        (framing::Fields::default().try_length(b"4")).expect("test operation must succeed");
    assert!(length.try_coding(b"chunked") == Err(field::Error::ContentLengthWithTransferEncoding));
    assert!(length.finish().request() == Ok(framing::RequestBody::Fixed(4)));

    let coding =
        (framing::Fields::default().try_coding(b"chunked")).expect("test operation must succeed");
    assert!(coding.try_length(b"4") == Err(field::Error::ContentLengthWithTransferEncoding));
    assert!(coding.finish().response(Method::Other, 200) == Ok(framing::ResponseBody::Chunked));
}

#[test]
fn request_boundary() {
    assert!(
        framing::Message::new(None, framing::Transfer::None).request()
            == Ok(framing::RequestBody::Empty)
    );
    assert!(
        framing::Message::new(Some(7), framing::Transfer::None).request()
            == Ok(framing::RequestBody::Fixed(7))
    );
    assert!(
        framing::Message::new(None, framing::Transfer::Chunked).request()
            == Ok(framing::RequestBody::Chunked)
    );
    assert!(
        framing::Message::new(None, framing::Transfer::Other).request()
            == Err(framing::Error::UnframedRequestTransferEncoding)
    );
    assert!(
        framing::Message::new(Some(7), framing::Transfer::Chunked).request()
            == Err(framing::Error::ContentLengthWithTransferEncoding)
    );
}

#[test]
fn response_boundary() {
    let response = |request, status, message: framing::Message| message.response(request, status);
    let none = framing::Message::new(None, framing::Transfer::None);
    let fixed = framing::Message::new(Some(7), framing::Transfer::None);
    let chunked = framing::Message::new(None, framing::Transfer::Chunked);

    assert!(response(Method::Head, 200, fixed) == Ok(framing::ResponseBody::None));
    assert!(response(Method::Other, 200, fixed) == Ok(framing::ResponseBody::Fixed(7)));
    assert!(response(Method::Other, 200, chunked) == Ok(framing::ResponseBody::Chunked));
    assert!(response(Method::Other, 200, none) == Ok(framing::ResponseBody::UntilEof));
    assert!(
        response(Method::Connect, 200, none) == Err(framing::Error::SuccessfulConnectIsTransition)
    );
    assert!(
        response(Method::Connect, 205, none) == Err(framing::Error::SuccessfulConnectIsTransition)
    );
    assert!(
        response(Method::Connect, 204, none) == Err(framing::Error::SuccessfulConnectIsTransition)
    );
    assert!(response(Method::Other, 304, fixed) == Ok(framing::ResponseBody::None));
    assert!(response(Method::Other, 204, fixed) == Err(framing::Error::ForbiddenResponseFraming));
    assert!(response(Method::Other, 205, fixed) == Err(framing::Error::ForbiddenResponseFraming));
    assert!(
        response(
            Method::Other,
            205,
            framing::Message::new(Some(0), framing::Transfer::None),
        ) == Ok(framing::ResponseBody::Empty(framing::Empty::Fixed))
    );
    assert!(
        response(Method::Other, 205, chunked)
            == Ok(framing::ResponseBody::Empty(framing::Empty::Chunked))
    );
    assert!(
        response(Method::Other, 205, none)
            == Ok(framing::ResponseBody::Empty(framing::Empty::UntilEof))
    );
    assert!(response(Method::Head, 204, fixed) == Err(framing::Error::ForbiddenResponseFraming));
    assert!(
        response(
            Method::Head,
            200,
            framing::Message::new(Some(7), framing::Transfer::Chunked),
        ) == Err(framing::Error::ContentLengthWithTransferEncoding)
    );
}

#[test]
fn response_outcome() {
    let none = framing::Message::new(None, framing::Transfer::None);
    let fixed = framing::Message::new(Some(7), framing::Transfer::None);

    for framing in [
        none,
        fixed,
        framing::Message::new(Some(0), framing::Transfer::Chunked),
    ] {
        assert!(
            framing::Outcome::admit(
                Method::Connect,
                switch::Intent::Connect,
                200,
                framing,
                field::Connection::default(),
                None,
            ) == Ok(framing::Outcome::Switch(switch::Kind::Connect))
        );
    }
    assert!(
        framing::Outcome::admit(
            Method::Connect,
            switch::Intent::Connect,
            407,
            fixed,
            field::Connection::default(),
            None,
        ) == Ok(framing::Outcome::Message(framing::ResponseBody::Fixed(7)))
    );
    assert!(
        framing::Outcome::admit(
            Method::Other,
            switch::Intent::None,
            101,
            none,
            field::Connection::default(),
            None,
        ) == Err(framing::Response::Transition(
            switch::Error::UnexpectedSwitch
        ))
    );
}
