use sark_client::{
    Error, Safety,
    protocol::redirect::{self, Machine, State},
};
use sark_http::{method::Method, payload::response::status, uri::target};

pub mod common;
use common::redirect::{INVALID, INVALID_UTF8};

#[test]
fn rejects_erased_errors() {
    let base = target::Target::parse("https://example.test/root").expect("base");
    for (location, expected) in INVALID {
        assert!(matches!(
            State::new(1, &base, location),
            Err(Error::Redirect(redirect::Error::Uri(target::Error::Origin(error))))
                if error == expected
        ));
        let request = Machine::new(1, &base, "/start", Method::GET, &[]).expect("request");
        let failure = request
            .response(status::Code::FOUND, || Some(location.as_bytes()))
            .err()
            .expect("invalid location");
        assert!(matches!(
            failure.cause(),
            Error::Redirect(redirect::Error::Uri(target::Error::Origin(error)))
                if *error == expected
        ));
        assert_eq!(failure.safety(), Safety::PossiblyCommitted);
    }
}

#[test]
fn location_utf8_error() {
    let base = target::Target::parse("https://example.test/root").expect("base");
    for location in [None, Some(INVALID_UTF8)] {
        let request = Machine::new(1, &base, "/start", Method::GET, &[]).expect("request");
        let failure = request
            .response(status::Code::FOUND, || location)
            .err()
            .expect("invalid location");
        let expected = match location {
            None => matches!(failure.cause(), Error::Redirect(redirect::Error::Location)),
            Some(bytes) => {
                matches!(failure.cause(), Error::Redirect(redirect::Error::LocationUtf8(error))
                if Some(*error) == std::str::from_utf8(bytes).err())
            }
        };
        assert!(expected, "exact location error");
        assert_eq!(failure.safety(), Safety::PossiblyCommitted);
    }
}
