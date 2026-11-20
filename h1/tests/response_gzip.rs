pub mod common;

use std::{cell::Cell, str};

use common::{
    body::{COMPRESSIBLE, Counted, TEMPLATE, replies},
    fields::{self, DATE, ENTRIES},
};
use dope_test::alloc::Tracker;
use o3::buffer::{storage, write::Slice};
use sark_h1::{
    route,
    wire::{Failure, output},
};
use sark_http::{
    field,
    h1::head::{
        request::Coding,
        response::{Date, Server},
    },
    owner::evidence,
    payload::{
        compress::{self, Compression, Gzip, Output},
        fixed::Body,
        response::{
            Bounded, Exact, Fixed, Static, exact,
            headers::{self, head::Head, set},
            status::Code,
        },
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn borrowed_plain_uses_output_room() {
    let source = Vec::from(b"plain".as_slice());
    let mut gzip = Gzip::try_new().expect("gzip");
    let mut head = 512;
    for inline in [true, false] {
        let mut bytes = [0; 512];
        let mut writer = Slice::new(&mut bytes[..head]);
        let (outcome, allocs) = Tracker::count(|| {
            output::Egress::new(writer.reborrow(), DATE, fields::GZIP_RESPONSE).try_route(
                Fixed::new(Code::OK, (), source.as_slice()),
                &mut gzip,
                replies::POLICY,
            )
        });
        match outcome {
            Ok(output::Complete::Copy { copy }) if inline => {
                let written = writer.len();
                assert_eq!(allocs, 0);
                assert!(copy.cause == evidence::Cause::Inline);
                assert_eq!((copy.read, copy.written), (source.len(), source.len()));
                head = written - source.len();
                assert_eq!(&bytes[head..written], source.as_slice());
            }
            Ok(output::Complete::Owned { body, cause }) if !inline => {
                assert_eq!(allocs, 1);
                assert_eq!(writer.len(), head);
                assert!(cause == evidence::Cause::Borrowed);
                assert_eq!(body.as_ref(), source.as_slice());
            }
            _ => panic!("borrowed placement"),
        }
    }
}

#[test]
fn frames_compressed() {
    let payload = COMPRESSIBLE;
    let result = storage::Shared::try_copy(payload);
    let body = result.expect("fixture body fits shared storage");
    let response = Fixed::new(
        Code::OK,
        Head::new(&TEMPLATE, set::Set::from_items([])),
        body,
    );
    let date = b"Mon, 01 Jan 2026 00:00:00 GMT";
    let mut bytes = [0; 1024];
    let mut writer = Slice::new(&mut bytes);
    let result = Gzip::try_new();
    let mut gzip = result.expect("test gzip pool allocation");
    let outcome = output::Egress::new(writer.reborrow(), date, fields::GZIP_RESPONSE).try_route(
        response,
        &mut gzip,
        route::Policy::new(route::Mode::Gzip, Date::Omit, Server::Omit),
    );
    let Ok(output::Complete::Compressed { body, read }) = outcome else {
        panic!("gzip policy must retain the compressed owner");
    };
    let result = str::from_utf8(writer.into_mut());
    let head = result.expect("valid H1 head");
    assert_eq!(read, payload.len());
    assert!(head.contains("content-encoding: gzip\r\n"));
    assert!(head.contains("vary: Accept-Encoding\r\n"));
    assert!(head.contains(&format!("Content-Length: {}\r\n", body.len())));
    assert!(!head.contains(&format!("Content-Length: {}\r\n", payload.len())));
    assert!(head.ends_with("\r\n\r\n"));
    assert!(!head.contains("Date:"));
    assert!(!head.contains("Server:"));
}

#[test]
fn required_releases_inline_and_retains_spill() {
    let mut gzip = Gzip::try_new().expect("gzip storage");
    let expected = gzip.try_encode(b"").expect("empty gzip");
    let policy = route::Policy::new(route::Mode::RequiredGzip, Date::Omit, Server::Omit);
    let encodes = Cell::new(0);
    let mut bytes = [0; 8192];
    let mut writer = Slice::new(&mut bytes);
    let mut head = 0;
    for _ in 0..=Gzip::SLOTS.get() {
        let start = writer.len();
        let outcome = output::Egress::new(writer.reborrow(), DATE, fields::GZIP_RESPONSE)
            .try_route(Counted::bounded(b"", &encodes), &mut gzip, policy);
        let Ok(output::Complete::Copy { copy }) = outcome else {
            panic!("required member releases its slot");
        };
        assert!(matches!(copy.cause, evidence::Cause::Compressed));
        assert_eq!((copy.read, copy.written), (0, expected.len()));
        head = writer.len() - start - copy.written;
        assert!(writer.reborrow().into_mut().ends_with(expected.as_ref()));
    }
    let mut bytes = [0; 512];
    let mut writer = Slice::new(&mut bytes[..head]);
    let outcome = output::Egress::new(writer.reborrow(), DATE, fields::GZIP_RESPONSE).try_route(
        Counted::bounded(b"", &encodes),
        &mut gzip,
        policy,
    );
    let Ok(output::Complete::Compressed { body, read }) = outcome else {
        panic!("spill keeps its frozen owner");
    };
    assert_eq!(read, 0);
    assert_eq!(body.as_ref(), expected.as_ref());
    assert_eq!(encodes.get(), Gzip::SLOTS.get() + 2);
}

#[test]
fn actual_content_encoding_controls_admission() {
    let mut gzip = Gzip::try_new().expect("gzip storage");
    let context = fields::RESPONSE.with_coding(Coding::GzipOnly);
    for mode in [route::Mode::Plain, route::Mode::RequiredGzip] {
        for actual in [false, true] {
            let emissions = Cell::new(0);
            let encodes = Cell::new(0);
            let mut bytes = [0; 2048];
            let mut writer = Slice::new(&mut bytes);
            let outcome = output::Egress::new(writer.reborrow(), DATE, context).try_route(
                Bounded::new(
                    Code::OK,
                    fields::Drifting {
                        emissions: &emissions,
                        first: fields::IDENTITY,
                        second: if actual {
                            fields::IDENTITY
                        } else {
                            headers::Entry::new(
                                field::literal!(headers::Name::try_bytes(b"x-content-length")),
                                fields::IDENTITY.value(),
                            )
                        },
                    },
                    Counted {
                        bytes: COMPRESSIBLE,
                        encodes: &encodes,
                    },
                ),
                &mut gzip,
                route::Policy::new(mode, Date::Omit, Server::Omit),
            );
            let required = matches!(mode, route::Mode::RequiredGzip);
            match (required, actual, outcome) {
                (true, true, Err(Failure::Output(error))) => {
                    assert_eq!(error, output::Error::Encoding { actual: 2 });
                }
                (false, false, Err(Failure::Output(error))) => {
                    assert_eq!(error, output::Error::NotAcceptable(Coding::GzipOnly));
                }
                (true, false, Ok(output::Complete::Copy { copy }))
                | (false, true, Ok(output::Complete::Copy { copy })) => {
                    assert!(matches!(
                        (required, copy.cause),
                        (true, evidence::Cause::Compressed) | (false, evidence::Cause::Inline)
                    ));
                }
                _ => panic!("actual content encoding"),
            }
            assert_eq!(emissions.get(), 2);
            assert_eq!(encodes.get(), 1);
        }
    }
}

#[test]
fn required_exhaustion_releases_source() {
    let mut gzip = Gzip::try_new().expect("gzip storage");
    let held: Vec<_> = (0..Gzip::SLOTS.get())
        .map(|_| gzip.try_encode(b"x").expect("fill gzip slots"))
        .collect();
    let source = storage::Shared::try_copy(b"x").expect("source");
    let mut bytes = [0; 512];
    let mut writer = Slice::new(&mut bytes);
    let (outcome, counts) = Tracker::counts(|| {
        output::Egress::new(writer.reborrow(), DATE, fields::GZIP_RESPONSE).try_route(
            Fixed::new(Code::OK, (), source),
            &mut gzip,
            route::Policy::new(route::Mode::RequiredGzip, Date::Omit, Server::Omit),
        )
    });
    let Err(Failure::Output(error)) = outcome else {
        panic!("required gzip preserves exhaustion");
    };
    assert_eq!(
        error,
        output::Error::Compression(compress::Error::Exhausted)
    );
    assert_eq!(
        error.body().as_slice(),
        sark_http::h1::codec::canned::SERVICE_UNAVAILABLE.as_slice(),
    );
    assert_eq!(counts, [0, 1]);
    drop(held);
}

#[test]
fn identity_refusal_preserves_prior_errors() {
    let context = fields::RESPONSE.with_coding(Coding::Refused);
    let encodes = Cell::new(0);
    let mut bytes = [0; 512];
    let mut writer = Slice::new(&mut bytes);
    let outcome = output::Egress::new(writer.reborrow(), DATE, context).try_route(
        Exact::new(Code::OK, (), replies::Untouched(&encodes)),
        &mut (),
        common::fields::PLAIN,
    );
    assert!(matches!(
        outcome,
        Err(Failure::Output(output::Error::Size(exact::Overflow {
            current: usize::MAX, added,
        }))) if added == std::num::NonZero::<usize>::MIN
    ));
    assert_eq!(writer.len(), 0);
    assert_eq!(encodes.replace(0), 1);
    for capacity in [0, bytes.len()] {
        let mut writer = Slice::new(&mut bytes[..capacity]);
        let outcome = output::Egress::new(writer.reborrow(), DATE, context).try_route(
            Counted::exact(&TEMPLATE, b"plain", &encodes),
            &mut (),
            common::fields::PLAIN,
        );
        let Err(Failure::Output(error)) = outcome else {
            panic!("identity response must be refused");
        };
        let expected = if capacity == 0 {
            output::Error::Wire(sark_h1::wire::response::Error::WriteCapacity {
                required: 17,
                actual: 0,
            })
        } else {
            output::Error::NotAcceptable(Coding::Refused)
        };
        assert_eq!(error, expected);
        assert_eq!(encodes.replace(0), usize::from(capacity != 0));
    }
}

#[test]
fn fixed_fields_emit_once() {
    let emits = Cell::new(0);
    let response = Fixed::new(
        Code::OK,
        fields::Drifting::values(&emits, b"first", b"a different second value"),
        storage::Shared::try_copy(COMPRESSIBLE).expect("body"),
    );
    let mut bytes = [0; 512];
    let mut writer = Slice::new(&mut bytes);
    let mut gzip = Gzip::try_new().expect("gzip");
    let (outcome, allocs) = Tracker::count(|| {
        output::Egress::new(writer.reborrow(), DATE, fields::GZIP_RESPONSE).try_route(
            response,
            &mut gzip,
            fields::GZIP_POLICY,
        )
    });
    let length = match outcome {
        Ok(output::Complete::Compressed { .. }) => Some(writer.len()),
        _ => None,
    }
    .expect("compressed response");
    assert_eq!(emits.get(), 1);
    assert_eq!(allocs, 0);
    assert!(
        str::from_utf8(&bytes[..length])
            .expect("head")
            .contains("x-drift: first\r\n")
    );
}

#[test]
fn exact_fields_emit_once() {
    for capacity in [256, 2048] {
        let emits = Cell::new(0);
        let encodes = Cell::new(0);
        let response = Exact::new(
            Code::OK,
            fields::Drifting::values(&emits, b"first", b"a different second value"),
            Counted {
                bytes: COMPRESSIBLE,
                encodes: &encodes,
            },
        );
        let mut bytes = [0; 2048];
        let mut writer = Slice::new(&mut bytes[..capacity]);
        let mut gzip = Gzip::try_new().expect("gzip");
        let (outcome, allocs) = Tracker::count(|| {
            output::Egress::new(writer.reborrow(), DATE, fields::GZIP_RESPONSE).try_route(
                response,
                &mut gzip,
                fields::GZIP_POLICY,
            )
        });
        let length = match outcome {
            Ok(output::Complete::Compressed { .. }) => Some(writer.len()),
            _ => None,
        }
        .expect("compressed response");
        assert_eq!(emits.get(), 1);
        assert_eq!(encodes.get(), 1);
        if capacity == bytes.len() {
            assert_eq!(allocs, 0);
        }
        assert!(
            str::from_utf8(&bytes[..length])
                .expect("head")
                .contains("x-drift: first\r\n")
        );
    }
}

#[test]
fn bounded_keeps_fields() {
    let emits = Cell::new(0);
    let encodes = Cell::new(0);
    let response = Bounded::new(
        Code::OK,
        fields::Drifting::values(&emits, b"measured", b"emitted"),
        Counted {
            bytes: COMPRESSIBLE,
            encodes: &encodes,
        },
    );
    let mut bytes = [0; 256];
    let mut writer = Slice::new(&mut bytes);
    let mut gzip = Gzip::try_new().expect("gzip");
    let outcome = output::Egress::new(writer.reborrow(), DATE, fields::GZIP_RESPONSE).try_route(
        response,
        &mut gzip,
        fields::GZIP_POLICY,
    );
    let (len, body) = match outcome {
        Ok(output::Complete::Compressed { body, .. }) => Some((writer.len(), body)),
        _ => None,
    }
    .expect("bounded response must compress");
    assert_eq!(emits.get(), 2);
    assert_eq!(encodes.get(), 1);
    let head = str::from_utf8(&bytes[..len]).expect("head");
    assert!(head.contains("x-drift: emitted\r\n"));
    assert!(head.contains("content-encoding: gzip\r\n"));
    assert!(head.contains(&format!("Content-Length: {}\r\n", body.len())));
}

#[test]
fn fixed_scalar_capacity() {
    let mut gzip = Gzip::try_new().expect("gzip fixture");
    let zipped = match gzip.encode(COMPRESSIBLE) {
        Output::Compressed(body) => Some(body),
        Output::Plain => None,
    }
    .expect("compressed fixture");
    let fields = [
        ENTRIES[0],
        ENTRIES[1],
        headers::Entry::new(
            field::literal!(headers::Name::try_bytes(b"content-encoding")),
            field::literal!(field::Value::try_bytes(b"gzip")),
        ),
        headers::Entry::new(
            field::literal!(headers::Name::try_bytes(b"vary")),
            field::literal!(field::Value::try_bytes(b"Accept-Encoding")),
        ),
    ];
    let mut complete = [0xa5; 256];
    let mut writer = Slice::new(&mut complete);
    let response = Static::new(Code::OK, fields, Body::new(zipped.as_ref()));
    let head_len = match output::Egress::new(writer.reborrow(), DATE, fields::RESPONSE).try_route(
        response,
        &mut (),
        common::fields::PLAIN,
    ) {
        Ok(output::Complete::Static { .. }) => Some(writer.len()),
        _ => None,
    }
    .expect("complete gzip head");

    for capacity in 0..=head_len {
        let mut expected = [0xa5; 256];
        let mut writer = Slice::new(&mut expected[..capacity]);
        let response = Static::new(Code::OK, ENTRIES, Body::new(COMPRESSIBLE));
        let plain = output::Egress::new(writer.reborrow(), DATE, fields::RESPONSE).try_route(
            response,
            &mut (),
            common::fields::PLAIN,
        );
        let wanted = match plain {
            Err(Failure::Output(error)) => Some(Err(error)),
            Ok(output::Complete::Static { .. }) => {
                let response = Static::new(Code::OK, fields, Body::new(zipped.as_ref()));
                let mut writer = Slice::new(&mut expected[..capacity]);
                match output::Egress::new(writer.reborrow(), DATE, fields::RESPONSE).try_route(
                    response,
                    &mut (),
                    common::fields::PLAIN,
                ) {
                    Err(Failure::Output(error)) => Some(Err(error)),
                    Ok(output::Complete::Static { .. }) => Some(Ok(writer.len())),
                    _ => None,
                }
            }
            _ => None,
        }
        .expect("scalar response or capacity failure");

        let mut actual = [0xa5; 256];
        let mut writer = Slice::new(&mut actual[..capacity]);
        let response = Fixed::new(Code::OK, ENTRIES, Body::new(COMPRESSIBLE));
        let outcome = output::Egress::new(writer.reborrow(), DATE, fields::GZIP_RESPONSE)
            .try_route(response, &mut gzip, fields::GZIP_POLICY);
        let got = match outcome {
            Err(Failure::Output(error)) => Some(Err(error)),
            Ok(output::Complete::Compressed { .. }) => Some(Ok(writer.len())),
            _ => None,
        }
        .expect("compressed response or capacity failure");
        assert_eq!(got, wanted, "capacity {capacity}");
        assert_eq!(actual, expected, "capacity {capacity}");
    }
}
