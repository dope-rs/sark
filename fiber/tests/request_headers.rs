use crate::common::h1::{fields, fields::Headers as _};
pub mod common;

use sark_core::{
    request::raw,
    route::{
        Project, Schema,
        field::header::{Fields as _, Slot as _},
    },
};
use sark_fiber::request::Head;
use sark_h1::{
    Input,
    wire::{head, head::line::Error, parsing},
};
use sark_http::{
    field::{
        head::{AcceptEncoding, MAX_HEADER_LINE_BYTES},
        metadata::{map::record, section::storage},
    },
    h1::{
        head::{
            line::{self, Request, Version},
            request::Coding,
        },
        target,
    },
    request::{Form, Layout, Target, span::Span},
};

use crate::common::route::glue::{
    Captured, EncodingReq, Failure, Full, HdrReq, HdrReqHeadersRaw, Minimal, MinimalHeadersRaw,
    ShortHdrReq, Wire,
};

#[test]
fn defaults_header() {
    for (wire, value) in fields::ENCODINGS {
        let expected = value.map(|raw| AcceptEncoding::try_parse(raw).expect("owner admission"));
        let source = parsing::Head::parse(Input::Borrowed(wire), u8::MAX)
            .expect("request line syntax")
            .expect("complete request line");
        let block = head::Block::<HdrReqHeadersRaw>::parse::<HdrReq>(
            wire,
            source.request().end(),
            16,
            Coding::Plain,
            source.request().version(),
        )
        .ok()
        .expect("header block syntax")
        .expect("complete absent-header block");
        assert_eq!(
            block.coding() == Coding::Gzip,
            expected.is_some_and(|value| value.gzip() != 0),
        );
        let span = source.request().target_span();
        let target = Target::from_span(Form::Path, span).expect("request path target");
        let owner =
            Head::bind(wire, Layout::new(target, None, Some(span))).expect("request head layout");
        // SAFETY: HdrReq parsed these headers from this exact retained input;
        // its generated parameter schema is the empty tuple.
        let request = unsafe { raw::Parsed::<_, HdrReq>::new(owner, (), block.into_headers()) };
        let parsed = <HdrReq as Project>::from_headers(&request)
            .expect("absent header field falls back to default");
        let (token, encoding) = parsed.captured();
        assert_eq!(token, fields::DEFAULT);
        assert_eq!(encoding, expected);
    }
    assert!(matches!(
        Wire::new(fields::BAD_ENCODING).try_request::<HdrReq>(16),
        Err(Failure::Head(head::Error::Line(Error::Field {
            cause: sark_http::field::head::Error::Value,
            ..
        })))
    ));
}

#[test]
fn captures_gzip() {
    let input: &[u8] = fields::GZIP;
    let parsed = match head::Block::<<EncodingReq as Schema>::RawHeaders>::parse::<EncodingReq>(
        input,
        0,
        16,
        Coding::Plain,
        Version::Eleven,
    ) {
        Ok(Some(parsed)) => Some(parsed),
        _ => None,
    }
    .expect("valid Accept-Encoding header");
    let gzip = matches!(parsed.coding(), Coding::Gzip);
    let headers = parsed.into_headers();

    assert!(gzip);
    assert_eq!(
        headers.captured(),
        Some(Span::checked(17, 4).expect("captured encoding span is representable"),),
    );
}

#[test]
fn scans_optional_headers() {
    for (coding, gzip) in [(Coding::Plain, false), (Coding::Gzip, true)] {
        assert!(
            head::Block::<MinimalHeadersRaw>::parse::<Minimal>(
                fields::GZIP,
                0,
                16,
                coding,
                Version::Eleven
            )
            .is_ok_and(|parsed| parsed
                .is_some_and(|parsed| matches!(parsed.coding(), Coding::Gzip) == gzip))
        );
    }

    let dupe = fields::DUPLICATE_EXPECT;
    assert!(matches!(
        head::Block::<MinimalHeadersRaw>::parse::<Minimal>(
            dupe,
            0,
            16,
            Coding::Plain,
            Version::Eleven
        ),
        Ok(Some(_))
    ));
    assert!(matches!(
        head::Block::<<Full as Schema>::RawHeaders>::parse::<Full>(
            dupe,
            0,
            16,
            Coding::Plain,
            Version::Eleven
        ),
        Ok(Some(_))
    ));
}

#[test]
fn checks_authority() {
    for wire in [fields::HOST.as_slice(), fields::ABSOLUTE, fields::OPTIONS] {
        assert!(matches!(
            Wire::new(wire).try_request::<Minimal>(16),
            Ok(Some(_))
        ));
    }

    for wire in [
        fields::EMPTY.as_slice(),
        fields::EMPTY_HOST,
        fields::DUPLICATE_HOST,
        fields::USER_HOST,
        fields::TEXT_PORT,
        fields::ASTERISK,
        fields::USER_TARGET,
    ] {
        let result = Wire::new(wire).try_request::<Minimal>(16);
        assert!(if wire.starts_with(fields::ASTERISK_PREFIX) {
            matches!(
                result,
                Err(Failure::Line(line::error::Request::Target(
                    target::Error::InvalidTarget,
                )))
            )
        } else {
            result.is_err()
        });
    }
}

#[test]
fn borrows_authority() {
    for (wire, expected) in [
        (fields::ORIGIN.as_slice(), fields::ORIGIN_HOST.as_slice()),
        (fields::TARGET, fields::TARGET_HOST),
    ] {
        let parsed = match Wire::new(wire).try_request::<Minimal>(16) {
            Ok(Some(parsed)) => Some(parsed),
            _ => None,
        }
        .expect("valid request");
        let host = parsed
            .layout()
            .authority_in(wire)
            .expect("admitted authority span stays inside the ingress owner");

        assert_eq!(host, expected);
        let owner = wire.as_ptr_range();
        assert!(host.as_ptr() >= owner.start);
        assert!(host.as_ptr_range().end <= owner.end);
    }
}

#[test]
fn preserves_prefixes() {
    let wire = fields::PREFIXES;
    let head = Request::parse(wire)
        .expect("well-formed request line")
        .expect("complete request line");

    for end in head.end()..wire.len() {
        assert!(
            matches!(Wire::new(&wire[..end]).try_request::<Minimal>(16), Ok(None)),
            "prefix {end}"
        );
    }
    assert!(matches!(
        Wire::new(wire).try_request::<Minimal>(16),
        Ok(Some(_))
    ));
}

#[test]
fn validates_header_value() {
    let input: &[u8] = fields::BARE_LF;
    assert!(
        matches!(
            head::Block::<HdrReqHeadersRaw>::parse::<HdrReq>(
                input,
                0,
                16,
                Coding::Plain,
                Version::Eleven
            ),
            Err(head::Error::Line(Error::Value {
                offset: 13,
                actual: b'\n',
            }))
        ),
        "a bare LF must not bypass value validation"
    );
}

#[test]
fn accepts_short_name() {
    let input: &[u8] = fields::SHORT;
    let parsed = match head::Block::<<ShortHdrReq as Schema>::RawHeaders>::parse::<ShortHdrReq>(
        input,
        0,
        16,
        Coding::Plain,
        Version::Eleven,
    ) {
        Ok(Some(parsed)) => Some(parsed),
        _ => None,
    }
    .expect("valid short custom header");
    let headers = parsed.into_headers();

    assert_eq!(
        headers.captured(),
        Some(Span::checked(4, 2).expect("short header span is representable"),),
    );
}

#[test]
fn dispatches_split_name() {
    let whole = <HdrReq as Schema>::Header::span(record::Span::from_slice(fields::NAME))
        .expect("contiguous generated header slot");
    let split =
        <HdrReq as Schema>::Header::span(record::Span::new(fields::NAME_FIRST, fields::NAME_LAST))
            .expect("split generated header slot");

    assert_eq!(whole.into_tag(), split.into_tag());
    let tag = storage::Tag::user(whole.into_tag());
    let restored = <HdrReq as Schema>::Header::from_tag(tag.user_slot().expect("user tag"))
        .expect("generated header slot");
    assert_eq!(restored.into_tag(), whole.into_tag());
}

#[test]
fn rejects_bad_values() {
    for block in [
        fields::LF_VALUE.as_slice(),
        fields::CR_VALUE,
        fields::NUL_VALUE,
        fields::BELL_VALUE,
        fields::DEL_VALUE,
    ] {
        assert!(
            head::Block::<<Minimal as Schema>::RawHeaders>::parse::<Minimal>(
                block,
                0,
                16,
                Coding::Plain,
                Version::Eleven
            )
            .is_err()
        );
    }
}

#[test]
fn accepts_field_bytes() {
    assert!(matches!(
        head::Block::<<Minimal as Schema>::RawHeaders>::parse::<Minimal>(
            fields::VALID,
            0,
            16,
            Coding::Plain,
            Version::Eleven
        ),
        Ok(Some(_))
    ));
}

#[test]
fn scans_lane_edges() {
    let long = fields::AGENT.padded(64);
    assert!(matches!(
        head::Block::<<Minimal as Schema>::RawHeaders>::parse::<Minimal>(
            &long,
            0,
            16,
            Coding::Plain,
            Version::Eleven
        ),
        Ok(Some(_))
    ));

    let partial = fields::PARTIAL;
    assert!(matches!(
        head::Block::<<Minimal as Schema>::RawHeaders>::parse::<Minimal>(
            partial,
            0,
            16,
            Coding::Plain,
            Version::Eleven
        ),
        Ok(None)
    ));
}

#[test]
fn enforces_header_count() {
    assert!(matches!(
        head::Block::<<Minimal as Schema>::RawHeaders>::parse::<Minimal>(
            fields::BLANK,
            0,
            0,
            Coding::Plain,
            Version::Eleven
        ),
        Ok(Some(_))
    ));
    assert!(matches!(
        head::Block::<MinimalHeadersRaw>::parse::<Minimal>(
            fields::HOST_LINE,
            0,
            0,
            Coding::Plain,
            Version::Eleven
        ),
        Err(head::Error::Fields {
            actual: 1,
            limit: 0,
        })
    ));
}

#[test]
fn enforces_line_limit() {
    let block = fields::LONG.padded(MAX_HEADER_LINE_BYTES + 16);
    assert!(
        matches!(head::Block::<MinimalHeadersRaw>::parse::<Minimal>(&block, 0, 16, Coding::Plain, Version::Eleven), Err(head::Error::Line(Error::Limit {
            observed,
            limit: MAX_HEADER_LINE_BYTES,
        })) if observed == block.len())
    );
}

#[test]
fn rejects_fold_aliases() {
    for input in [fields::CR_NAME.as_slice(), fields::FOLD_NAME] {
        assert!(
            head::Block::<HdrReqHeadersRaw>::parse::<HdrReq>(
                input,
                0,
                16,
                Coding::Plain,
                Version::Eleven
            )
            .is_err()
        );
    }
    assert!(matches!(
        head::Block::<HdrReqHeadersRaw>::parse::<HdrReq>(
            fields::MIXED_NAME,
            0,
            16,
            Coding::Plain,
            Version::Eleven
        ),
        Ok(Some(_))
    ));
}
