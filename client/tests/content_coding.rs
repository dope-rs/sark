use std::{io, pin::Pin};

use o3::{
    base::cell::region::Token,
    buffer::{resident, segments},
};
use sark_client::reply::decode::{Decoder, Policy};
use sark_http::payload::{
    compress::inflate::{self, Checked},
    response::Method,
};

pub mod common;
use common::response::{
    Decode as _, EMPTY_GZIP, GZIP, MULTIPLE, NESTED, PLAIN, SINGLE, TRANSFERS, bad_crc,
    transfer_wire,
};

#[test]
fn transfer_keeps_content() {
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |mut token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(32768, token.proof())
            .expect("resident budget");
        let segments = segments::Pool::try_new(segments::Plan {
            segments: 4,
            chains: 1,
        })
        .expect("body metadata");
        let decoder = Decoder::new(Policy::Strict, 4096, budget, &token);
        for (fields, body, coding) in TRANSFERS {
            let wire = transfer_wire(fields);
            let response = segments
                .try_wire(budget, Method::Other, &wire)
                .expect("chunked response");
            assert_eq!(response.body().contiguous(), Some(NESTED));
            let response = decoder
                .decode(&mut token, response)
                .expect("content decoding");
            assert_eq!(response.body().contiguous(), Some(body));
            assert_eq!(response.headers().get("content-encoding"), coding);
        }
    });
}

#[test]
fn multiple_codings() {
    let encoded = NESTED;
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |mut token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(4096, token.proof())
            .expect("resident budget");
        let segments = segments::Pool::try_new(segments::Plan {
            segments: 4,
            chains: 1,
        })
        .expect("body metadata");
        let decoder = Decoder::new(Policy::Strict, 4096, budget, &token);
        for (fields, coding) in MULTIPLE {
            let response = segments
                .try_body(budget, fields, encoded)
                .expect("response fixture");
            let response = decoder
                .decode(&mut token, response)
                .expect("preserved encoding");
            assert_eq!(response.body().contiguous(), Some(encoded));
            assert_eq!(response.headers().get("content-encoding"), Some(coding));
            assert_eq!(
                response.headers().get("content-length"),
                Some(encoded.len().to_string().as_bytes())
            );
        }
    });
}

#[test]
fn single_gzip() {
    let encoded = GZIP;
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |mut token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(4096, token.proof())
            .expect("resident budget");
        let segments = segments::Pool::try_new(segments::Plan {
            segments: 4,
            chains: 1,
        })
        .expect("body metadata");
        let decoder = Decoder::new(Policy::Strict, 4096, budget, &token);
        let response = segments
            .try_body(budget, SINGLE, encoded)
            .expect("response fixture");
        let response = decoder.decode(&mut token, response).expect("decoded gzip");
        assert_eq!(response.body().contiguous(), Some(PLAIN));
        assert!(response.headers().get("content-encoding").is_none());
        assert!(response.headers().get("content-length").is_none());
    });
}

#[test]
fn empty_gzip_framing() {
    use sark_client::Error;
    use sark_http::payload::compress::inflate;

    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |mut token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(4096, token.proof())
            .expect("budget");
        let segments = segments::Pool::try_new(segments::Plan {
            segments: 1,
            chains: 0,
        })
        .expect("metadata");
        let decoder = Decoder::new(Policy::Strict, 4096, budget, &token);
        for (method, wire) in EMPTY_GZIP {
            let response = segments.try_wire(budget, method, wire).expect("response");
            let response = decoder
                .decode(&mut token, response)
                .expect("metadata-only coding");
            assert_eq!(
                response.headers().get("content-encoding"),
                Some(b"gzip".as_slice())
            );
        }
        let response = segments.try_body(budget, SINGLE, &[]).expect("response");
        assert!(matches!(decoder.decode(&mut token, response),
            Err(Error::Decompression(inflate::DecodeError::Stream(error)))
                if matches!(error.error(), inflate::Error::Eof { part: inflate::Part::Header, .. })
        ));
    });
}

#[test]
fn member_limit() {
    use sark_client::Error;

    let mut encoded = GZIP.to_vec();
    encoded.extend_from_slice(GZIP);
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |mut token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(4096, token.proof())
            .expect("resident budget");
        let segments = segments::Pool::try_new(segments::Plan {
            segments: 4,
            chains: 1,
        })
        .expect("metadata");
        for policy in [Policy::Strict, Policy::Lenient] {
            for limit in [PLAIN.len(), PLAIN.len() * 2] {
                let decoder = Decoder::new(policy, limit, budget, &token);
                let response = segments
                    .try_body(budget, SINGLE, &encoded)
                    .expect("response");
                let result = decoder.decode(&mut token, response);
                if limit == PLAIN.len() && policy == Policy::Strict {
                    assert!(
                        matches!(result, Err(Error::Decompression(inflate::DecodeError::SizeLimit { actual, limit })) if actual > limit)
                    );
                    continue;
                }
                let response = result.expect("decoded or retained response");
                if limit == PLAIN.len() {
                    assert_eq!(response.body().contiguous(), Some(encoded.as_slice()));
                    assert_eq!(
                        response.headers().get("content-encoding"),
                        Some(b"GZip".as_slice())
                    );
                } else {
                    let body = response.body();
                    let bytes = body.contiguous().expect("contiguous decoded body");
                    assert_eq!(bytes.len(), PLAIN.len() * 2);
                    assert_eq!(&bytes[..PLAIN.len()], PLAIN);
                    assert_eq!(&bytes[PLAIN.len()..], PLAIN);
                    assert!(response.headers().get("content-encoding").is_none());
                }
            }
        }
    });
}

#[test]
fn checked_lenient_cancels() {
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |mut token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(4096, token.proof())
            .expect("resident budget");
        let segments = segments::Pool::try_new(segments::Plan {
            segments: 1,
            chains: 0,
        })
        .expect("body metadata");
        let decoder = Decoder::new(Policy::Lenient, 4096, budget, &token);
        let free = budget.available();
        let response = segments.try_body(budget, SINGLE, GZIP).expect("response");
        let held = budget.available();
        let mut work = 0;
        let failure = decoder
            .decode_with(&mut token, response, || {
                if budget.available() < held {
                    work += 1;
                    if work == 3 {
                        return Err(io::ErrorKind::TimedOut);
                    }
                }
                Ok(())
            })
            .err()
            .expect("interrupted gzip work");
        assert!(matches!(failure, Checked::Check(io::ErrorKind::TimedOut)));
        assert_eq!(budget.available(), free);
        assert_eq!(segments.available().segments, 1);

        let response = segments
            .try_body(budget, SINGLE, GZIP)
            .expect("reuse metadata");
        let response = decoder.decode(&mut token, response).expect("reuse decoder");
        assert_eq!(response.body().contiguous(), Some(PLAIN));
    });
}

#[test]
fn checked_work_precedes() {
    let encoded = bad_crc();
    let mut domain = resident::Domain::new();
    Token::scope_pinned(Pin::new(&mut domain), |mut token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(4096, token.proof())
            .expect("resident budget");
        let segments = segments::Pool::try_new(segments::Plan {
            segments: 1,
            chains: 0,
        })
        .expect("body metadata");
        let decoder = Decoder::new(Policy::Strict, 4096, budget, &token);
        let free = budget.available();
        let response = segments
            .try_body(budget, SINGLE, &encoded)
            .expect("response");
        let held = budget.available();
        let mut charged = false;
        let mut check = || {
            if budget.available() < held {
                charged = true;
            } else if charged {
                return Err(io::ErrorKind::TimedOut);
            }
            Ok(())
        };
        let failure = decoder
            .decode_with(&mut token, response, &mut check)
            .err()
            .expect("completed checksum failure");
        assert!(matches!(failure,
            Checked::Work(sark_client::Error::Decompression(inflate::DecodeError::Stream(error)))
                if error.written() == PLAIN.len()
                    && matches!(error.error(), inflate::Error::Checksum { expected, actual }
                        if expected == actual ^ 1)
        ));
        assert_eq!(check(), Err(io::ErrorKind::TimedOut));
        assert_eq!(budget.available(), free);
        assert_eq!(segments.available().segments, 1);
    });
}
