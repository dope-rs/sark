pub mod common;

use common::{
    codec::{
        Output, Scope as _,
        decoder::{self, Decoder},
    },
    response::{RESET_CHUNKS, RESET_HEAD},
};
use dope::manifold::client::session::codec;
use o3::buffer::{resident, storage::Shared};
use sark_client::protocol::{
    self,
    codec::{Codec, output},
    response,
};
use sark_http::{h1::switch::Error, payload::response::Method};

#[test]
fn reset_chunk_limit() {
    use sark_http::h1::{chunk, codec::decode::chunks};

    resident::Domain::try_scope(64, |budget| {
        let codec = Codec::default();
        let decoder = Decoder::try_new(&codec).expect("staging");
        let head = Shared::try_copy(RESET_HEAD).expect("head");
        for (index, (prefix, tail)) in RESET_CHUNKS.into_iter().enumerate() {
            let mut state = codec::Stateful::state(&codec, budget);
            let (output, _) = decoder
                .try_parse(&mut state, &head, &budget)
                .expect("head admission")
                .expect("head");
            assert!(output.completion() == output::Completion::Pending);
            drop(output);
            let prefix = Shared::try_copy(prefix).expect("chunk prefix");
            assert!(
                decoder
                    .try_parse(&mut state, &prefix, &budget)
                    .expect("chunk size prefix")
                    .is_none()
            );
            let tail = Shared::try_copy(tail).expect("chunk tail");
            let result = decoder.try_parse(&mut state, &tail, &budget);
            if index == 0 {
                let (output, _) = result.expect("zero chunk").expect("completion");
                assert!(output.completion() == output::Completion::Complete);
                assert!(output.into_events().next().is_none());
            } else {
                assert!(matches!(
                    result,
                    Err(decoder::Error::Protocol(protocol::Error::Chunk(
                        chunks::Error::Framing(chunk::Error::Limit {
                            axis: chunk::Axis::Body,
                            actual: 1,
                            maximum: 0,
                        })
                    )))
                ));
            }
        }
    })
    .expect("resident domain");
}

#[test]
fn correlates_pipeline() {
    resident::Domain::try_scope(512, |budget| {
        let codec = Codec::default();
        let decoder = Decoder::try_new(&codec).expect("HTTP codec staging");
        let mut state = codec::Stateful::state(&codec, budget);
        let wire = (Shared::try_copy(
            concat!(
                "HTTP/1.1 103 Early Hints\r\nLink: </style.css>\r\n\r\n",
                "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n",
                "HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\nok",
            )
            .as_bytes(),
        ))
        .expect("response fixture storage");

        let (output, consumed) = decoder
            .try_item(&mut state, &wire, &budget, || Some(Method::Head))
            .expect("complete response item must parse")
            .expect("complete response item");
        let end = output.completion();
        assert!(matches!(
            Output::new(output).only(),
            Some(Ok(response::Event::Informational(_)))
        ));
        assert!((end) == (output::Completion::Pending));

        let rest = (wire.try_get(consumed.get()..))
            .expect("final HEAD response")
            .expect("final HEAD response");
        let (output, used) = decoder
            .try_item(&mut state, &rest, &budget, || Some(Method::Head))
            .expect("complete response item must parse")
            .expect("complete response item");
        let end = output.completion();
        assert!(matches!(
            Output::new(output).only(),
            Some(Ok(response::Event::Head(_)))
        ));
        assert!((end) == (output::Completion::Complete));

        let offset = (consumed.checked_add(used.get())).expect("combined response lengths");
        let rest = (wire.try_get(offset.get()..))
            .expect("GET response")
            .expect("GET response");
        let (output, used) = decoder
            .try_item(&mut state, &rest, &budget, || Some(Method::Other))
            .expect("complete response item must parse")
            .expect("complete response item");
        let end = output.completion();
        assert!(matches!(
            Output::new(output).only(),
            Some(Ok(response::Event::Head(_)))
        ));
        assert!((end) == (output::Completion::Pending));

        let body = (wire.try_get(offset.get() + used.get()..))
            .expect("GET body")
            .expect("GET body");
        let mut queries = 0;
        let (output, used) = decoder
            .try_item(&mut state, &body, &budget, || {
                queries += 1;
                None
            })
            .expect("complete response item must parse")
            .expect("complete response item");
        assert_eq!(
            queries, 0,
            "body events must not consult the request correlation ledger"
        );
        let end = output.completion();
        let data = match Output::new(output)
            .only()
            .expect("GET body must emit data")
            .expect("GET body must emit data")
        {
            response::Event::Data(data) => Some(data),
            _ => None,
        }
        .expect("GET body must emit data");
        assert_eq!(data.as_slice(), b"ok");
        assert_eq!(used.get(), 2);
        assert!((end) == (output::Completion::Complete));
    })
    .expect("resident test domain must admit its budget");
}

#[test]
fn rejects_unsolicited_head() {
    resident::Domain::try_scope(64, |budget| {
        let codec = Codec::default();
        let decoder = Decoder::try_new(&codec).expect("HTTP codec staging");
        let mut state = codec::Stateful::state(&codec, budget);
        let wire = (Shared::try_copy(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
            .expect("response fixture storage");
        let (output, _) = decoder
            .try_item(&mut state, &wire, &budget, || None)
            .expect("complete response item must parse")
            .expect("complete response item");
        let error = (Output::new(output)
            .only()
            .expect("unregistered response must emit an error"))
        .expect_err("unregistered response must fail");
        assert!(matches!(error, protocol::Error::UnsolicitedResponse));
    })
    .expect("resident test domain must admit its budget");
}

#[test]
fn rejects_switch() {
    resident::Domain::try_scope(64, |budget| {
        let codec = Codec::default();
        let decoder = Decoder::try_new(&codec).expect("HTTP codec staging");
        let mut state = codec::Stateful::state(&codec, budget);
        let wire = (Shared::try_copy(b"HTTP/1.1 101 Switching Protocols\r\n\r\n"))
            .expect("response fixture storage");
        let (output, _) = decoder
            .try_item(&mut state, &wire, &budget, || Some(Method::Other))
            .expect("complete response item must parse")
            .expect("complete response item");
        let error = (Output::new(output)
            .only()
            .expect("unsolicited switch must emit an error"))
        .expect_err("unsolicited switch must fail");
        assert!(matches!(
            error,
            protocol::Error::Transition(Error::UnexpectedSwitch)
        ));
    })
    .expect("resident test domain must admit its budget");
}
