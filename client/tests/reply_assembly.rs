pub mod common;

use common::{codec::Scope as _, reply::events};
use o3::buffer::{resident, segments};
use sark_client::{
    Error,
    protocol::{self, response::Event, sequence},
    reply::assembly::Assembly,
};

#[test]
fn head_proof() {
    resident::Domain::try_scope(64 * 1024, |budget| {
        let pool = segments::Pool::try_new(segments::Plan {
            segments: 1,
            chains: 0,
        })
        .expect("body pool");
        let [info, head, data, trailers] = events(budget).expect("response events");
        let waiting = Assembly::new(&pool);
        assert!(matches!(
            waiting.missing(),
            Error::Protocol(protocol::Error::Sequence(error))
                if error == sequence::Error::new(sequence::Phase::Head, sequence::Event::End)
        ));
        let waiting = waiting
            .accept(info)
            .expect("informational head")
            .continue_value()
            .expect("wait for final head");
        let mut body = waiting
            .accept(head)
            .expect("final head")
            .break_value()
            .expect("body admission");
        let pointer = match &data {
            Event::Data(data) => Some(data.as_slice().as_ptr()),
            _ => None,
        }
        .expect("data event");
        body.accept(data).expect("body data");
        let [info, head, data, early] = events(budget).expect("reordered events");
        for (event, kind) in [
            (data, sequence::Event::Data),
            (early, sequence::Event::Trailers),
        ] {
            let error = Assembly::new(&pool)
                .accept(event)
                .err()
                .expect("event before head");
            assert!(matches!(
                error,
                Error::Protocol(protocol::Error::Sequence(error))
                    if error == sequence::Error::new(sequence::Phase::Head, kind)
            ));
        }
        for (event, kind) in [
            (info, sequence::Event::Informational),
            (head, sequence::Event::Head),
        ] {
            let error = body.accept(event).expect_err("head after body admission");
            assert!(matches!(
                error,
                Error::Protocol(protocol::Error::Sequence(error))
                    if error == sequence::Error::new(sequence::Phase::Body, kind)
            ));
        }
        body.accept(trailers).expect("trailers");
        let [info, head, data, trailers] = events(budget).expect("events after trailers");
        for (event, kind) in [
            (info, sequence::Event::Informational),
            (head, sequence::Event::Head),
            (data, sequence::Event::Data),
            (trailers, sequence::Event::Trailers),
        ] {
            let error = body.accept(event).expect_err("event after trailers");
            assert!(matches!(
                error,
                Error::Protocol(protocol::Error::Sequence(error))
                    if error == sequence::Error::new(sequence::Phase::Trailers, kind)
            ));
        }
        let response = body.finish();
        assert_eq!(
            response.trailers().and_then(|fields| fields.get("X-Tail")),
            Some(b"kept".as_slice())
        );
        let body = response.into_body();
        let data = body.contiguous().expect("one body segment");
        assert_eq!(data.as_ptr(), pointer);
        assert_eq!(data, b"x");
    })
    .expect("resident domain");
}
