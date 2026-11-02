use crate::common::conn::{Connection as _, Events};
pub mod common;

use dope_quic::{errors::Code, wire::stream::Id};
use o3::depot::{capacity, queue::ring};
use sark_core::manifold::stream::Boundary;
use sark_h3::conn::{Connection, Discard, event, io::Receive};
use sark_http::field::metadata::packed;

#[test]
fn discards_wrapped_events() {
    use event::{Event, Token};

    let mut events =
        ring::Fifo::<Event<'_, (), packed::Decoded, Token>>::try_new(capacity::Count::new(8))
            .expect("valid event queue");
    let target = Token::new(0).expect("bounded stream");
    let other = Token::new(4).expect("bounded stream");
    for _ in 0..6 {
        assert!(events.push_back(Event::GoAway { id: 0 }).is_ok());
        events.pop_front().expect("queued event");
    }
    let (mut conn, ..) = Connection::try_request(Id::ZERO).expect("request fixture");
    let [(first, bytes), (last, pushed)] = [
        (b"\0\x03abc".as_slice(), Boundary::More),
        (b"\0\x02de".as_slice(), Boundary::End),
    ]
    .map(|(wire, boundary)| {
        Receive::new(&mut conn)
            .borrowed(Id::ZERO, wire, boundary)
            .expect("DATA fixture");
        match Events::new(&mut conn).poll_token().expect("DATA event") {
            Some(Event::Data { message, data }) => (message, data),
            _ => panic!("DATA event"),
        }
    });
    for event in [
        Event::Finished { stream_id: other },
        Event::Data {
            message: first,
            data: bytes,
        },
        Event::GoAway { id: 17 },
        Event::PushData {
            message: last,
            push_id: 2,
            data: pushed,
        },
        Event::Reset {
            stream_id: other,
            error_code: Code::from_u16(23),
            origin: event::Origin::Transport,
            released: 11,
        },
        Event::Reset {
            stream_id: other,
            error_code: Code::from_u16(29),
            origin: event::Origin::Transport,
            released: 7,
        }
        .map_identity(|_| Ok::<_, std::convert::Infallible>(target))
        .expect("project reset credit"),
        Event::Finished { stream_id: target },
    ] {
        assert!(events.push_back(event).is_ok());
    }
    assert!(matches!(
        Discard::new(&mut events).try_for(target),
        Ok((5, 7))
    ));
    assert!(matches!(
        Discard::new(&mut events).try_for(target),
        Ok((0, 0))
    ));
    assert!(
        matches!(events.pop_front(), Some(Event::Finished { stream_id }) if stream_id == other)
    );
    assert!(matches!(events.pop_front(), Some(Event::GoAway { id: 17 })));
    assert!(
        matches!(events.pop_front(), Some(Event::Reset { stream_id, error_code, released: 11, .. }) if stream_id == other && error_code == Code::from_u16(23))
    );
    assert!(events.is_empty());
}
