use sark_core::manifold::stream::Boundary;
use sark_h2::{
    stream,
    stream::{Error, Id, Lifecycle},
};

#[test]
fn stream_id_classification() {
    let connection = Id::new(0).expect("connection ID");
    assert_eq!(connection, Id::CONNECTION);
    assert!(connection.is_zero());
    assert!(!connection.is_client());
    assert!(!connection.is_server());
    for (raw, client) in [(1, true), (2, false), (3, true), (4, false)] {
        let id = Id::new(raw).expect("stream ID");
        assert!(!id.is_zero());
        assert_eq!((id.is_client(), id.is_server()), (client, !client), "{raw}");
    }
}

#[test]
fn stream_transitions() {
    use stream::{
        Event::{Data, Headers},
        Side::{Local, Remote},
        State::{
            Closed, HalfClosedLocal, HalfClosedRemote, Idle, Open, ReservedLocal, ReservedRemote,
        },
    };
    let headers = Headers {
        boundary: Boundary::More,
    };
    let end_headers = Headers {
        boundary: Boundary::End,
    };
    let data = Data {
        boundary: Boundary::More,
    };
    let end_data = Data {
        boundary: Boundary::End,
    };
    for (state, side, event, expected) in [
        (Idle, Local, headers, Open),
        (Idle, Local, end_headers, HalfClosedLocal),
        (Idle, Remote, headers, Open),
        (Idle, Remote, end_headers, HalfClosedRemote),
        (Open, Local, end_data, HalfClosedLocal),
        (Open, Remote, end_data, HalfClosedRemote),
        (Open, Local, end_headers, HalfClosedLocal),
        (Open, Remote, end_headers, HalfClosedRemote),
        (Open, Local, data, Open),
        (Open, Remote, data, Open),
        (HalfClosedLocal, Remote, end_data, Closed),
        (HalfClosedLocal, Remote, end_headers, Closed),
        (HalfClosedRemote, Local, end_data, Closed),
        (HalfClosedRemote, Local, end_headers, Closed),
        (ReservedLocal, Local, headers, HalfClosedRemote),
        (ReservedLocal, Local, end_headers, Closed),
        (ReservedRemote, Remote, headers, HalfClosedLocal),
        (ReservedRemote, Remote, end_headers, Closed),
    ] {
        let next = match side {
            Local => state.send(event),
            Remote => state.recv(event),
        };
        assert_eq!(next, Ok(expected), "{state:?} {side:?} {event:?}");
    }

    let protocol_errors = [
        (Idle, Local, data),
        (Idle, Remote, data),
        (ReservedLocal, Remote, headers),
        (ReservedRemote, Local, headers),
    ];
    let closed_streams = [
        (HalfClosedLocal, Local, data),
        (HalfClosedRemote, Remote, data),
        (Closed, Local, headers),
        (Closed, Remote, headers),
    ];
    for (protocol, cases) in [(true, protocol_errors), (false, closed_streams)] {
        for (state, side, event) in cases {
            let error = if protocol {
                Error::Protocol { state, side, event }
            } else {
                Error::StreamClosed { state, side, event }
            };
            let next = match side {
                Local => state.send(event),
                Remote => state.recv(event),
            };
            assert_eq!(next, Err(error));
        }
    }
}

#[test]
fn reserved_lifecycle() {
    assert_eq!(Lifecycle::reserved().state, stream::State::ReservedRemote);
}
