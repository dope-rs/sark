use sark_h2::stream::{IdGen, TransitionError};
use sark_h2::{Side, Stream, StreamId, stream};

#[test]
fn stream_id_classification() {
    assert!(StreamId(0).is_zero());
    assert!(!StreamId(0).is_client());
    assert!(!StreamId(0).is_server());

    assert!(StreamId(1).is_client());
    assert!(!StreamId(1).is_server());
    assert!(!StreamId(1).is_zero());

    assert!(StreamId(2).is_server());
    assert!(!StreamId(2).is_client());

    assert!(StreamId(3).is_client());
    assert!(StreamId(4).is_server());
}

#[test]
fn stream_id_const_connection() {
    assert_eq!(StreamId::CONNECTION, StreamId(0));
    assert!(StreamId::CONNECTION.is_zero());
}

#[test]
fn id_gen_client_sequence() {
    let mut g = IdGen::new(1);
    assert_eq!(g.peek(), StreamId(1));
    assert_eq!(g.next_id(), Some(StreamId(1)));
    assert_eq!(g.next_id(), Some(StreamId(3)));
    assert_eq!(g.next_id(), Some(StreamId(5)));
    assert_eq!(g.peek(), StreamId(7));
}

#[test]
fn id_gen_server_sequence() {
    let mut g = IdGen::new(2);
    assert_eq!(g.next_id(), Some(StreamId(2)));
    assert_eq!(g.next_id(), Some(StreamId(4)));
    assert_eq!(g.next_id(), Some(StreamId(6)));
}

#[test]
fn id_gen_exhaustion_client() {
    let mut g = IdGen::new(StreamId::MAX);
    assert_eq!(g.next_id(), Some(StreamId(StreamId::MAX)));
    assert_eq!(g.next_id(), None);
    assert_eq!(g.next_id(), None);
}

#[test]
fn id_gen_exhaustion_server() {
    let mut g = IdGen::new(StreamId::MAX - 1);
    assert_eq!(g.next_id(), Some(StreamId(StreamId::MAX - 1)));
    assert_eq!(g.next_id(), None);
}

#[test]
fn idle_send_headers_to_open() {
    let s = stream::State::Idle;
    assert_eq!(
        s.send(stream::Event::Headers { end_stream: false }),
        Ok(stream::State::Open)
    );
}

#[test]
fn idle_send_headers_end_stream_to_half_closed_local() {
    let s = stream::State::Idle;
    assert_eq!(
        s.send(stream::Event::Headers { end_stream: true }),
        Ok(stream::State::HalfClosedLocal)
    );
}

#[test]
fn idle_recv_headers_to_open() {
    let s = stream::State::Idle;
    assert_eq!(
        s.recv(stream::Event::Headers { end_stream: false }),
        Ok(stream::State::Open)
    );
}

#[test]
fn idle_recv_headers_end_stream_to_half_closed_remote() {
    let s = stream::State::Idle;
    assert_eq!(
        s.recv(stream::Event::Headers { end_stream: true }),
        Ok(stream::State::HalfClosedRemote)
    );
}

#[test]
fn idle_send_data_protocol_error() {
    let s = stream::State::Idle;
    assert_eq!(
        s.send(stream::Event::Data { end_stream: false }),
        Err(TransitionError::Protocol)
    );
}

#[test]
fn idle_recv_data_protocol_error() {
    let s = stream::State::Idle;
    assert_eq!(
        s.recv(stream::Event::Data { end_stream: false }),
        Err(TransitionError::Protocol)
    );
}

#[test]
fn open_send_data_end_stream_to_half_closed_local() {
    let s = stream::State::Open;
    assert_eq!(
        s.send(stream::Event::Data { end_stream: true }),
        Ok(stream::State::HalfClosedLocal)
    );
}

#[test]
fn open_recv_data_end_stream_to_half_closed_remote() {
    let s = stream::State::Open;
    assert_eq!(
        s.recv(stream::Event::Data { end_stream: true }),
        Ok(stream::State::HalfClosedRemote)
    );
}

#[test]
fn open_send_headers_end_stream_to_half_closed_local() {
    let s = stream::State::Open;
    assert_eq!(
        s.send(stream::Event::Headers { end_stream: true }),
        Ok(stream::State::HalfClosedLocal)
    );
}

#[test]
fn open_recv_headers_end_stream_to_half_closed_remote() {
    let s = stream::State::Open;
    assert_eq!(
        s.recv(stream::Event::Headers { end_stream: true }),
        Ok(stream::State::HalfClosedRemote)
    );
}

#[test]
fn open_send_data_no_end_remains_open() {
    let s = stream::State::Open;
    assert_eq!(
        s.send(stream::Event::Data { end_stream: false }),
        Ok(stream::State::Open)
    );
}

#[test]
fn open_recv_data_no_end_remains_open() {
    let s = stream::State::Open;
    assert_eq!(
        s.recv(stream::Event::Data { end_stream: false }),
        Ok(stream::State::Open)
    );
}

#[test]
fn half_closed_local_recv_data_end_to_closed() {
    let s = stream::State::HalfClosedLocal;
    assert_eq!(
        s.recv(stream::Event::Data { end_stream: true }),
        Ok(stream::State::Closed)
    );
}

#[test]
fn half_closed_local_recv_headers_end_to_closed() {
    let s = stream::State::HalfClosedLocal;
    assert_eq!(
        s.recv(stream::Event::Headers { end_stream: true }),
        Ok(stream::State::Closed)
    );
}

#[test]
fn half_closed_remote_send_data_end_to_closed() {
    let s = stream::State::HalfClosedRemote;
    assert_eq!(
        s.send(stream::Event::Data { end_stream: true }),
        Ok(stream::State::Closed)
    );
}

#[test]
fn half_closed_remote_send_headers_end_to_closed() {
    let s = stream::State::HalfClosedRemote;
    assert_eq!(
        s.send(stream::Event::Headers { end_stream: true }),
        Ok(stream::State::Closed)
    );
}

#[test]
fn half_closed_local_send_data_stream_closed_err() {
    let s = stream::State::HalfClosedLocal;
    assert_eq!(
        s.send(stream::Event::Data { end_stream: false }),
        Err(TransitionError::StreamClosed)
    );
}

#[test]
fn half_closed_remote_recv_data_stream_closed_err() {
    let s = stream::State::HalfClosedRemote;
    assert_eq!(
        s.recv(stream::Event::Data { end_stream: false }),
        Err(TransitionError::StreamClosed)
    );
}

#[test]
fn reserved_local_send_headers_to_half_closed_remote() {
    let s = stream::State::ReservedLocal;
    assert_eq!(
        s.send(stream::Event::Headers { end_stream: false }),
        Ok(stream::State::HalfClosedRemote)
    );
}

#[test]
fn reserved_local_send_headers_end_to_closed() {
    let s = stream::State::ReservedLocal;
    assert_eq!(
        s.send(stream::Event::Headers { end_stream: true }),
        Ok(stream::State::Closed)
    );
}

#[test]
fn reserved_remote_recv_headers_to_half_closed_local() {
    let s = stream::State::ReservedRemote;
    assert_eq!(
        s.recv(stream::Event::Headers { end_stream: false }),
        Ok(stream::State::HalfClosedLocal)
    );
}

#[test]
fn reserved_remote_recv_headers_end_to_closed() {
    let s = stream::State::ReservedRemote;
    assert_eq!(
        s.recv(stream::Event::Headers { end_stream: true }),
        Ok(stream::State::Closed)
    );
}

#[test]
fn reserved_local_recv_headers_protocol_error() {
    let s = stream::State::ReservedLocal;
    assert_eq!(
        s.recv(stream::Event::Headers { end_stream: false }),
        Err(TransitionError::Protocol)
    );
}

#[test]
fn reserved_remote_send_headers_protocol_error() {
    let s = stream::State::ReservedRemote;
    assert_eq!(
        s.send(stream::Event::Headers { end_stream: false }),
        Err(TransitionError::Protocol)
    );
}

#[test]
fn closed_send_headers_stream_closed_err() {
    let s = stream::State::Closed;
    assert_eq!(
        s.send(stream::Event::Headers { end_stream: false }),
        Err(TransitionError::StreamClosed)
    );
}

#[test]
fn closed_recv_headers_stream_closed_err() {
    let s = stream::State::Closed;
    assert_eq!(
        s.recv(stream::Event::Headers { end_stream: false }),
        Err(TransitionError::StreamClosed)
    );
}

#[test]
fn rst_stream_from_any_state_closes() {
    for s in [
        stream::State::Idle,
        stream::State::ReservedLocal,
        stream::State::ReservedRemote,
        stream::State::Open,
        stream::State::HalfClosedLocal,
        stream::State::HalfClosedRemote,
        stream::State::Closed,
    ] {
        assert_eq!(s.send(stream::Event::RstStream), Ok(stream::State::Closed));
        assert_eq!(s.recv(stream::Event::RstStream), Ok(stream::State::Closed));
    }
}

#[test]
fn step_dispatches_by_side() {
    let s = stream::State::Idle;
    assert_eq!(
        s.step(stream::Event::Headers { end_stream: true }, Side::Local),
        Ok(stream::State::HalfClosedLocal)
    );
    assert_eq!(
        s.step(stream::Event::Headers { end_stream: true }, Side::Remote),
        Ok(stream::State::HalfClosedRemote)
    );
}

#[test]
fn stream_struct_send_recv_mutates() {
    let mut s = Stream::new(StreamId(1));
    assert_eq!(s.state, stream::State::Idle);

    let next = s
        .send(stream::Event::Headers { end_stream: false })
        .unwrap();
    assert_eq!(next, stream::State::Open);
    assert_eq!(s.state, stream::State::Open);

    let next = s.recv(stream::Event::Data { end_stream: true }).unwrap();
    assert_eq!(next, stream::State::HalfClosedRemote);
    assert_eq!(s.state, stream::State::HalfClosedRemote);

    let next = s.send(stream::Event::Data { end_stream: true }).unwrap();
    assert_eq!(next, stream::State::Closed);
    assert_eq!(s.state, stream::State::Closed);
}

#[test]
fn stream_reserve_constructors() {
    let s = Stream::reserve_local(StreamId(2));
    assert_eq!(s.state, stream::State::ReservedLocal);

    let s = Stream::reserve_remote(StreamId(2));
    assert_eq!(s.state, stream::State::ReservedRemote);
}

#[test]
fn push_promise_from_open_keeps_open() {
    let s = stream::State::Open;
    assert_eq!(s.send(stream::Event::PushPromise), Ok(stream::State::Open));
    assert_eq!(s.recv(stream::Event::PushPromise), Ok(stream::State::Open));
}

#[test]
fn push_promise_from_idle_protocol_error() {
    let s = stream::State::Idle;
    assert_eq!(
        s.send(stream::Event::PushPromise),
        Err(TransitionError::Protocol)
    );
    assert_eq!(
        s.recv(stream::Event::PushPromise),
        Err(TransitionError::Protocol)
    );
}
