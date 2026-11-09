use sark_core::manifold::stream::Boundary;
use sark_h2::{
    stream,
    stream::{Error, Id, Lifecycle},
};

#[test]
fn stream_id_classification() {
    assert!(Id::new(0).expect("fixture domain").is_zero());
    assert!(!Id::new(0).expect("fixture domain").is_client());
    assert!(!Id::new(0).expect("fixture domain").is_server());

    assert!(Id::new(1).expect("fixture domain").is_client());
    assert!(!Id::new(1).expect("fixture domain").is_server());
    assert!(!Id::new(1).expect("fixture domain").is_zero());

    assert!(Id::new(2).expect("fixture domain").is_server());
    assert!(!Id::new(2).expect("fixture domain").is_client());

    assert!(Id::new(3).expect("fixture domain").is_client());
    assert!(Id::new(4).expect("fixture domain").is_server());
}

#[test]
fn connection_id() {
    assert!((Id::CONNECTION) == (Id::new(0).expect("fixture domain")));
    assert!(Id::CONNECTION.is_zero());
}

#[test]
fn idle_send_headers() {
    let s = stream::State::Idle;
    assert!(
        (s.send(stream::Event::Headers {
            boundary: Boundary::More
        })) == (Ok(stream::State::Open))
    );
}

#[test]
fn idle_send_end() {
    let s = stream::State::Idle;
    assert!(
        (s.send(stream::Event::Headers {
            boundary: Boundary::End
        })) == (Ok(stream::State::HalfClosedLocal))
    );
}

#[test]
fn idle_recv_headers() {
    let s = stream::State::Idle;
    assert!(
        (s.recv(stream::Event::Headers {
            boundary: Boundary::More
        })) == (Ok(stream::State::Open))
    );
}

#[test]
fn idle_recv_end() {
    let s = stream::State::Idle;
    assert!(
        (s.recv(stream::Event::Headers {
            boundary: Boundary::End
        })) == (Ok(stream::State::HalfClosedRemote))
    );
}

#[test]
fn idle_send_data() {
    let s = stream::State::Idle;
    let event = stream::Event::Data {
        boundary: Boundary::More,
    };
    assert!(
        s.send(event)
            == Err(Error::Protocol {
                state: s,
                event,
                side: stream::Side::Local,
            })
    );
}

#[test]
fn idle_recv_data() {
    let s = stream::State::Idle;
    let event = stream::Event::Data {
        boundary: Boundary::More,
    };
    assert!(
        s.recv(event)
            == Err(Error::Protocol {
                state: s,
                event,
                side: stream::Side::Remote,
            })
    );
}

#[test]
fn open_send_data_end() {
    let s = stream::State::Open;
    assert!(
        (s.send(stream::Event::Data {
            boundary: Boundary::End
        })) == (Ok(stream::State::HalfClosedLocal))
    );
}

#[test]
fn open_recv_data_end() {
    let s = stream::State::Open;
    assert!(
        (s.recv(stream::Event::Data {
            boundary: Boundary::End
        })) == (Ok(stream::State::HalfClosedRemote))
    );
}

#[test]
fn open_send_headers_end() {
    let s = stream::State::Open;
    assert!(
        (s.send(stream::Event::Headers {
            boundary: Boundary::End
        })) == (Ok(stream::State::HalfClosedLocal))
    );
}

#[test]
fn open_recv_headers_end() {
    let s = stream::State::Open;
    assert!(
        (s.recv(stream::Event::Headers {
            boundary: Boundary::End
        })) == (Ok(stream::State::HalfClosedRemote))
    );
}

#[test]
fn open_send_data() {
    let s = stream::State::Open;
    assert!(
        (s.send(stream::Event::Data {
            boundary: Boundary::More
        })) == (Ok(stream::State::Open))
    );
}

#[test]
fn open_recv_data() {
    let s = stream::State::Open;
    assert!(
        (s.recv(stream::Event::Data {
            boundary: Boundary::More
        })) == (Ok(stream::State::Open))
    );
}

#[test]
fn local_recv_data_end() {
    let s = stream::State::HalfClosedLocal;
    assert!(
        (s.recv(stream::Event::Data {
            boundary: Boundary::End
        })) == (Ok(stream::State::Closed))
    );
}

#[test]
fn local_recv_headers_end() {
    let s = stream::State::HalfClosedLocal;
    assert!(
        (s.recv(stream::Event::Headers {
            boundary: Boundary::End
        })) == (Ok(stream::State::Closed))
    );
}

#[test]
fn remote_send_data_end() {
    let s = stream::State::HalfClosedRemote;
    assert!(
        (s.send(stream::Event::Data {
            boundary: Boundary::End
        })) == (Ok(stream::State::Closed))
    );
}

#[test]
fn remote_send_headers_end() {
    let s = stream::State::HalfClosedRemote;
    assert!(
        (s.send(stream::Event::Headers {
            boundary: Boundary::End
        })) == (Ok(stream::State::Closed))
    );
}

#[test]
fn local_send_data_closed() {
    let s = stream::State::HalfClosedLocal;
    let event = stream::Event::Data {
        boundary: Boundary::More,
    };
    assert!(
        s.send(event)
            == Err(Error::StreamClosed {
                state: s,
                event,
                side: stream::Side::Local,
            })
    );
}

#[test]
fn remote_recv_data_closed() {
    let s = stream::State::HalfClosedRemote;
    let event = stream::Event::Data {
        boundary: Boundary::More,
    };
    assert!(
        s.recv(event)
            == Err(Error::StreamClosed {
                state: s,
                event,
                side: stream::Side::Remote,
            })
    );
}

#[test]
fn reserved_local_send() {
    let s = stream::State::ReservedLocal;
    assert!(
        (s.send(stream::Event::Headers {
            boundary: Boundary::More
        })) == (Ok(stream::State::HalfClosedRemote))
    );
}

#[test]
fn reserved_local_end() {
    let s = stream::State::ReservedLocal;
    assert!(
        (s.send(stream::Event::Headers {
            boundary: Boundary::End
        })) == (Ok(stream::State::Closed))
    );
}

#[test]
fn reserved_remote_recv() {
    let s = stream::State::ReservedRemote;
    assert!(
        (s.recv(stream::Event::Headers {
            boundary: Boundary::More
        })) == (Ok(stream::State::HalfClosedLocal))
    );
}

#[test]
fn reserved_remote_end() {
    let s = stream::State::ReservedRemote;
    assert!(
        (s.recv(stream::Event::Headers {
            boundary: Boundary::End
        })) == (Ok(stream::State::Closed))
    );
}

#[test]
fn reserved_local_recv() {
    let s = stream::State::ReservedLocal;
    let event = stream::Event::Headers {
        boundary: Boundary::More,
    };
    assert!(
        s.recv(event)
            == Err(Error::Protocol {
                state: s,
                event,
                side: stream::Side::Remote,
            })
    );
}

#[test]
fn reserved_remote_send() {
    let s = stream::State::ReservedRemote;
    let event = stream::Event::Headers {
        boundary: Boundary::More,
    };
    assert!(
        s.send(event)
            == Err(Error::Protocol {
                state: s,
                event,
                side: stream::Side::Local,
            })
    );
}

#[test]
fn closed_send_headers() {
    let s = stream::State::Closed;
    let event = stream::Event::Headers {
        boundary: Boundary::More,
    };
    assert!(
        s.send(event)
            == Err(Error::StreamClosed {
                state: s,
                event,
                side: stream::Side::Local,
            })
    );
}

#[test]
fn closed_recv_headers() {
    let s = stream::State::Closed;
    let event = stream::Event::Headers {
        boundary: Boundary::More,
    };
    assert!(
        s.recv(event)
            == Err(Error::StreamClosed {
                state: s,
                event,
                side: stream::Side::Remote,
            })
    );
}

#[test]
fn reserved_lifecycle() {
    let s = Lifecycle::reserved();
    assert!((s.state) == (stream::State::ReservedRemote));
}
