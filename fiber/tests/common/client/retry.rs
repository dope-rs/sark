use std::{net::SocketAddr, num::NonZero};

use dope::core::driver::route::KeyTag;
use dope_fiber::{BlockOn, task::time::budget};
use dope_test::harness::{GUARD, peer::TcpAcceptor};
use sark::client::protocol::retry::Policy;
use sark_client::{Error, Failure, Safety, conn, protocol, timeout};
use sark_fiber::client::{Stream, config::Config};
use sark_http::{
    h1::{field::syntax, head},
    method::Method,
};

use crate::common::client::runtime::{
    cases::{Flavor, request::Request},
    peer,
};

pub fn effect(flavor: Flavor, address: SocketAddr, config: Config) -> Request<'static> {
    Request::new(flavor, address, config, Method::POST, "/effect", &[], b"effect")
}

pub enum Post<'a> {
    Disconnect,
    Timeout,
    Reply(&'a [u8]),
}

impl Post<'_> {
    pub fn serve(self, acceptor: &mut TcpAcceptor<'_>) -> Result<(), peer::Error> {
        let accepted = acceptor
            .accept_for(GUARD)?
            .ok_or(dope_test::harness::peer::Error::TimedOut)?;
        accepted.read_wait(Some(GUARD))?;
        accepted.write_wait(Some(GUARD))?;
        accepted.set_nodelay(true)?;
        let mut conn = peer::Conn::new(&accepted);
        assert!(conn.try_request()?.ok_or(peer::Error::UnexpectedEof)?.is_effect());
        match self {
            Self::Disconnect => peer::try_shutdown(&accepted)?,
            Self::Timeout => {
                peer::closed(&mut conn)?;
                return peer::try_shutdown(&accepted).map_err(peer::Error::Io);
            }
            Self::Reply(wire) => conn.write_all(wire)?,
        }
        peer::closed(&mut conn)
    }
}

pub fn assert_timeout<T, E, M>(result: Result<T, Failure<E, M>>) {
    let failure = result.err().expect("POST must time out");
    assert!(failure.safety() == Safety::PossiblyCommitted);
    assert!(matches!(failure.cause(), Error::Timeout(error)
        if error.phase() == timeout::Phase::Head && error.elapsed() == budget::Elapsed::Budget));
}

pub fn assert_invalid_head<'d, B: ?Sized>(
    app: &mut impl BlockOn<'d>,
    mut response: Stream<'_, '_, '_, '_, 'd, B, KeyTag<0>>,
) {
    let failure = loop {
        if let Err(failure) = app
            .block_on(response.next_event())
            .expect("drive response event")
            .expect("invalid field must fail before completion")
        {
            break failure;
        }
    };
    assert!(failure.safety() == Safety::PossiblyCommitted);
    assert!(matches!(
        failure.cause(),
        Error::Protocol(protocol::Error::Head(head::ParseError::Field(
            syntax::Error::Name { offset: 20, actual: b' ' }
        )))
    ));
}

pub fn observe<E, M>(attempts: &mut [(u8, Safety, bool); 2]) -> impl FnMut(&Failure<E, M>) + '_ {
    let mut index = 0;
    move |failure| {
        attempts[index] = (
            failure.attempt(),
            failure.safety(),
            matches!(
                failure.cause(),
                Error::Transport(conn::Error::Classified(conn::Kind::RemoteEof))
            ),
        );
        index += 1;
    }
}

pub const AGGRESSIVE: Policy = Policy {
    idempotent_attempts: NonZero::<u8>::MIN.saturating_add(7),
    non_idempotent_attempts: NonZero::<u8>::MIN.saturating_add(7),
    reconnect_on_stale_non_idempotent: true,
};

pub const INVALID_GZIP: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 3\r\nConnection: close\r\n\r\nbad";

pub const MISSING_LOCATION: &[u8] =
    b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
