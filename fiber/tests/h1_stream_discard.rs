pub mod common;

use std::{
    net::Shutdown,
    thread,
    time::Duration,
};

use common::{
    route::{
        body::{__SarkHandlerPingX70696E67, discard::*},
        chunked::peer::Peer,
    },
    server::Plan,
};
use dope_test::harness::peer::Peer as TcpPeer;
use sark_core::{body::admit::Retention, route::Schema};

#[test]
fn discard_policy_is_static() {
    assert_eq!(
        <<__SarkHandlerPingX70696E67 as sark_core::route::Spec>::Request as Schema>::BODY_RETENTION,
        Retention::Discarded,
    );
    assert_eq!(UpReq::BODY_RETENTION, Retention::Discarded);
}

#[test]
fn large_body_reuses_conn() {
    Plan::serve::<StreamDiscardDispatch>(|bind| {
        let total: usize = 8 * 1024 * 1024;
        let socket = TcpPeer::at(bind)
            .try_read(Duration::from_secs(5))
            .expect("response peer");
        let mut peer = Peer::new(socket);
        let wire = UpReq::wire(total);
        peer.socket
            .write_all(&wire[..18])
            .expect("write split upload head");
        thread::sleep(Duration::from_millis(20));
        peer.socket
            .write_all(&wire[18..])
            .expect("write upload head and body");
        let (_, resp) = peer.try_response().expect("upload response");
        assert_eq!(
            resp.as_slice(),
            total.to_string().as_bytes(),
            "upload response: {resp:?}"
        );
        let (_, pong) = peer.try_exchange(PING).expect("ping response");
        assert_eq!(pong, b"pong", "reuse after drain");
    });
}

#[test]
fn body_tail_pipelines() {
    Plan::serve::<StreamDiscardDispatch>(|bind| {
        let total: usize = 256 * 1024;
        let socket = TcpPeer::at(bind)
            .try_read(Duration::from_secs(5))
            .expect("response peer");
        let mut peer = Peer::new(socket);
        let prefix = vec![0x11u8; 1000];
        let mut first = UpReq::head(total).into_bytes();
        first.extend_from_slice(&prefix);
        peer.socket
            .write_all(&first)
            .expect("write upload head and body prefix");
        let (_, resp) = peer.try_response().expect("upload response");
        assert_eq!(resp.as_slice(), total.to_string().as_bytes(), "{resp:?}");
        let mut second = vec![0x22u8; total - prefix.len()];
        second.extend_from_slice(PING);
        peer.socket
            .write_all(&second)
            .expect("write body tail and pipelined ping");
        let (_, resp) = peer.try_response().expect("pipelined ping");
        assert_eq!(
            resp.as_slice(),
            b"pong",
            "pipelined request after drain boundary: {resp:?}"
        );
    });
}

#[test]
fn small_body_is_inline() {
    Plan::serve::<StreamDiscardDispatch>(|bind| {
        let socket = TcpPeer::at(bind)
            .try_read(Duration::from_secs(5))
            .expect("response peer");
        let mut peer = Peer::new(socket);
        let body = b"tiny";
        let req = UpReq::head(body.len());
        peer.socket
            .write_all(req.as_bytes())
            .expect("write small upload head");
        peer.socket
            .write_all(body)
            .expect("write small upload body");
        let (_, resp) = peer.try_response().expect("small upload response");
        assert_eq!(
            resp.as_slice(),
            body.len().to_string().as_bytes(),
            "{resp:?}"
        );
        let (_, pong) = peer.try_exchange(PING).expect("ping response");
        assert_eq!(pong, b"pong");
    });
}

#[test]
fn deep_pipeline_drains() {
    Plan::serve::<StreamDiscardDispatch>(|bind| {
        const DEPTH: usize = 8;
        const BODY_LEN: usize = 1024 * 1024;

        let mut peer = Peer::new(TcpPeer::at(bind).try_connect().expect("connect"));
        peer.socket
            .write_wait(Some(Duration::from_secs(5)))
            .expect("set pipeline write deadline");
        let body = [0xa5; 64 * 1024];
        let head = UpReq::head(BODY_LEN);
        for _ in 0..DEPTH {
            peer.socket
                .write_all(head.as_bytes())
                .expect("write pipelined upload head");
            for _ in 0..BODY_LEN / body.len() {
                peer.socket
                    .write_all(&body)
                    .expect("write pipelined upload body");
            }
        }

        peer.socket
            .read_wait(Some(Duration::from_secs(5)))
            .expect("set pipeline read deadline");
        let digits = BODY_LEN.to_string();
        for _ in 0..DEPTH {
            let (status, body) = peer.try_response().expect("read pipelined response");
            assert_eq!(status, 200);
            assert_eq!(body, digits.as_bytes());
        }
    });
}

#[test]
fn peer_close_stays_live() {
    Plan::serve::<StreamDiscardDispatch>(|bind| {
        let total: usize = 4 * 1024 * 1024;
        let socket = TcpPeer::at(bind)
            .try_read(Duration::from_secs(5))
            .expect("response peer");
        let mut peer = Peer::new(socket);
        let (_, resp) = peer
            .try_exchange(UpReq::head(total).as_bytes())
            .expect("upload response");
        drop(peer.buffered);
        assert_eq!(resp.as_slice(), total.to_string().as_bytes(), "{resp:?}");
        peer.socket
            .write_all(&vec![0u8; 128 * 1024])
            .expect("write partial upload body");
        peer.socket
            .shutdown(Shutdown::Both)
            .expect("close partial upload peer");
        let socket = TcpPeer::at(bind)
            .try_read(Duration::from_secs(5))
            .expect("response peer");
        let mut fresh = Peer::new(socket);
        let (_, pong) = fresh.try_exchange(PING).expect("fresh ping response");
        assert_eq!(pong, b"pong", "server wedged after mid-body FIN");
    });
}
