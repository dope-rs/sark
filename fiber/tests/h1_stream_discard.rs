pub mod common;

use std::{
    net::{Shutdown, SocketAddr},
    thread,
    time::Duration,
};

use common::route::{body::discard::*, chunked::peer::Peer};
use common::server::recover;

use dope_test::harness::{peer::Peer as TcpPeer, server::Pair};
use sark_core::{body::admit::Retention, route::Schema};

#[test]
fn discard_policy_is_static() {
    assert_eq!(
        <<__SarkHandlerPingX70696E67 as sark_core::route::Spec>::Request as Schema>::BODY_RETENTION,
        Retention::Discarded,
    );
    assert_eq!(UpReq::BODY_RETENTION, Retention::Discarded);
}

fn serve(client: impl FnOnce(SocketAddr)) {
    Pair::bind()
        .expect("bind test harness")
        .check(
            |context, listener| {
                try_serve(context, listener, recover, |result| {
                    result.expect("server execution")
                })
            },
            client,
            |result| {
                let reports = result.expect("discard server shutdown");
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "unexpected reports server operations: {reports:?}"
                );
            },
        )
        .expect("run test server");
}

#[test]
fn large_body_reuses_conn() {
    serve(|bind| {
        let total: usize = 8 * 1024 * 1024;
        let mut sock = TcpPeer::at(bind).try_connect().expect("connect");
        sock.read_wait(Some(Duration::from_secs(5)))
            .expect("set response deadline");
        let mut buffered = Vec::new();
        let wire = UpReq::wire(total);
        sock.write_all(&wire[..18])
            .expect("write split upload head");
        thread::sleep(Duration::from_millis(20));
        sock.write_all(&wire[18..])
            .expect("write upload head and body");
        let (_, resp) = Peer::new(&mut sock, &mut buffered)
            .try_response()
            .expect("upload response");
        assert_eq!(
            resp.as_slice(),
            total.to_string().as_bytes(),
            "upload response: {resp:?}"
        );
        let (_, pong) = Peer::new(&mut sock, &mut buffered)
            .try_exchange(PING)
            .expect("ping response");
        assert_eq!(pong, b"pong", "reuse after drain");
    });
}

#[test]
fn body_tail_pipelines() {
    serve(|bind| {
        let total: usize = 256 * 1024;
        let mut sock = TcpPeer::at(bind).try_connect().expect("connect");
        sock.read_wait(Some(Duration::from_secs(5)))
            .expect("set response deadline");
        let mut buffered = Vec::new();
        let prefix = vec![0x11u8; 1000];
        let mut first = UpReq::head(total).into_bytes();
        first.extend_from_slice(&prefix);
        sock.write_all(&first)
            .expect("write upload head and body prefix");
        let (_, resp) = Peer::new(&mut sock, &mut buffered)
            .try_response()
            .expect("upload response");
        assert_eq!(resp.as_slice(), total.to_string().as_bytes(), "{resp:?}");
        let mut second = vec![0x22u8; total - prefix.len()];
        second.extend_from_slice(PING);
        sock.write_all(&second)
            .expect("write body tail and pipelined ping");
        let (_, resp) = Peer::new(&mut sock, &mut buffered)
            .try_response()
            .expect("pipelined ping");
        assert_eq!(
            resp.as_slice(),
            b"pong",
            "pipelined request after drain boundary: {resp:?}"
        );
    });
}

#[test]
fn small_body_is_inline() {
    serve(|bind| {
        let mut sock = TcpPeer::at(bind).try_connect().expect("connect");
        sock.read_wait(Some(Duration::from_secs(5)))
            .expect("set response deadline");
        let mut buffered = Vec::new();
        let body = b"tiny";
        let req = UpReq::head(body.len());
        sock.write_all(req.as_bytes())
            .expect("write small upload head");
        sock.write_all(body).expect("write small upload body");
        let (_, resp) = Peer::new(&mut sock, &mut buffered)
            .try_response()
            .expect("small upload response");
        assert_eq!(
            resp.as_slice(),
            body.len().to_string().as_bytes(),
            "{resp:?}"
        );
        let (_, pong) = Peer::new(&mut sock, &mut buffered)
            .try_exchange(PING)
            .expect("ping response");
        assert_eq!(pong, b"pong");
    });
}

#[test]
fn deep_pipeline_drains() {
    serve(|bind| {
        const DEPTH: usize = 8;
        const BODY_LEN: usize = 1024 * 1024;

        let mut sock = TcpPeer::at(bind).try_connect().expect("connect");
        sock.write_wait(Some(Duration::from_secs(5)))
            .expect("set pipeline write deadline");
        let body = [0xa5; 64 * 1024];
        let head = UpReq::head(BODY_LEN);
        for _ in 0..DEPTH {
            sock.write_all(head.as_bytes())
                .expect("write pipelined upload head");
            for _ in 0..BODY_LEN / body.len() {
                sock.write_all(&body).expect("write pipelined upload body");
            }
        }

        sock.read_wait(Some(Duration::from_secs(5)))
            .expect("set pipeline read deadline");
        let mut buffered = Vec::new();
        let digits = BODY_LEN.to_string();
        for _ in 0..DEPTH {
            let (status, body) = Peer::new(&mut sock, &mut buffered)
                .try_response()
                .expect("read pipelined response");
            assert_eq!(status, 200);
            assert_eq!(body, digits.as_bytes());
        }
    });
}

#[test]
fn peer_close_stays_live() {
    serve(|bind| {
        let total: usize = 4 * 1024 * 1024;
        let mut sock = TcpPeer::at(bind).try_connect().expect("connect");
        sock.read_wait(Some(Duration::from_secs(5)))
            .expect("set response deadline");
        let (_, resp) = Peer::new(&mut sock, &mut Vec::new())
            .try_exchange(UpReq::head(total).as_bytes())
            .expect("upload response");
        assert_eq!(resp.as_slice(), total.to_string().as_bytes(), "{resp:?}");
        sock.write_all(&vec![0u8; 128 * 1024])
            .expect("write partial upload body");
        sock.shutdown(Shutdown::Both)
            .expect("close partial upload peer");
        let mut fresh = TcpPeer::at(bind).try_connect().expect("connect fresh peer");
        fresh
            .read_wait(Some(Duration::from_secs(5)))
            .expect("set response deadline");
        let (_, pong) = Peer::new(&mut fresh, &mut Vec::new())
            .try_exchange(PING)
            .expect("fresh ping response");
        assert_eq!(pong, b"pong", "server wedged after mid-body FIN");
    });
}
