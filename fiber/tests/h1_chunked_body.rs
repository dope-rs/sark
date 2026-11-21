pub mod common;

use std::{
    net::{Shutdown, SocketAddr},
    time::Duration,
};

use common::{
    retained,
    route::chunked::{
        apps::ChunkedDispatch,
        peer::{Error, Peer},
        wire::{LARGE_BODY_LEN, Request, UPLOAD},
    },
    server::{MAX_CONNECTIONS, Plan},
};
use dope::core::io::socket::option::Buffer;
use dope_test::harness::{peer::Error as PeerError, server::Pair};

fn serve<R>(buffers: Option<Buffer>, client: impl FnOnce(SocketAddr) -> R) -> R {
    let pair = Pair::bind().expect("listener");
    let plan = Plan::new(pair.addr(), Duration::from_secs(10));
    let plan = match buffers {
        Some(buffers) => plan.with_buffers(buffers),
        None => plan,
    }
    .build();
    retained::run::<ChunkedDispatch, _>((pair, plan), MAX_CONNECTIONS, client)
}

#[test]
fn waits_once() -> Result<(), Error> {
    serve(None, |bind| {
        let mut fixed = Peer::try_connect(bind)?;
        fixed.socket
            .write_all(
                b"POST /public/expect HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n",
            )
            ?;
        assert_eq!(fixed.try_response()?, (100, Vec::new()));
        fixed.socket.write_all(b"body")?;
        assert_eq!(fixed.try_response()?, (200, b"body".to_vec()));

        let mut chunked = Peer::try_connect(bind)?;
        chunked.socket
            .write_all(
                b"POST /expect HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n",
            )
            ?;
        assert_eq!(chunked.try_response()?, (100, Vec::new()));
        chunked.socket.write_all(b"4\r\nbody\r\n0\r\n\r\n")?;
        assert_eq!(chunked.try_response()?, (200, b"body".to_vec()));

        let mut ready = Peer::try_connect(bind)?;
        ready.socket
            .write_all(
                b"POST /expect HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\nbody",
            )
            ?;
        assert_eq!(ready.try_response()?, (200, b"body".to_vec()));

        let mut bad = Peer::try_connect(bind)?;
        bad.socket.write_all(
            b"POST /expect HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\nExpect: fancy\r\n\r\n",
        )?;
        assert_eq!(bad.try_response()?.0, 400);
        Ok(())
    })
}

#[test]
fn closing_discard_drains() -> Result<(), Error> {
    serve(None, |bind| {
        for complete in [true, false] {
            let mut peer = Peer::try_connect(bind)?;
            peer.socket.write_wait(Some(Duration::from_secs(5)))?;
            peer.socket
                .write_all(&Request::new("/discard").close_head(UPLOAD.len()))?;
            assert_eq!(
                peer.try_response()?,
                (200, UPLOAD.len().to_string().into_bytes())
            );
            peer.socket.read_wait(Some(Duration::from_millis(25)))?;
            let waiting = peer
                .socket
                .read(&mut [0; 1])
                .expect_err("close must wait for the body");
            assert_eq!(waiting, PeerError::TimedOut);
            peer.socket.read_wait(Some(Duration::from_secs(5)))?;
            if complete {
                peer.socket.write_all(UPLOAD)?;
            } else {
                peer.socket.write_all(&UPLOAD[..8192])?;
                peer.socket.shutdown(Shutdown::Write)?;
            }
            assert!(peer.buffered.is_empty());
            assert_eq!(peer.socket.read(&mut [0; 1])?, 0);
        }
        Ok(())
    })
}

#[test]
fn drains_during_response() -> Result<(), Error> {
    let buffers = Buffer::try_from(16 * 1024_usize).expect("bounded socket buffers");
    serve(Some(buffers), |bind| {
        let mut peer = Peer::try_bounded(bind)?;
        peer.socket.write_wait(Some(Duration::from_secs(5)))?;
        peer.socket
            .write_all(&Request::new("/discard/large").close_head(LARGE_BODY_LEN))?;
        for chunk in UPLOAD[..LARGE_BODY_LEN].chunks(16 * 1024) {
            peer.socket.write_all(chunk)?;
        }
        let (status, body) = peer.try_response()?;
        assert_eq!(status, 200);
        assert_eq!(body.len(), LARGE_BODY_LEN);
        assert!(body.iter().all(|&byte| byte == b'x'));
        assert!(peer.buffered.is_empty());
        assert_eq!(peer.socket.read(&mut [0; 1])?, 0);
        Ok(())
    })
}

#[test]
fn split_keeps_pipeline() -> Result<(), Error> {
    serve(None, |bind| {
        let mut peer = Peer::try_connect(bind)?;
        let request = Request::new("/sync").chunked(&[b"abc", b"defg"], true);
        let end = request
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .expect("request head terminator")
            + 4;
        peer.socket.write_all(&request[..end])?;
        for byte in &request[end..request.len() - 1] {
            peer.socket.write_all(&[*byte])?;
        }
        let mut tail = vec![request[request.len() - 1]];
        tail.extend_from_slice(b"GET /ping HTTP/1.1\r\nHost: local\r\n\r\n");
        peer.socket.write_all(&tail)?;

        assert_eq!(peer.try_response()?, (200, b"abcdefg".to_vec()));
        assert_eq!(peer.try_response()?, (200, b"pong".to_vec()));
        Ok(())
    })
}

#[test]
fn discard_keeps_pipeline() -> Result<(), Error> {
    serve(None, |bind| {
        let mut peer = Peer::try_connect(bind)?;
        let mut request = Request::new("/discard").chunked(&[b"abc", b"defg"], true);
        request.extend_from_slice(b"GET /ping HTTP/1.1\r\nHost: local\r\n\r\n");
        peer.socket.write_all(&request)?;

        assert_eq!(peer.try_response()?, (200, b"7".to_vec()));
        assert_eq!(peer.try_response()?, (200, b"pong".to_vec()));
        Ok(())
    })
}

#[test]
fn invalid_is_atomic() -> Result<(), Error> {
    serve(None, |bind| {
        let mut bad = Peer::try_connect(bind)?;
        bad.socket.write_all(
            b"POST /discard HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabcX\n0\r\n\r\n",
        )
        ?;
        assert_eq!(bad.try_response()?.0, 400);
        assert!(bad.buffered.is_empty());
        assert_eq!(bad.socket.read(&mut [0; 1])?, 0);

        let mut large = Peer::try_connect(bind)?;
        large
            .socket
            .write_all(&Request::new("/limited").chunked(&[b"123456"], false))?;
        assert_eq!(large.try_response()?.0, 413);
        assert!(large.buffered.is_empty());
        assert_eq!(large.socket.read(&mut [0; 1])?, 0);
        Ok(())
    })
}

#[test]
fn invalid_after_reply() -> Result<(), Error> {
    serve(None, |bind| {
        let mut peer = Peer::try_connect(bind)?;
        peer.socket.write_all(
            b"GET /ping HTTP/1.1\r\nHost: local\r\n\r\nPOST /discard HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabcX\n0\r\n\r\n",
        )?;
        assert_eq!(peer.try_response()?, (200, b"pong".to_vec()));
        assert_eq!(peer.try_response()?.0, 400);
        assert!(peer.buffered.is_empty());
        assert_eq!(peer.socket.read(&mut [0; 1])?, 0);
        Ok(())
    })
}

#[test]
fn close_ends_pipeline() -> Result<(), Error> {
    serve(None, |bind| {
        let mut peer = Peer::try_connect(bind)?;
        peer.socket.write_all(
            b"GET /ping HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\nGET /ping HTTP/1.1\r\nHost: local\r\n\r\n",
        )?;
        assert_eq!(peer.try_response()?, (200, b"pong".to_vec()));
        assert!(peer.buffered.is_empty());
        assert_eq!(peer.socket.read(&mut [0; 1])?, 0);
        Ok(())
    })
}

#[test]
fn async_discard_pipeline() -> Result<(), Error> {
    serve(None, |bind| {
        let mut peer = Peer::try_connect(bind)?;
        let mut requests = Request::new("/async/discard").fixed(b"x");
        requests.extend_from_slice(&Request::new("/async/discard").chunked(&[b"x"], false));
        requests.extend_from_slice(b"GET /ping HTTP/1.1\r\nHost: local\r\n\r\n");
        peer.socket.write_all(&requests)?;

        for expected in [b"1".as_slice(), b"1", b"pong"] {
            assert_eq!(peer.try_response()?, (200, expected.to_vec()));
        }
        assert!(peer.buffered.is_empty());
        Ok(())
    })
}

#[test]
fn closing_fixed_discard() -> Result<(), Error> {
    serve(None, |bind| {
        let mut peer = Peer::try_connect(bind)?;
        peer.socket.write_wait(Some(Duration::from_secs(5)))?;
        peer.socket
            .write_all(&Request::new("/async/discard").close_head(UPLOAD.len()))?;
        peer.socket.write_all(UPLOAD)?;
        assert_eq!(
            peer.try_response()?,
            (200, UPLOAD.len().to_string().into_bytes())
        );
        assert!(peer.buffered.is_empty());
        assert_eq!(peer.socket.read(&mut [0; 1])?, 0);
        Ok(())
    })
}

#[test]
fn bodies_survive_wakes() -> Result<(), Error> {
    serve(None, |bind| {
        let mut peer = Peer::try_connect(bind)?;
        let mut requests =
            Request::new("/async/segments").chunked(&[b"one-", b"two-", b"three"], false);
        requests.extend_from_slice(
            &Request::new("/async/contiguous").chunked(&[b"owner-", b"backed"], false),
        );
        requests.extend_from_slice(
            &Request::new("/async/json")
                .chunked(&[br#"{"value":"#, br#""json-owner""#, br#"}"#], false),
        );
        peer.socket.write_all(&requests)?;

        assert_eq!(peer.try_response()?, (200, b"one-two-three".to_vec()));
        assert_eq!(peer.try_response()?, (200, b"owner-backed".to_vec()));
        assert_eq!(peer.try_response()?, (200, b"json-owner".to_vec()));
        Ok(())
    })
}

#[test]
fn open_json_failure_finishes() -> Result<(), Error> {
    serve(None, |bind| {
        for (framing, head, tail) in [
            (
                "fixed",
                b"POST /async/json HTTP/1.1\r\nHost: local\r\nContent-Length: 32768\r\n\r\n".as_slice(),
                b"".as_slice(),
            ),
            (
                "chunked",
                b"POST /async/json HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n4000\r\n".as_slice(),
                b"\r\n".as_slice(),
            ),
        ] {
            let mut request = head.to_vec();
            request.extend_from_slice(&[b'!'; 16 * 1024]);
            request.extend_from_slice(tail);
            let mut peer = Peer::try_connect(bind)?;
            peer.socket.write_all(&request)?;
            // Deliver a complete staging page while both framing modes remain
            // open. Cleanup must finish without another input byte or EOF.
            let response = peer
                .try_response()
                .unwrap_or_else(|error| panic!("{framing} JSON response: {error:?}"));
            assert_eq!(response.0, 400, "{framing}");
            assert!(peer.buffered.is_empty());
            let closed = peer.socket
                .read(&mut [0; 1])
                .unwrap_or_else(|error| panic!("{framing} JSON close: {error:?}"));
            assert_eq!(closed, 0, "{framing}");
        }
        Ok(())
    })
}

#[test]
fn open_json_waits_for_input() -> Result<(), Error> {
    serve(None, |bind| {
        let value = vec![b'k'; 16 * 1024];
        let mut json = br#"{"value":""#.to_vec();
        json.extend_from_slice(&value);
        json.extend_from_slice(br#""}"#);
        for chunked in [false, true] {
            let request = Request::new("/async/json");
            let request = if chunked {
                request.chunked(&[&json], false)
            } else {
                request.fixed(&json)
            };
            let tail = if chunked {
                b"\"}\r\n0\r\n\r\n".len()
            } else {
                2
            };
            let split = request.len() - tail;
            let mut peer = Peer::try_connect(bind)?;
            peer.socket.write_all(&request[..split])?;
            peer.socket.read_wait(Some(Duration::from_millis(25)))?;
            let waiting = peer.socket.read(&mut [0; 1]).expect_err("incomplete JSON");
            assert_eq!(waiting, PeerError::TimedOut);
            peer.socket.read_wait(Some(Duration::from_secs(5)))?;
            peer.socket.write_all(&request[split..])?;
            let (status, body) = peer.try_response()?;
            assert_eq!((status, body.as_slice()), (200, value.as_slice()));
        }
        Ok(())
    })
}

#[test]
fn chunked_json_pipelines() -> Result<(), Error> {
    serve(None, |bind| {
        let mut peer = Peer::try_connect(bind)?;
        let value = vec![b'x'; 48 * 1024];
        let mut json = br#"{"value":""#.to_vec();
        json.extend_from_slice(&value);
        json.extend_from_slice(br#""}"#);

        let mut prefix =
            b"POST /async/json HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n"
                .to_vec();
        prefix.extend_from_slice(format!("{:x}\r\n", json.len()).as_bytes());
        prefix.extend_from_slice(&json);
        prefix.extend_from_slice(b"\r\n");
        peer.socket.write_all(&prefix)?;
        peer.socket
            .write_all(b"0\r\n\r\nGET /ping HTTP/1.1\r\nHost: local\r\n\r\n")?;

        assert_eq!(peer.try_response()?, (200, value));
        assert_eq!(peer.try_response()?, (200, b"pong".to_vec()));
        Ok(())
    })
}

#[test]
fn fixed_json_pipelines() -> Result<(), Error> {
    serve(None, |bind| {
        let mut peer = Peer::try_connect(bind)?;
        let value = vec![b'y'; 96 * 1024];
        let mut json = br#"{"value":""#.to_vec();
        json.extend_from_slice(&value);
        json.extend_from_slice(br#""}"#);
        let request = Request::new("/async/json").fixed(&json);
        let split = request.len() - 2;
        peer.socket.write_all(&request[..split])?;
        let mut tail = request[split..].to_vec();
        tail.extend_from_slice(b"GET /ping HTTP/1.1\r\nHost: local\r\n\r\n");
        peer.socket.write_all(&tail)?;

        assert_eq!(peer.try_response()?, (200, value));
        assert_eq!(peer.try_response()?, (200, b"pong".to_vec()));
        Ok(())
    })
}

#[test]
fn discard_releases_wire() -> Result<(), Error> {
    serve(None, |bind| {
        const BODY_LEN: usize = 128 * 1024;
        let mut peer = Peer::try_connect(bind)?;
        let mut prefix =
            b"POST /async/discard HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n"
                .to_vec();
        prefix.extend_from_slice(format!("{BODY_LEN:x}\r\n").as_bytes());
        prefix.resize(prefix.len() + BODY_LEN, b'x');
        prefix.extend_from_slice(b"\r\n");
        peer.socket.write_all(&prefix)?;
        peer.socket.write_all(b"0\r\n\r\n")?;

        assert_eq!(
            peer.try_response()?,
            (200, BODY_LEN.to_string().into_bytes())
        );
        Ok(())
    })
}
