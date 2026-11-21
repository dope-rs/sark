pub mod common;

use std::{thread, time::Duration};

use common::{
    retained::{self, Echo, EchoDispatch, NESTED, Post, wire},
    route::body::retained as users,
    server::{MAX_CONNECTIONS, Plan},
};

#[test]
fn pipeline_keeps_bytes() {
    retained::run::<users::EchoDispatch, _>(Plan::isolated(), MAX_CONNECTIONS, |bind| {
        let mut peer = retained::try_connect(bind).expect("connect");

        let req1 = b"GET http://origin.example/users/USER-REQ1/teams/TEAM-REQ1?q=kept HTTP/1.1\r\nHost: ignored.example\r\nx-echo-marker: MARKER-REQ1-VALUE\r\n\r\n";
        let req2 = b"GET /users/USER-REQ2/teams/TEAM-REQ2 HTTP/1.1\r\nHost: yyyyyyyyyyyyyyyyyyyyyyyy\r\nx-echo-marker: ZZZZZZZZZZZZZZZZZZZZZZZZ\r\n\r\n";
        let mut batch = Vec::new();
        batch.extend_from_slice(req1);
        batch.extend_from_slice(req2);
        peer.socket
            .write_all(&batch)
            .expect("connected test socket accepts both pipelined requests");

        let resp1 = peer.try_text().expect("HTTP response");
        let resp2 = peer.try_text().expect("HTTP response");

        assert_eq!(resp1.0, 200, "resp1 body: {:?}", resp1.1);
        assert_eq!(
            resp1.1,
            "team=TEAM-REQ1 user=USER-REQ1 marker=MARKER-REQ1-VALUE authority=origin.example",
            "first response did not echo the original request bytes (use-after-free?)"
        );
        assert_eq!(resp2.0, 200, "resp2 body: {:?}", resp2.1);
        assert_eq!(
            resp2.1,
            "team=TEAM-REQ2 user=USER-REQ2 marker=ZZZZZZZZZZZZZZZZZZZZZZZZ authority=yyyyyyyyyyyyyyyyyyyyyyyy"
        );
    });
}

#[test]
fn retains_split_and_whole_requests() {
    for split in [true, false] {
        retained::run::<EchoDispatch, _>(Plan::isolated(), MAX_CONNECTIONS, |bind| {
            let mut peer = retained::try_connect(bind).expect("connect");
            let requests = [
                Echo::new("REQ1ID", "MARKER-ONE", "BODY-REQ1-AAAA"),
                Echo::new("REQ2ID", "MARKER-TWO", "BODY-REQ2-BBBB"),
            ];
            let first = requests[0].wire();
            let rest = if split {
                let end = first
                    .windows(4)
                    .position(|w| w == b"\r\n\r\n")
                    .expect("head")
                    + 4;
                let (head, body) = first.split_at(end);
                peer.socket.write_all(head).expect("write first head");
                thread::sleep(Duration::from_millis(15));
                body
            } else {
                first.as_slice()
            };
            peer.socket.write_all(rest).expect("finish first request");
            thread::sleep(Duration::from_millis(15));
            peer.socket
                .write_all(&requests[1].wire())
                .expect("write second request");

            for request in requests {
                let reply = peer.try_text().expect("HTTP response");
                assert_eq!(reply, (200, request.expected()), "split head: {split}");
            }
        });
    }
}

#[test]
fn body_transfer() {
    retained::run::<EchoDispatch, _>(Plan::isolated(), MAX_CONNECTIONS, |bind| {
        let payload = vec![b'x'; 16 * 1024];
        let mut peer = retained::try_connect(bind).expect("connect");
        peer.socket
            .write_all(&wire(&payload))
            .expect("write retained body request");

        let (status, body) = peer.try_text().expect("HTTP response");
        assert_eq!(status, 200);
        assert_eq!(body.as_bytes(), payload);
    });
}

#[test]
fn listener_admission() {
    const TASKS: usize = 1;
    const { assert!(TASKS < MAX_CONNECTIONS) };
    retained::run::<EchoDispatch, _>(Plan::isolated(), TASKS, |bind| {
        let head = b"POST /retained/id?q=query HTTP/1.1\r\nHost: x\r\nx-echo-marker: marker\r\nContent-Length: 2\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n";
        let mut sockets = Vec::with_capacity(MAX_CONNECTIONS);
        for _ in 0..MAX_CONNECTIONS {
            let mut peer = retained::try_connect(bind).expect("connect");
            peer.socket.write_all(head).expect("write partial body");
            let (status, body) = peer.try_text().expect("HTTP response");
            assert_eq!(status, 100, "continuation was not admitted");
            assert!(body.is_empty());
            sockets.push(peer);
        }

        assert_eq!(sockets.len(), MAX_CONNECTIONS);
        for mut peer in sockets {
            peer.socket.write_all(b"xy").expect("finish body");
            let (status, body) = peer.try_text().expect("HTTP response");
            assert_eq!(status, 200, "continuation rejected before task admission");
            assert_eq!(body, "xy");
        }
    });
}

#[test]
fn json_transfer() {
    retained::run::<EchoDispatch, _>(Plan::isolated(), MAX_CONNECTIONS, |bind| {
        let mut peer = retained::try_connect(bind).expect("connect");
        peer.socket
            .write_all(&Post::new("/retained-json", br#"{"value":"owner-backed-json"}"#).wire())
            .expect("write retained JSON request");
        peer.socket
            .write_all(&Post::new("/retained-json/nested", NESTED).wire())
            .expect("write nested retained JSON request");

        let (status, body) = peer.try_text().expect("HTTP response");
        assert_eq!(status, 200);
        assert_eq!(body, "owner-backed-json");
        let (status, body) = peer.try_text().expect("HTTP response");
        assert_eq!(status, 200);
        assert_eq!(body, "first-owner");
    });
}
