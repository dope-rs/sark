pub mod common;

use std::{cell::Cell, thread, time::Duration};

use common::{
    retained::{self, Echo, NESTED, Post, State, wire},
    route::{body::retained::EchoDispatch, chunked::peer::Peer},
    server::{self, Plan},
};

#[test]
fn pipeline_keeps_bytes() {
    let (harness, server) = Plan::isolated();

    harness.run(
        |context, listener| {
            let mut reports = Plan::try_reports(&server).map_err(dope::core::BranchError::Second)?;
            let failures = Cell::new(Vec::new());
            let result = server::serve::<EchoDispatch, _>(
                server.with_bound(listener),
                context,
                State(&failures),
                server::MAX_CONNECTIONS,
                &mut reports,
            );
            server::reclaimed(&reports);
            let failures = failures.into_inner();
            assert!(failures.is_empty(), "retention: {failures:?}");
            result
        },
        |bind| {
            let mut sock = retained::try_connect(bind).expect("connect");

            let req1 = b"GET http://origin.example/users/USER-REQ1/teams/TEAM-REQ1?q=kept HTTP/1.1\r\nHost: ignored.example\r\nx-echo-marker: MARKER-REQ1-VALUE\r\n\r\n";
            let req2 = b"GET /users/USER-REQ2/teams/TEAM-REQ2 HTTP/1.1\r\nHost: yyyyyyyyyyyyyyyyyyyyyyyy\r\nx-echo-marker: ZZZZZZZZZZZZZZZZZZZZZZZZ\r\n\r\n";
            let mut batch = Vec::new();
            batch.extend_from_slice(req1);
            batch.extend_from_slice(req2);
            sock.write_all(&batch)
                .expect("connected test socket accepts both pipelined requests");

            let mut acc = Vec::new();
            let resp1 = Peer::new(&mut sock, &mut acc).try_text().expect("HTTP response");
            let resp2 = Peer::new(&mut sock, &mut acc).try_text().expect("HTTP response");

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
        },
    ).expect("harness");
}

#[test]
fn retains_split_and_whole_requests() {
    for split in [true, false] {
        let (harness, server) = Plan::isolated();
        harness
            .run(
                |context, listener| {
                    retained::serve(server, context, listener, server::MAX_CONNECTIONS)
                },
                |bind| {
                    let mut socket = retained::try_connect(bind).expect("connect");
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
                        socket.write_all(head).expect("write first head");
                        thread::sleep(Duration::from_millis(15));
                        body
                    } else {
                        first.as_slice()
                    };
                    socket.write_all(rest).expect("finish first request");
                    thread::sleep(Duration::from_millis(15));
                    socket
                        .write_all(&requests[1].wire())
                        .expect("write second request");

                    let mut buffered = Vec::new();
                    for request in requests {
                        let reply = Peer::new(&mut socket, &mut buffered)
                            .try_text()
                            .expect("HTTP response");
                        assert_eq!(reply, (200, request.expected()), "split head: {split}");
                    }
                },
            )
            .expect("harness");
    }
}

#[test]
fn body_transfer() {
    let (harness, server) = Plan::isolated();
    harness
        .run(
            |context, listener| retained::serve(server, context, listener, server::MAX_CONNECTIONS),
            |bind| {
                let payload = vec![b'x'; 16 * 1024];
                let mut sock = retained::try_connect(bind).expect("connect");
                sock.write_all(&wire(&payload))
                    .expect("write retained body request");

                let mut acc = Vec::new();
                let (status, body) = Peer::new(&mut sock, &mut acc)
                    .try_text()
                    .expect("HTTP response");
                assert_eq!(status, 200);
                assert_eq!(body.as_bytes(), payload);
            },
        )
        .expect("harness");
}

#[test]
fn listener_admission() {
    const TASKS: usize = 1;
    const { assert!(TASKS < server::MAX_CONNECTIONS) };
    let (harness, server) = Plan::isolated();
    harness
        .run(
            |context, listener| retained::serve(server, context, listener, TASKS),
            |bind| {
                let head = b"POST /retained/id?q=query HTTP/1.1\r\nHost: x\r\nx-echo-marker: marker\r\nContent-Length: 2\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n";
                let mut sockets = Vec::with_capacity(server::MAX_CONNECTIONS);
                for _ in 0..server::MAX_CONNECTIONS {
                    let mut socket = retained::try_connect(bind).expect("connect");
                    socket.write_all(head).expect("write partial body");
                    let mut acc = Vec::new();
                    let (status, body) = Peer::new(&mut socket, &mut acc).try_text().expect("HTTP response");
                    assert_eq!(status, 100, "continuation was not admitted");
                    assert!(body.is_empty());
                    sockets.push((socket, acc));
                }

                assert_eq!(sockets.len(), server::MAX_CONNECTIONS);
                for (mut socket, mut acc) in sockets {
                    socket.write_all(b"xy").expect("finish body");
                    let (status, body) = Peer::new(&mut socket, &mut acc).try_text().expect("HTTP response");
                    assert_eq!(status, 200, "continuation rejected before task admission");
                    assert_eq!(body, "xy");
                }
            },
        )
        .expect("harness");
}

#[test]
fn json_transfer() {
    let (harness, server) = Plan::isolated();
    harness
        .run(
            |context, listener| retained::serve(server, context, listener, server::MAX_CONNECTIONS),
            |bind| {
                let mut sock = retained::try_connect(bind).expect("connect");
                sock.write_all(
                    &Post::new("/retained-json", br#"{"value":"owner-backed-json"}"#).wire(),
                )
                .expect("write retained JSON request");
                sock.write_all(&Post::new("/retained-json/nested", NESTED).wire())
                    .expect("write nested retained JSON request");

                let mut acc = Vec::new();
                let (status, body) = Peer::new(&mut sock, &mut acc)
                    .try_text()
                    .expect("HTTP response");
                assert_eq!(status, 200);
                assert_eq!(body, "owner-backed-json");
                let (status, body) = Peer::new(&mut sock, &mut acc)
                    .try_text()
                    .expect("HTTP response");
                assert_eq!(status, 200);
                assert_eq!(body, "first-owner");
            },
        )
        .expect("harness");
}
