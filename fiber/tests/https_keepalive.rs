pub mod common;

use std::net::{Ipv4Addr, SocketAddr};

use common::{client::runtime::sockets, server::Plan, support};

fn check_plain(bind: SocketAddr) -> Result<(), sockets::Error> {
    let mut client = sockets::Plain::connect(bind)?;
    for _ in 0..2 {
        client.request("/hello", bind)?;
        let response = client.response()?;
        let reply = sockets::Response::try_parse(&response)?;
        assert_eq!((reply.status, reply.body), (200, b"hello".as_slice()));
    }
    Ok(())
}

fn check_tls(bind: SocketAddr) -> Result<(), sockets::Error> {
    let mut client = sockets::Tls::connect(
        bind,
        support::tls().client().expect("TLS client config"),
        Ipv4Addr::LOCALHOST.into(),
    )?;
    let body = b"discard only decrypted bytes";
    let head = format!(
        "POST /hello HTTP/1.1\r\nHost: {bind}\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    client.write(head.as_bytes())?;
    let response = client.response()?;
    let reply = sockets::Response::try_parse(&response)?;
    assert_eq!((reply.status, reply.body), (200, b"hello".as_slice()));
    let mut tail = body.to_vec();
    tail.extend_from_slice(format!("GET /hello HTTP/1.1\r\nHost: {bind}\r\n\r\n").as_bytes());
    client.write(&tail)?;
    let response = client.response()?;
    let reply = sockets::Response::try_parse(&response)?;
    assert_eq!((reply.status, reply.body), (200, b"hello".as_slice()));
    let chunks = 128;
    let head = format!(
        "POST /hello HTTP/1.1\r\nHost: {bind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        sockets::UPLOAD_CHUNK.len() * chunks
    );
    client.write(head.as_bytes())?;
    let response = client.response()?;
    let reply = sockets::Response::try_parse(&response)?;
    assert_eq!((reply.status, reply.body), (200, b"hello".as_slice()));
    for _ in 0..chunks {
        client.write(&sockets::UPLOAD_CHUNK)?;
    }
    assert!(matches!(
        client.response(),
        Err(sockets::Error::Closed { buffered: 0 })
    ));
    drop(client);
    Ok(())
}

#[test]
fn https_streams_large_body() {
    let (pair, plan) = Plan::isolated();
    pair.run(
        move |ready, listener| support::serve::<true>(plan.with_bound(listener), ready),
        |bind| {
            let mut client = sockets::Tls::connect(
                bind,
                support::tls().client().expect("TLS client config"),
                Ipv4Addr::LOCALHOST.into(),
            )?;
            client.request("/big", bind)?;
            let response = client.response()?;
            let reply = sockets::Response::try_parse(&response)?;
            assert_eq!(reply.status, 200);
            assert_eq!(reply.body.len(), support::BIG_LEN);
            assert!(reply.body.iter().all(|&byte| byte == b'x'));
            drop(client);
            Ok::<_, sockets::Error>(())
        },
    )
    .expect("TLS streaming server")
    .expect("TLS large response");
}

#[test]
fn modes_reuse_connections() {
    let (pair, plan) = Plan::isolated();
    pair.run_unbound(
        move |ready| support::serve::<false>(plan, ready),
        check_plain,
    )
    .expect("configured HTTP server")
    .expect("plain keepalive");

    let (pair, plan) = Plan::isolated();
    pair.run(
        move |ready, listener| support::serve::<false>(plan.with_bound(listener), ready),
        check_plain,
    )
    .expect("bound HTTP server")
    .expect("plain keepalive");

    let (pair, plan) = Plan::isolated();
    pair.run_unbound(move |ready| support::serve::<true>(plan, ready), check_tls)
        .expect("configured TLS server")
        .expect("TLS keepalive");

    let (pair, plan) = Plan::isolated();
    pair.run(
        move |ready, listener| support::serve::<true>(plan.with_bound(listener), ready),
        check_tls,
    )
    .expect("bound TLS server")
    .expect("TLS keepalive");
}
