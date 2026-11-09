use std::{io::Read, net::TcpListener, str};

use common::{app, client::Probe, upgrade::OWNED_HEADERS};
use dope::core::{driver::life::recovery, io::Reclaimed};
use dope_test::harness::scenario::run::{Entry, Task as _};
use sark_http::field;
use sark_ws::{client, client::headers};

pub mod common;

#[test]
fn owned_names_are_reserved() {
    for (index, owned) in OWNED_HEADERS
        .into_iter()
        .chain(["content-length", "transfer-encoding"])
        .enumerate()
    {
        let config = client::Config::new("127.0.0.1", "/ws");
        let config = config.expect("endpoint");
        let result = config.with_header(owned.to_ascii_uppercase(), "shadow");
        assert!(matches!(
            result,
            Err(client::Error::Header(headers::Error::Reserved { index: found }))
                if found == index
        ));
    }
}

#[test]
fn field_rejects_http_ctl() {
    let config = client::Config::new("127.0.0.1", "/ws");
    let config = config.expect("endpoint");
    let agent = config.user_agent("a\0b");
    assert!(matches!(
        agent,
        Err(client::Error::Agent(field::Error::InvalidByte {
            index: 1,
            byte: 0,
        }))
    ));

    let config = client::Config::new("127.0.0.1", "/ws");
    let config = config.expect("endpoint");
    let custom = config.with_header("X-Probe", "a\0b");
    assert!(matches!(
        custom,
        Err(client::Error::Header(headers::Error::Value(
            field::Error::InvalidByte { index: 1, byte: 0 },
        )))
    ));
}

#[test]
fn config_rejects_injection() {
    for (host, path) in [
        ("safe.example\r\nX-Injected: yes", "/ws"),
        ("safe.example", "/ws HTTP/1.1\r\nHost: injected.example"),
        ("safe.example", "/ws\r\n\r\nGET /second"),
        ("safe.example", "https://other.example/ws"),
        ("safe.example", "*"),
    ] {
        assert!(matches!(
            client::Config::new(host, path),
            Err(client::Error::Origin(_) | client::Error::Target(_))
        ));
    }

    let config = client::Config::new("safe.example", "/ws");
    let config = config.expect("endpoint");
    let host = config.with_host("safe.example\r\nX-Injected: yes");
    assert!(matches!(host, Err(client::Error::Target(_))));

    let config = client::Config::new("safe.example", "/ws");
    let config = config.expect("endpoint");
    let path = config.with_path("/ws\r\n\r\nGET /second");
    assert!(matches!(path, Err(client::Error::Origin(_))));
}

#[test]
fn owned_headers_are_unique() {
    let mut retired = Vec::new();
    {
        let sock = TcpListener::bind("127.0.0.1:0").expect("bind handshake probe");
        let addr = sock.local_addr().expect("handshake probe address");
        let ((), req) = Entry(move || {
            let (mut stream, _) = sock.accept().expect("accept handshake probe");
            let req = app::Upgrade::new(&stream).try_head().expect("request head");
            app::Upgrade::new(&stream)
                .try_respond(&app::Head::new(&req).try_accept().expect("opening accept"))
                .expect("opening response");
            while stream.read(&mut [0; 64]).expect("client close") != 0 {}
            req
        })
        .with(|| {
            let config = {
                let agent = String::from("sark\tcafé");
                let name = String::from("X-Configured");
                let value = String::from("config");
                let config = client::Config::new("[::1]:443", "/ws/%2F?q=%0D%0A");
                let config = config.expect("endpoint");
                let config = config.user_agent(agent.as_str());
                let config = config.expect("agent");
                let config = config.with_header(name.as_str(), value.as_str());
                let config = config.expect("header");
                let config = config.with_header("X-Removed", "gone");
                config.expect("header")
            };
            let probe =
                Probe::new(addr, config).with_headers(vec![("X-Dynamic".into(), "handler".into())]);
            let probe = probe.expect("headers");
            drop(probe.omit(vec!["x-removed".into()]).run(
                common::client::Goal::Open,
                recovery::Policy::new(|event| retired.push(event), common::recover),
            ));
        })
        .expect("handshake probe");
        let req = str::from_utf8(&req).expect("ASCII handshake request");
        assert!(req.starts_with("GET /ws/%2F?q=%0D%0A HTTP/1.1\r\nHost: [::1]:443\r\n"));
        assert!(req.contains("\r\nUser-Agent: sark\tcafé\r\n"));
        assert!(!req.contains("\r\nX-Removed:"));
        let fields: Vec<_> = req
            .split("\r\n")
            .filter_map(|line| line.split_once(':'))
            .collect();

        for owned in OWNED_HEADERS {
            assert_eq!(
                fields
                    .iter()
                    .filter(|field| field.0.eq_ignore_ascii_case(owned))
                    .count(),
                1,
                "{owned} must have one wire authority"
            );
        }
        assert_eq!(
            fields
                .iter()
                .filter(|(name, value)| *name == "X-Configured" && value.trim() == "config")
                .count(),
            1
        );
        assert_eq!(
            fields
                .iter()
                .filter(|(name, value)| *name == "X-Dynamic" && value.trim() == "handler")
                .count(),
            1
        );
    };
    assert!(
        retired
            .iter()
            .all(|event| matches!(event, Reclaimed::Shutdown)),
        "unexpected reports WebSocket client completion: {retired:?}"
    );
}
