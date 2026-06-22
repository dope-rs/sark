#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use dope_extra::testing::run_with_trigger;
use http::StatusCode;
use o3::buffer::Owned;
use sark::{Build, ServerCfg};

#[sark_gen::request]
struct EmptyReq {}

#[sark_gen::response(raw)]
struct Reply {
    status: StatusCode,
    body: Owned,
}

#[sark_gen::handler]
async fn sleep_handler(_req: EmptyReq, _state: &(), timer: sark::Timer) -> Reply {
    timer.sleep(Duration::from_millis(100)).await;
    let mut body = Owned::new();
    body.extend_from_slice(b"slept 100ms");
    Reply {
        status: StatusCode::OK,
        body,
    }
}

sark_gen::define_route! {
    SleepDispatch: () => {
        GET "/sleep" => async sleep_handler,
    }
}

#[test]
fn handler_awaits_timer() {
    let bind: std::net::SocketAddr = "127.0.0.1:18890".parse().unwrap();
    let cfg = ServerCfg {
        bind,
        max_conn: 16,
        backlog: 16,
    };

    run_with_trigger(
        bind,
        |ctx, trigger| Build::http(sleep_dispatch::new(&()), cfg.clone(), ctx, Some(trigger)),
        |bind| {
            let mut sock = TcpStream::connect(bind).expect("connect");
            let start = Instant::now();
            sock.write_all(b"GET /sleep HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                .unwrap();
            let mut resp = String::new();
            sock.read_to_string(&mut resp).unwrap();
            let elapsed = start.elapsed();

            assert!(
                elapsed >= Duration::from_millis(90),
                "elapsed: {:?}",
                elapsed
            );
            assert!(resp.contains("200 OK"), "resp: {}", resp);
            assert!(resp.contains("slept 100ms"), "resp: {}", resp);
        },
    );
}
