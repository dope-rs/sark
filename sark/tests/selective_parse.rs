#![allow(clippy::too_many_arguments)]

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use dope_test::Harness;
use http::StatusCode;
use o3::buffer::{Bytes, Retained};
use sark::{Executor, Throughput, driver};

#[sark_gen::request]
struct MinimalReq {}

#[sark_gen::request(full)]
struct FullReq {}

#[sark_gen::request]
struct RoutedReq {
    #[path("id", default = "")]
    id: Bytes<Retained>,
    #[query("tag", default = "")]
    tag: Bytes<Retained>,
}

#[sark_gen::response(raw)]
struct Reply {
    status: StatusCode,
    body: &'static [u8],
}

#[sark_gen::handler]
fn minimal(_req: MinimalReq, _state: &()) -> Reply {
    Reply {
        status: StatusCode::OK,
        body: b"minimal",
    }
}

#[sark_gen::handler]
fn full(_req: FullReq, _state: &()) -> Reply {
    Reply {
        status: StatusCode::OK,
        body: b"full",
    }
}

#[sark_gen::handler]
fn routed(req: RoutedReq, _state: &()) -> Reply {
    Reply {
        status: if req.id.as_slice() == b"42" && req.tag.as_slice() == b"rust" {
            StatusCode::IM_A_TEAPOT
        } else {
            StatusCode::OK
        },
        body: b"routed",
    }
}

sark_gen::define_route! {
    SelectiveDispatch: () => {
        GET "/minimal" => minimal,
        GET "/full" => full,
        GET "/items/:id" => routed,
    }
}

fn exchange(addr: std::net::SocketAddr, target: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set timeout");
    let mut request = b"GET ".to_vec();
    request.extend_from_slice(target);
    request.extend_from_slice(b" HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n");
    stream.write_all(&request).expect("send request");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read response");
    response
}

fn status(response: &[u8]) -> &[u8] {
    response.get(9..12).expect("HTTP status")
}

#[test]
fn generated_routes_parse_only_declared_capabilities() {
    let bind: std::net::SocketAddr = "127.0.0.1:38767".parse().unwrap();
    let server = support::http_server(bind, Duration::from_secs(10));

    Harness::new(bind)
        .run_with_trigger(
            |_ctx, trigger| {
                let config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let executor = Executor::new(config)?
                    .with_storage(dope_net::link::egress::storage::Storage::default());
                executor.enter(|mut session| {
                    let timer =
                        sark::Timer::with_capacity(support::MAX_CONNECTIONS.saturating_mul(2));
                    server.clone().serve(
                        &mut session,
                        SelectiveDispatch::new(
                            &(),
                            &timer,
                            sark::app::Config {
                                timer_capacity: support::MAX_CONNECTIONS.saturating_mul(2),
                                task_capacity: support::MAX_CONNECTIONS,
                            },
                        ),
                        Some(trigger),
                    )
                })
            },
            |addr| {
                assert_eq!(status(&exchange(addr, b"/minimal?ignored=yes")), b"200");
                assert_eq!(status(&exchange(addr, b"/items/42?tag=rust")), b"418");

                let invalid_unused_query = b"/minimal?ignored=\x01";
                assert_eq!(status(&exchange(addr, invalid_unused_query)), b"200");

                let invalid_full_query = b"/full?ignored=\x01";
                assert_eq!(status(&exchange(addr, invalid_full_query)), b"400");
            },
        )
        .expect("harness");
}
