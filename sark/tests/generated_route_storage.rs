#![cfg(target_os = "linux")]

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use dope_extra::harness::Harness;
use http::StatusCode;
use o3::buffer::Shared;
use sark::{Executor, Throughput, driver};
use sark_core::http::{IterStream, Stream};

type ChunkStream = Stream<IterStream<core::array::IntoIter<Shared, 1>>>;

#[sark_gen::request]
struct EmptyRequest {}

#[sark_gen::response(raw)]
struct Output {
    status: StatusCode,
    body: &'static [u8],
}

#[sark_gen::handler]
fn sync_handler(_request: EmptyRequest, _state: &()) -> Output {
    Output {
        status: StatusCode::OK,
        body: b"sync",
    }
}

#[sark_gen::handler]
async fn narrow_handler(_request: EmptyRequest, _state: &(), timer: sark::Timer) -> Output {
    timer.sleep(Duration::ZERO).await;
    Output {
        status: StatusCode::OK,
        body: b"narrow",
    }
}

#[sark_gen::handler]
async fn wide_handler(_request: EmptyRequest, _state: &(), timer: sark::Timer) -> Output {
    let padding = [7u8; 256];
    timer.sleep(Duration::ZERO).await;
    let _ = padding;
    Output {
        status: StatusCode::OK,
        body: b"wide",
    }
}

#[sark_gen::handler]
fn stream_handler(_request: EmptyRequest, _state: &()) -> ChunkStream {
    Stream::from_chunks([Shared::copy_from_slice(b"stream")])
}

sark_gen::define_route! {
    SyncStorageApp: () => {
        GET "/sync" => sync_handler,
    }
}

sark_gen::define_route! {
    MixedStorageApp: () => {
        GET "/sync" => sync_handler,
        GET "/narrow" => async(capacity = 1) narrow_handler,
        GET "/wide" => async(capacity = 3) wide_handler,
        GET "/stream" => stream(capacity = 2) stream_handler,
    }
}

#[test]
fn sync_only_app_has_no_legacy_task_slab() {
    let _ = SyncStorageApp::new::<dope_net::wire::identity::Identity>(
        (),
        sark::app::Config {
            timer_capacity: 1,
            task_capacity: 1,
        },
    );
}

#[test]
fn mixed_app_uses_route_capacities() {
    let _ = MixedStorageApp::new::<dope_net::wire::identity::Identity>(
        (),
        sark::app::Config {
            timer_capacity: 1,
            task_capacity: 4,
        },
    );
}

#[test]
fn async_and_stream_routes_poll_their_own_slabs() {
    let bind: std::net::SocketAddr = "127.0.0.1:18925".parse().unwrap();
    let server = support::http_server(bind, Duration::from_secs(10));

    Harness::new(bind)
        .run_with_trigger(
            |_context, trigger| {
                let driver_config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let executor = Executor::new(driver_config)?;
                executor.enter(|mut session| {
                    server.clone().serve(
                        &mut session,
                        MixedStorageApp::new(
                            (),
                            sark::app::Config {
                                timer_capacity: support::MAX_CONNECTIONS.saturating_mul(2),
                                task_capacity: support::MAX_CONNECTIONS,
                            },
                        ),
                        Some(trigger),
                    )
                })
            },
            |bind| {
                let mut stream = TcpStream::connect(bind).expect("connect");
                stream
                    .write_all(b"GET /wide HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                    .unwrap();
                let mut response = String::new();
                stream.read_to_string(&mut response).unwrap();
                assert!(response.contains("200 OK"), "response: {response}");
                assert!(response.contains("wide"), "response: {response}");

                let mut stream = TcpStream::connect(bind).expect("connect");
                stream
                    .write_all(b"GET /stream HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                    .unwrap();
                let mut response = String::new();
                stream.read_to_string(&mut response).unwrap();
                assert!(response.contains("200 OK"), "response: {response}");
                assert!(response.contains("stream"), "response: {response}");
            },
        )
        .expect("harness");
}
