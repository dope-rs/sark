use std::net::SocketAddr;

use o3::buffer::Shared;
use sark::{HttpServer, Tcp, Throughput, app, driver, listener, tcp};
use sark_core::http::{IterStream, Stream};

#[sark_gen::request(ordered)]
struct DownloadRequest {
    #[path("n")]
    pub n: usize,
}

type ChunkStream = Stream<IterStream<std::iter::Map<std::ops::Range<usize>, fn(usize) -> Shared>>>;

const MAX_CONNECTIONS: usize = 1024;

#[sark_gen::handler]
fn download(request: DownloadRequest, _state: &()) -> ChunkStream {
    let n = request.n.min(10_000);
    let mk: fn(usize) -> Shared = |i| Shared::from(format!("chunk {i}\n").into_bytes());
    Stream::from_chunks((0..n).map(mk)).header(b"content-type", b"text/plain; charset=utf-8")
}

sark_gen::define_route! {
    DownloadApp: () => {
        GET "/download/:n" => stream(capacity = MAX_CONNECTIONS) download,
    }
}

fn main() -> std::io::Result<()> {
    const HTTP_LISTENER_ID: u8 = 0;
    const DATE_UPDATER_ID: u8 = 1;
    let bind: SocketAddr = std::env::var("BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse()
        .expect("invalid BIND");

    let server = HttpServer::<HTTP_LISTENER_ID, DATE_UPDATER_ID, Throughput>::new(
        listener::Config::<Tcp> {
            bind,
            max_connections: MAX_CONNECTIONS,
            backlog: 1024,
            stream: tcp::stream::Config {
                no_delay: Some(true),
                ..Default::default()
            },
            transport: tcp::listener::Config {
                reuse_port: true,
                ..Default::default()
            },
            egress: Default::default(),
        },
        std::time::Duration::from_secs(10),
    );

    eprintln!("sark download example: GET http://{bind}/download/<n>");

    server.run(
        vec![0u16],
        |_cpu| driver::Config::for_tcp_profile::<Throughput>(MAX_CONNECTIONS),
        |server, session| {
            server.serve(
                session,
                DownloadApp::new(
                    (),
                    app::Config {
                        timer_capacity: MAX_CONNECTIONS.saturating_mul(2),
                        task_capacity: MAX_CONNECTIONS,
                    },
                ),
                None,
            )
        },
    )
}
