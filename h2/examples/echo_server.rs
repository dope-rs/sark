use std::env;
use std::net::SocketAddr;

use dope::runtime::{Launcher, WorkerContext, WorkerEntry};
use sark_h2::server::{Body, Config, Response, serve_sync};

struct Worker;

impl WorkerEntry for Worker {
    type Input = Config;

    fn run(config: Self::Input, context: WorkerContext) -> std::io::Result<()> {
        let large = Body::repeat(b'x', 1 << 20);
        serve_sync(
            move |request| {
                if request.path().is_some_and(|path| path == b"/large") {
                    Response::text(large.clone())
                } else {
                    Response::text(b"hello from sark-h2\n")
                }
            },
            config,
            context,
            None,
        )
    }
}

fn main() -> std::io::Result<()> {
    let port: u16 = env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(18080);
    let bind: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let config = Config {
        bind_addr: bind,
        max_connections: 1024,
        max_connections_per_ip: 512,
        listen_backlog: 256,
        max_handler_tasks: 0,
        max_request_body_bytes: 16 << 20,
        max_connection_body_bytes: 64 << 20,
        max_outbound_bytes: 64 << 10,
        socket_receive_buffer_bytes: None,
        socket_send_buffer_bytes: None,
        tcp_fast_open_backlog: None,
        receive_buffer_bytes: 64 << 10,
        receive_buffer_count: 1024,
    };
    eprintln!("echo_server: listening on {bind}");
    Launcher::unbound(1)?.run::<Worker>(vec![config])
}
