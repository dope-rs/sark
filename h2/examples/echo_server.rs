use std::env;
use std::future::ready;
use std::net::SocketAddr;

use dope::fiber::Fiber;
use dope::launcher::Launcher;
use o3::buffer::Shared;
use sark_h2::hpack::OwnedHeader;
use sark_h2::server::{Cfg, Handler, Request, Response, serve};

struct Echo;

impl Handler for Echo {
    type Fut<'h> = std::future::Ready<Response>;

    fn on_request<'h>(&'h self, req: Request) -> Fiber<'h, Self::Fut<'h>> {
        let path = req
            .headers
            .iter()
            .find(|h| h.name == b":path")
            .map(|h| h.value.clone())
            .unwrap_or_default();
        let body: Shared = if path == b"/large" {
            Shared::from(vec![b'x'; 1 << 20])
        } else {
            Shared::from(b"hello from sark-h2\n".to_vec())
        };
        let headers = vec![
            OwnedHeader::new(b":status", b"200"),
            OwnedHeader::new(b"content-type", b"text/plain"),
            OwnedHeader::new(b"content-length", body.len().to_string().as_bytes()),
        ];
        Fiber::new(ready(Response::new(headers, body)))
    }
}

fn main() -> std::io::Result<()> {
    let port: u16 = env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(18080);
    let bind: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let cfg = Cfg {
        bind,
        max_conn: 1024,
        backlog: 256,
    };
    eprintln!("echo_server: listening on {bind}");
    Launcher::new(vec![0u16]).run(move |ctx| serve(Echo, cfg.clone(), ctx, None))
}
