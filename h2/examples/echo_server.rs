use std::env;
use std::net::SocketAddr;

use dope::launcher::Launcher;
use sark_h2::server::{Cfg, Handler, serve};
use sark_h2::{Conn, Header, ServerRole, conn};

struct Echo;

impl Handler for Echo {
    fn on_event(&mut self, event: conn::Event, conn: &mut Conn<ServerRole>) {
        if let conn::Event::Headers {
            stream_id,
            end_stream,
            ..
        } = event
        {
            if !end_stream {
                return;
            }
            let status = Header {
                name: b":status",
                value: b"200",
            };
            let content_type = Header {
                name: b"content-type",
                value: b"text/plain",
            };
            let content_length = Header {
                name: b"content-length",
                value: b"19",
            };
            let body: &[u8] = b"hello from sark-h2\n";
            let _ = conn.send_response(stream_id, &[status, content_type, content_length], false);
            let _ = conn.send_data(stream_id, body, true);
        }
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
