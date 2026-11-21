use std::net::SocketAddr;

use dope_fiber::BlockOn as _;
use sark_client::protocol::response::Event;
use sark_fiber::client::config::Config;

use crate::common::client::{assert_shutdown, runtime::pools};

pub struct Capture {
    pub status: u16,
    pub body: Vec<u8>,
    pub trailers: usize,
    pub info: usize,
}

pub fn read(addr: SocketAddr, path: &str) -> Capture {
    let config = Config::try_new("127.0.0.1").expect("HTTP host allocation");
    pools::Plan::new(addr, config, 1, 1)
        .expect("HTTP client plan")
        .enter(
            (),
            |mut app| {
                app.wait().expect("HTTP peer ready");
                let mut stream = app
                    .app
                    .block_on(app.handle.stream_get(path))
                    .expect("drive HTTP request")
                    .expect("HTTP stream");
                let mut status = None;
                let mut body = Vec::new();
                let mut trailers = 0;
                let mut info = 0;
                while let Some(event) = app
                    .app
                    .block_on(stream.next_event())
                    .expect("drive HTTP response")
                {
                    match event.expect("HTTP response event") {
                        Event::Informational(_) => info += 1,
                        Event::Head(head) => status = Some(head.status().as_u16()),
                        Event::Data(data) => body.extend_from_slice(data.as_slice()),
                        Event::Trailers(fields) => {
                            trailers += fields
                                .as_bytes()
                                .iter()
                                .filter(|&&byte| byte == b'\n')
                                .count();
                        }
                    }
                }
                Capture {
                    status: status.expect("HTTP response head"),
                    body,
                    trailers,
                    info,
                }
            },
            assert_shutdown,
        )
        .expect("HTTP client scope")
}
