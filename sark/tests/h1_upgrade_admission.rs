use sark_h1::{pump::Egress, route::Driver, session::Conn};

pub mod common;

use common::router::ProbeRouter;
use sark_h1::wire::head;

#[test]
fn partial_upgrade_rejected() {
    let conn = b"POST /probe HTTP/1.1\r\nHost: origin.example\r\nConnection: Upgrade\r\nContent-Length: 4\r\n\r\nbody";
    let up = b"POST /probe HTTP/1.1\r\nHost: origin.example\r\nUpgrade: websocket\r\nContent-Length: 4\r\n\r\nbody";

    for wire in [conn.as_slice(), up.as_slice()] {
        'split: for split in 0..=wire.len() {
            let router = ProbeRouter::new();
            let mut driver = Driver::try_new(&router, *b"Thu, 01 Jan 1970 00:00:00 GMT")
                .expect("route caches must bind");
            let mut conn = Conn::default();
            let mut write = [0_u8; 4096];

            for chunk in [&wire[..split], &wire[split..]] {
                let cycle =
                    driver.ingest(&mut conn, chunk, &mut write, head::Connection::KeepAlive);
                if let Ok(Egress::Static { prefix: 0, body }) = cycle.state
                    && body.as_slice().starts_with(b"HTTP/1.1 400")
                {
                    continue 'split;
                }
            }
            panic!("generated admission must reject the request: split {split}");
        }
    }
}
