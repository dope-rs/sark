use std::net::SocketAddr;

use dope::launcher::Launcher;
use o3::buffer::Shared;
use sark::{Build, ServerCfg};
use sark_core::http::{IterStream, Stream};

#[sark_gen::request(ordered)]
struct DownloadRequest {
    #[path("n", default = "10")]
    pub n: sark_core::http::LocalFrameBytes,
}

type ChunkStream = Stream<IterStream<std::iter::Map<std::ops::Range<usize>, fn(usize) -> Shared>>>;

#[sark_gen::handler]
fn download(request: DownloadRequest, _state: &()) -> ChunkStream {
    let n: usize = std::str::from_utf8(request.n.as_bytes())
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let mk: fn(usize) -> Shared = |i| Shared::copy_from_slice(format!("chunk {i}\n").as_bytes());
    Stream::from_chunks((0..n).map(mk)).header(b"content-type", b"text/plain; charset=utf-8")
}

sark_gen::define_route! {
    DownloadApp: () => {
        GET "/download/:n" => download,
    }
}

fn main() -> std::io::Result<()> {
    let bind: SocketAddr = std::env::var("BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse()
        .expect("invalid BIND");

    let cfg = ServerCfg {
        bind,
        max_conn: 1024,
        backlog: 1024,
        head_timeout: std::time::Duration::from_secs(10),
    };

    eprintln!("sark download example: GET http://{bind}/download/<n>");

    Launcher::new(vec![0u16])
        .run(move |ctx| Build::http(download_app::new(&()), cfg.clone(), ctx, None))
}
