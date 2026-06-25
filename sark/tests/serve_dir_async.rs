use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use dope::fiber::Holding;
use dope::manifold::Manifold;
use dope::manifold::file::Files;
use dope::runtime::park::Parker;
use dope::runtime::token::{Epoch, LocalIdx, Token};
use dope::{Drive, DriverConfig, Event, Executor};
use sark::fs::ServeDir;
use sark_core::http::{Response, StatusCode};

const ID: u8 = 7;
const SLOTS: usize = 64;

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "sark_serve_dir_async_{}_{}",
            std::process::id(),
            name
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("static")).unwrap();
        Self { root }
    }

    fn write(&self, rel: &str, bytes: &[u8]) {
        let path = self.root.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }

    fn remove(&self, rel: &str) {
        let _ = fs::remove_file(self.root.join(rel));
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn run<F, T>(exec: &mut Executor, host: Holding<'_, Files<ID, SLOTS>>, fut: F) -> T
where
    F: Future<Output = T>,
{
    let driver: &mut dope::Driver = exec.driver_mut();
    let sentinel = Token::new(ID, LocalIdx::new((SLOTS - 1) as u32), Epoch::INITIAL);
    let slot = Parker::make_slot(&*driver, sentinel);
    let waker = slot.make_waker();
    let mut cx = Context::from_waker(&waker);

    let mut fut = Box::pin(fut);
    let mut cqe_buf = [dope::Cqe::ZERO; 32];
    let mut wake_buf: Vec<Token> = Vec::new();

    for _ in 0..2000 {
        if let Poll::Ready(out) = Pin::as_mut(&mut fut).poll(&mut cx) {
            return out;
        }
        let _ = Drive::park(driver, Duration::from_millis(20));
        let n = Drive::drain(driver, &mut cqe_buf);
        for cqe in &cqe_buf[..n] {
            if let Ok(ev) = Event::try_from(*cqe) {
                Manifold::dispatch(host.hold(), ev, driver);
            }
        }
        wake_buf.clear();
        Parker::drain(&*driver, &mut wake_buf);
    }
    panic!("future did not complete");
}

fn cfg() -> dope::DriverCfg {
    dope::DriverCfg::for_profile::<dope::runtime::profile::Throughput>()
}

fn serve_one(
    exec: &mut Executor,
    host: Holding<'_, Files<ID, SLOTS>>,
    serve: &ServeDir,
    rel: &[u8],
    accept_encoding: &[u8],
) -> Response {
    let driver_ptr: *mut dope::Driver = exec.driver_mut();
    run(exec, host, async move {
        // SAFETY: single-threaded test; the driver outlives this future and run() releases
        // its &mut Driver borrow before each poll.
        let driver = unsafe { &mut *driver_ptr };
        serve.serve_async(host, driver, rel, accept_encoding).await
    })
}

fn content_type(resp: &Response) -> String {
    resp.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

fn wire(resp: &Response) -> String {
    String::from_utf8_lossy(resp.wire_headers()).to_ascii_lowercase()
}

#[test]
fn cold_miss_serves_bytes_via_async_file_path() {
    let fx = Fixture::new("cold_miss");
    let payload: Vec<u8> = (0..9000u32).map(|i| (i % 251) as u8).collect();
    fx.write("static/data.bin", &payload);
    let serve = ServeDir::new(&fx.root).cache_valid(Duration::from_secs(3600));

    let mut exec = Executor::new(cfg()).unwrap();
    let mut files: Pin<Box<Files<ID, SLOTS>>> = Box::pin(Files::new());
    let host = Holding::of(files.as_mut());

    let resp = serve_one(&mut exec, host, &serve, b"static/data.bin", b"");

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.body(), payload.as_slice());
    assert_eq!(content_type(&resp), "application/octet-stream");
}

#[test]
fn warm_hit_serves_after_delete_with_zero_file_ops() {
    let fx = Fixture::new("warm_hit");
    fx.write("static/cached.css", b"ORIGINAL-CSS-BYTES");
    let serve = ServeDir::new(&fx.root).cache_valid(Duration::from_secs(3600));

    let mut exec = Executor::new(cfg()).unwrap();
    let mut files: Pin<Box<Files<ID, SLOTS>>> = Box::pin(Files::new());
    let host = Holding::of(files.as_mut());

    let first = serve_one(&mut exec, host, &serve, b"static/cached.css", b"");
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.body(), b"ORIGINAL-CSS-BYTES");

    fx.remove("static/cached.css");

    let second = serve_one(&mut exec, host, &serve, b"static/cached.css", b"");
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(second.body(), b"ORIGINAL-CSS-BYTES");
    assert_eq!(content_type(&second), "text/css");
}

#[test]
fn async_precompressed_variant_then_cached() {
    let fx = Fixture::new("async_precompress");
    fx.write("static/asset.css", b"original-uncompressed");
    fx.write("static/asset.css.br", b"BR-BYTES");
    fx.write("static/asset.css.gz", b"GZIP-BYTES");
    let serve = ServeDir::new(&fx.root)
        .precompressed_br()
        .precompressed_gzip()
        .cache_valid(Duration::from_secs(3600));

    let mut exec = Executor::new(cfg()).unwrap();
    let mut files: Pin<Box<Files<ID, SLOTS>>> = Box::pin(Files::new());
    let host = Holding::of(files.as_mut());

    let warm = serve_one(
        &mut exec,
        host,
        &serve,
        b"static/asset.css",
        b"br;q=1, gzip;q=0.8",
    );
    assert_eq!(warm.body(), b"BR-BYTES");
    assert!(wire(&warm).contains("content-encoding: br"));

    fx.remove("static/asset.css.br");
    fx.remove("static/asset.css.gz");
    fx.remove("static/asset.css");

    let br = serve_one(
        &mut exec,
        host,
        &serve,
        b"static/asset.css",
        b"br;q=1, gzip;q=0.8",
    );
    assert_eq!(br.body(), b"BR-BYTES");
    assert!(wire(&br).contains("content-encoding: br"));
}

#[test]
fn async_cold_miss_nonexistent_is_404() {
    let fx = Fixture::new("async_404");
    let serve = ServeDir::new(&fx.root);

    let mut exec = Executor::new(cfg()).unwrap();
    let mut files: Pin<Box<Files<ID, SLOTS>>> = Box::pin(Files::new());
    let host = Holding::of(files.as_mut());

    let resp = serve_one(&mut exec, host, &serve, b"static/missing.txt", b"");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[test]
fn async_cache_bounds_total_bytes() {
    let fx = Fixture::new("async_lru");
    let big = vec![b'x'; 4096];
    for i in 0..8 {
        fx.write(&format!("static/f{i}.bin"), &big);
    }
    let serve = ServeDir::new(&fx.root)
        .cache_capacity(4096 * 3)
        .cache_valid(Duration::from_secs(3600));

    let mut exec = Executor::new(cfg()).unwrap();
    let mut files: Pin<Box<Files<ID, SLOTS>>> = Box::pin(Files::new());
    let host = Holding::of(files.as_mut());

    for i in 0..8 {
        let path = format!("static/f{i}.bin");
        let resp = serve_one(&mut exec, host, &serve, path.as_bytes(), b"");
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body().len(), 4096);
    }

    for i in 0..8 {
        fx.remove(&format!("static/f{i}.bin"));
    }

    let recent = serve_one(&mut exec, host, &serve, b"static/f7.bin", b"");
    assert_eq!(recent.status(), StatusCode::OK);

    let evicted = serve_one(&mut exec, host, &serve, b"static/f0.bin", b"");
    assert_eq!(evicted.status(), StatusCode::NOT_FOUND);
}
