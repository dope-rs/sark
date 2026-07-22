use std::fs;
use std::path::PathBuf;
use std::pin::{Pin, pin};
use std::time::Duration;

use dope::manifold::file::{FileManifold, Files};
use dope::runtime::{Executor, Session};
use dope_fiber::{Fiber, SessionExt as _};
use o3::cell::BrandCell as Branded;
use sark::fs::ServeDir;
use sark_core::http::{Response, StatusCode};

const ID: u8 = 7;
const SLOTS: usize = 64;

fn hash_state() -> dope::hash::State {
    dope::hash::Seed::new([1, 2]).state()
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Host<'d, 'scope> {
    #[pin]
    #[manifold]
    files: FileManifold<'scope, 'd, ID, SLOTS>,
}

type FileSession<'scope, 'd> = Session<'scope, 'd, Files<'d, ID, SLOTS>>;

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

fn run<'scope, 'd, F, T>(
    sess: &mut FileSession<'scope, 'd>,
    host: Pin<&Branded<'d, Host<'d, 'scope>>>,
    fut: F,
) -> T
where
    F: Fiber<'d, Output = T>,
{
    sess.block_on(host, fut).expect("runtime")
}

fn cfg() -> dope::driver::Config {
    dope::driver::Config::for_profile::<dope::runtime::profile::Throughput>()
}

fn enter<R>(
    f: impl for<'scope, 'd> FnOnce(
        &mut FileSession<'scope, 'd>,
        Pin<&Branded<'d, Host<'d, 'scope>>>,
        &'scope Files<'d, ID, SLOTS>,
    ) -> R,
) -> R {
    Executor::new(cfg())
        .unwrap()
        .with_storage_factory(Files::<ID, SLOTS>::factory())
        .enter(|mut sess| {
            let files = sess.storage();
            let manifold = pin!(Branded::new(Host {
                files: files.manifold(),
            }));
            f(&mut sess, manifold.as_ref(), files)
        })
}

fn serve_one<'scope, 'd>(
    sess: &mut FileSession<'scope, 'd>,
    host: Pin<&Branded<'d, Host<'d, 'scope>>>,
    files: &'scope Files<'d, ID, SLOTS>,
    serve: &ServeDir,
    rel: &[u8],
    accept_encoding: &[u8],
) -> Response {
    run(sess, host, serve.serve_async(files, rel, accept_encoding))
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
    let serve = ServeDir::new(&fx.root, hash_state()).cache_valid(Duration::from_secs(3600));

    enter(|sess, host, files| {
        let resp = serve_one(sess, host, files, &serve, b"static/data.bin", b"");

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), payload.as_slice());
        assert_eq!(content_type(&resp), "application/octet-stream");
    });
}

#[test]
fn warm_hit_serves_after_delete_with_zero_file_ops() {
    let fx = Fixture::new("warm_hit");
    fx.write("static/cached.css", b"ORIGINAL-CSS-BYTES");
    let serve = ServeDir::new(&fx.root, hash_state()).cache_valid(Duration::from_secs(3600));

    enter(|sess, host, files| {
        let first = serve_one(sess, host, files, &serve, b"static/cached.css", b"");
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(first.body(), b"ORIGINAL-CSS-BYTES");

        fx.remove("static/cached.css");

        let second = serve_one(sess, host, files, &serve, b"static/cached.css", b"");
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(second.body(), b"ORIGINAL-CSS-BYTES");
        assert_eq!(content_type(&second), "text/css");
    });
}

#[test]
fn stale_cache_revalidates_through_async_metadata() {
    let fx = Fixture::new("stale_async_stat");
    fx.write("static/cached.txt", b"old");
    let serve = ServeDir::new(&fx.root, hash_state()).cache_valid(Duration::ZERO);

    enter(|sess, host, files| {
        let first = serve_one(sess, host, files, &serve, b"static/cached.txt", b"");
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(first.body(), b"old");

        fx.write("static/cached.txt", b"new-and-longer");

        let second = serve_one(sess, host, files, &serve, b"static/cached.txt", b"");
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(second.body(), b"new-and-longer");
    });
}

#[test]
fn async_precompressed_variant_then_cached() {
    let fx = Fixture::new("async_precompress");
    fx.write("static/asset.css", b"original-uncompressed");
    fx.write("static/asset.css.br", b"BR-BYTES");
    fx.write("static/asset.css.gz", b"GZIP-BYTES");
    let serve = ServeDir::new(&fx.root, hash_state())
        .precompressed_br()
        .precompressed_gzip()
        .cache_valid(Duration::from_secs(3600));

    enter(|sess, host, files| {
        let warm = serve_one(
            sess,
            host,
            files,
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
            sess,
            host,
            files,
            &serve,
            b"static/asset.css",
            b"br;q=1, gzip;q=0.8",
        );
        assert_eq!(br.body(), b"BR-BYTES");
        assert!(wire(&br).contains("content-encoding: br"));
    });
}

#[test]
fn async_cold_miss_nonexistent_is_404() {
    let fx = Fixture::new("async_404");
    let serve = ServeDir::new(&fx.root, hash_state());

    enter(|sess, host, files| {
        let resp = serve_one(sess, host, files, &serve, b"static/missing.txt", b"");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    });
}

#[test]
fn async_cache_bounds_total_bytes() {
    let fx = Fixture::new("async_lru");
    let big = vec![b'x'; 4096];
    for i in 0..8 {
        fx.write(&format!("static/f{i}.bin"), &big);
    }
    let serve = ServeDir::new(&fx.root, hash_state())
        .cache_capacity(4096 * 3)
        .cache_valid(Duration::from_secs(3600));

    enter(|sess, host, files| {
        for i in 0..8 {
            let path = format!("static/f{i}.bin");
            let resp = serve_one(sess, host, files, &serve, path.as_bytes(), b"");
            assert_eq!(resp.status(), StatusCode::OK);
            assert_eq!(resp.body().len(), 4096);
        }

        for i in 0..8 {
            fx.remove(&format!("static/f{i}.bin"));
        }

        let recent = serve_one(sess, host, files, &serve, b"static/f7.bin", b"");
        assert_eq!(recent.status(), StatusCode::OK);

        let evicted = serve_one(sess, host, files, &serve, b"static/f0.bin", b"");
        assert_eq!(evicted.status(), StatusCode::NOT_FOUND);
    });
}

#[test]
fn async_read_rejects_files_over_configured_limit() {
    let fx = Fixture::new("max_file_bytes");
    fx.write("static/large.bin", &[b'x'; 8192]);
    let serve = ServeDir::new(&fx.root, hash_state()).max_file_bytes(4096);

    enter(|sess, host, files| {
        let response = serve_one(sess, host, files, &serve, b"static/large.bin", b"");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    });
}

#[test]
fn concurrent_async_reads_share_the_global_byte_budget() {
    let fx = Fixture::new("shared_byte_budget");
    fx.write("static/first.bin", &[b'a'; 4096]);
    fx.write("static/second.bin", &[b'b'; 4096]);
    let serve = ServeDir::new(&fx.root, hash_state())
        .cache_capacity(0)
        .max_file_bytes(8192)
        .read_budget(4096);

    enter(|sess, host, files| {
        let batch = dope_fiber::Batch::from_array([
            serve.serve_async(files, b"static/first.bin", b""),
            serve.serve_async(files, b"static/second.bin", b""),
        ]);
        let responses = run(sess, host, batch).collect::<Vec<_>>();
        let successes = responses
            .iter()
            .filter(|response| response.status() == StatusCode::OK)
            .count();
        let rejected = responses
            .iter()
            .filter(|response| response.status() == StatusCode::SERVICE_UNAVAILABLE)
            .count();
        assert_eq!(successes, 1);
        assert_eq!(rejected, 1);
    });
}

#[test]
fn same_path_async_reads_share_one_budget_lease() {
    let fx = Fixture::new("same_path_singleflight");
    fx.write("static/shared.bin", &[b'a'; 4096]);
    let serve = ServeDir::new(&fx.root, hash_state())
        .cache_capacity(0)
        .max_file_bytes(8192)
        .read_budget(4096);

    enter(|sess, host, files| {
        let batch = dope_fiber::Batch::from_array([
            serve.serve_async(files, b"static/shared.bin", b""),
            serve.serve_async(files, b"static/shared.bin", b""),
        ]);
        let responses = run(sess, host, batch).collect::<Vec<_>>();
        assert_eq!(responses.len(), 2);
        assert!(
            responses
                .iter()
                .all(|response| response.status() == StatusCode::OK)
        );
        assert!(
            responses
                .iter()
                .all(|response| response.body() == [b'a'; 4096])
        );
    });
}

#[test]
fn concurrent_tiny_misses_use_only_the_read_budget() {
    let fx = Fixture::new("scratch_capacity");
    for index in 0..9 {
        fx.write(&format!("static/{index}.bin"), b"x");
    }
    let serve = ServeDir::new(&fx.root, hash_state())
        .cache_capacity(0)
        .max_file_bytes(1)
        .read_budget(9);

    enter(|sess, host, files| {
        let batch = dope_fiber::Batch::from_array([
            serve.serve_async(files, b"static/0.bin", b""),
            serve.serve_async(files, b"static/1.bin", b""),
            serve.serve_async(files, b"static/2.bin", b""),
            serve.serve_async(files, b"static/3.bin", b""),
            serve.serve_async(files, b"static/4.bin", b""),
            serve.serve_async(files, b"static/5.bin", b""),
            serve.serve_async(files, b"static/6.bin", b""),
            serve.serve_async(files, b"static/7.bin", b""),
            serve.serve_async(files, b"static/8.bin", b""),
        ]);
        let responses = run(sess, host, batch).collect::<Vec<_>>();
        let successes = responses
            .iter()
            .filter(|response| response.status() == StatusCode::OK)
            .count();
        let overloaded = responses
            .iter()
            .filter(|response| response.status() == StatusCode::SERVICE_UNAVAILABLE)
            .count();
        assert_eq!(successes, 9);
        assert_eq!(overloaded, 0);
    });
}
