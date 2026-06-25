use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use sark::fs::ServeDir;
use sark_core::http::{Response, StatusCode};

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let root =
            std::env::temp_dir().join(format!("sark_serve_dir_{}_{}", std::process::id(), name));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("static")).unwrap();
        Self { root }
    }

    fn write(&self, rel: &str, bytes: &[u8]) {
        let path = self.root.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }

    fn serve(&self) -> ServeDir {
        ServeDir::new(&self.root)
    }

    fn remove(&self, rel: &str) {
        let _ = fs::remove_file(self.root.join(rel));
    }

    fn touch_grow(&self, rel: &str, bytes: &[u8]) {
        let path = self.root.join(rel);
        fs::write(&path, bytes).unwrap();
        let later = std::time::SystemTime::now() + Duration::from_secs(10);
        let f = fs::OpenOptions::new().write(true).open(&path).unwrap();
        let _ = f.set_modified(later);
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
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
fn mime_table_exact_values() {
    let fx = Fixture::new("mime");
    fx.write("static/reset.css", b"body{}");
    fx.write("static/app.js", b"console.log(1)");
    fx.write("static/manifest.json", b"{}");
    fx.write("static/icon.svg", b"<svg/>");
    fx.write("static/blob.bin", b"\0\0");
    let serve = fx.serve();

    assert_eq!(
        content_type(&serve.serve(b"static/reset.css", b"")),
        "text/css"
    );
    assert_eq!(
        content_type(&serve.serve(b"static/app.js", b"")),
        "application/javascript"
    );
    assert_eq!(
        content_type(&serve.serve(b"static/manifest.json", b"")),
        "application/json"
    );
    assert_eq!(
        content_type(&serve.serve(b"static/icon.svg", b"")),
        "image/svg+xml"
    );
    assert_eq!(
        content_type(&serve.serve(b"static/blob.bin", b"")),
        "application/octet-stream"
    );
}

#[test]
fn identity_body_matches_disk_exactly() {
    let fx = Fixture::new("identity");
    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    fx.write("static/data.txt", &payload);
    let serve = fx.serve();

    let resp = serve.serve(b"static/data.txt", b"");
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.body(), payload.as_slice());
    assert!(!wire(&resp).contains("content-encoding"));
}

#[test]
fn empty_file_serves_empty_body() {
    let fx = Fixture::new("empty");
    fx.write("static/empty.txt", b"");
    let resp = fx.serve().serve(b"static/empty.txt", b"");
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.body().len(), 0);
}

#[test]
fn precompressed_br_preferred_then_gzip() {
    let fx = Fixture::new("precompress");
    fx.write("static/app.css", b"the original uncompressed bytes");
    fx.write("static/app.css.br", b"BR-BYTES");
    fx.write("static/app.css.gz", b"GZIP-BYTES-LONGER");
    let serve = fx.serve().precompressed_br().precompressed_gzip();

    let resp = serve.serve(b"static/app.css", b"br;q=1, gzip;q=0.8");
    assert_eq!(content_type(&resp), "text/css");
    assert_eq!(resp.body(), b"BR-BYTES");
    let w = wire(&resp);
    assert!(w.contains("content-encoding: br"));
    assert!(w.contains("vary: accept-encoding"));

    let resp = serve.serve(b"static/app.css", b"gzip");
    assert_eq!(content_type(&resp), "text/css");
    assert_eq!(resp.body(), b"GZIP-BYTES-LONGER");
    assert!(wire(&resp).contains("content-encoding: gzip"));
}

#[test]
fn higher_q_overrides_default_priority() {
    let fx = Fixture::new("qvalues");
    fx.write("static/app.css", b"original");
    fx.write("static/app.css.br", b"BR");
    fx.write("static/app.css.gz", b"GZIP");
    let serve = fx.serve().precompressed_br().precompressed_gzip();

    let resp = serve.serve(b"static/app.css", b"br;q=0.5, gzip;q=0.9");
    assert!(wire(&resp).contains("content-encoding: gzip"));

    let resp = serve.serve(b"static/app.css", b"br;q=0, gzip");
    assert!(wire(&resp).contains("content-encoding: gzip"));
}

#[test]
fn no_accept_encoding_serves_identity_even_with_siblings() {
    let fx = Fixture::new("identity_siblings");
    fx.write("static/app.css", b"original-bytes");
    fx.write("static/app.css.br", b"BR");
    let serve = fx.serve().precompressed_br().precompressed_gzip();

    let resp = serve.serve(b"static/app.css", b"");
    assert_eq!(resp.body(), b"original-bytes");
    assert!(!wire(&resp).contains("content-encoding"));
}

#[test]
fn option_off_ignores_siblings() {
    let fx = Fixture::new("option_off");
    fx.write("static/app.css", b"original-bytes");
    fx.write("static/app.css.br", b"BR");
    let serve = fx.serve();

    let resp = serve.serve(b"static/app.css", b"br, gzip");
    assert_eq!(resp.body(), b"original-bytes");
    assert!(!wire(&resp).contains("content-encoding"));
}

#[test]
fn missing_sibling_falls_back_to_identity() {
    let fx = Fixture::new("missing_sibling");
    fx.write("static/app.css", b"original-bytes");
    let serve = fx.serve().precompressed_br().precompressed_gzip();

    let resp = serve.serve(b"static/app.css", b"br, gzip");
    assert_eq!(resp.body(), b"original-bytes");
    assert!(!wire(&resp).contains("content-encoding"));
}

#[test]
fn missing_br_falls_back_to_gzip_sibling() {
    let fx = Fixture::new("br_to_gzip");
    fx.write("static/app.css", b"original-bytes");
    fx.write("static/app.css.gz", b"GZIP");
    let serve = fx.serve().precompressed_br().precompressed_gzip();

    let resp = serve.serve(b"static/app.css", b"br;q=1, gzip;q=0.8");
    assert_eq!(resp.body(), b"GZIP");
    assert!(wire(&resp).contains("content-encoding: gzip"));
}

#[test]
fn nonexistent_is_404() {
    let fx = Fixture::new("nonexistent");
    let resp = fx.serve().serve(b"static/nonexistent.txt", b"");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[test]
fn traversal_attempts_are_rejected() {
    let fx = Fixture::new("traversal");
    fx.write("static/ok.txt", b"ok");
    let serve = fx.serve();

    for attack in [
        &b"../Cargo.toml"[..],
        b"static/../../etc/passwd",
        b"/etc/passwd",
        b"static\\..\\secret",
        b"static/..%2fsecret",
    ] {
        assert_eq!(
            serve.serve(attack, b"").status(),
            StatusCode::NOT_FOUND,
            "expected 404 for {:?}",
            String::from_utf8_lossy(attack)
        );
    }

    let mut nul = b"static/ok".to_vec();
    nul.push(0);
    nul.extend_from_slice(b".txt");
    assert_eq!(serve.serve(&nul, b"").status(), StatusCode::NOT_FOUND);

    assert_eq!(serve.serve(b"static/ok.txt", b"").status(), StatusCode::OK);
}

#[test]
fn cache_hit_serves_after_delete_within_window() {
    let fx = Fixture::new("cache_hit_delete");
    fx.write("static/cached.css", b"ORIGINAL-CSS-BYTES");
    let serve = fx.serve().cache_valid(Duration::from_secs(3600));

    let first = serve.serve(b"static/cached.css", b"");
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.body(), b"ORIGINAL-CSS-BYTES");

    fx.remove("static/cached.css");

    let second = serve.serve(b"static/cached.css", b"");
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(second.body(), b"ORIGINAL-CSS-BYTES");
    assert_eq!(content_type(&second), "text/css");
}

#[test]
fn cache_reloads_after_window_when_mtime_changes() {
    let fx = Fixture::new("cache_reload");
    fx.write("static/v.txt", b"v1");
    let serve = fx.serve().cache_valid(Duration::ZERO);

    let first = serve.serve(b"static/v.txt", b"");
    assert_eq!(first.body(), b"v1");

    fx.touch_grow("static/v.txt", b"version-two-longer");

    let second = serve.serve(b"static/v.txt", b"");
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(second.body(), b"version-two-longer");
}

#[test]
fn cache_unchanged_after_window_still_served() {
    let fx = Fixture::new("cache_unchanged");
    fx.write("static/stable.txt", b"stable-bytes");
    let serve = fx.serve().cache_valid(Duration::ZERO);

    assert_eq!(
        serve.serve(b"static/stable.txt", b"").body(),
        b"stable-bytes"
    );
    assert_eq!(
        serve.serve(b"static/stable.txt", b"").body(),
        b"stable-bytes"
    );
    assert_eq!(
        serve.serve(b"static/stable.txt", b"").body(),
        b"stable-bytes"
    );
}

#[test]
fn cache_revalidate_detects_deletion() {
    let fx = Fixture::new("cache_revalidate_gone");
    fx.write("static/gone.txt", b"here");
    let serve = fx.serve().cache_valid(Duration::ZERO);

    assert_eq!(serve.serve(b"static/gone.txt", b"").body(), b"here");
    fx.remove("static/gone.txt");
    assert_eq!(
        serve.serve(b"static/gone.txt", b"").status(),
        StatusCode::NOT_FOUND
    );
}

#[test]
fn cache_precompressed_variant_served_from_cache() {
    let fx = Fixture::new("cache_precompress");
    fx.write("static/asset.css", b"original-uncompressed");
    fx.write("static/asset.css.br", b"BR-BYTES");
    fx.write("static/asset.css.gz", b"GZIP-BYTES");
    let serve = fx
        .serve()
        .precompressed_br()
        .precompressed_gzip()
        .cache_valid(Duration::from_secs(3600));

    let warm = serve.serve(b"static/asset.css", b"br;q=1, gzip;q=0.8");
    assert_eq!(warm.body(), b"BR-BYTES");

    fx.remove("static/asset.css.br");
    fx.remove("static/asset.css.gz");
    fx.remove("static/asset.css");

    let br = serve.serve(b"static/asset.css", b"br;q=1, gzip;q=0.8");
    assert_eq!(br.body(), b"BR-BYTES");
    assert!(wire(&br).contains("content-encoding: br"));
    assert_eq!(content_type(&br), "text/css");

    let gz = serve.serve(b"static/asset.css", b"gzip");
    assert_eq!(gz.body(), b"GZIP-BYTES");
    assert!(wire(&gz).contains("content-encoding: gzip"));

    let identity = serve.serve(b"static/asset.css", b"");
    assert_eq!(identity.body(), b"original-uncompressed");
    assert!(!wire(&identity).contains("content-encoding"));
}

#[test]
fn cache_lru_eviction_bounds_total_bytes() {
    let fx = Fixture::new("cache_lru");
    let big = vec![b'x'; 4096];
    for i in 0..8 {
        fx.write(&format!("static/f{i}.bin"), &big);
    }
    let serve = fx
        .serve()
        .cache_capacity(4096 * 3)
        .cache_valid(Duration::from_secs(3600));

    for i in 0..8 {
        let path = format!("static/f{i}.bin");
        let resp = serve.serve(path.as_bytes(), b"");
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body().len(), 4096);
    }

    for i in 0..8 {
        fx.remove(&format!("static/f{i}.bin"));
    }

    let recent = serve.serve(b"static/f7.bin", b"");
    assert_eq!(recent.status(), StatusCode::OK);
    assert_eq!(recent.body().len(), 4096);

    let evicted = serve.serve(b"static/f0.bin", b"");
    assert_eq!(evicted.status(), StatusCode::NOT_FOUND);
}

#[test]
fn cache_disabled_when_capacity_zero() {
    let fx = Fixture::new("cache_disabled");
    fx.write("static/d.txt", b"first");
    let serve = fx.serve().cache_capacity(0);

    assert_eq!(serve.serve(b"static/d.txt", b"").body(), b"first");
    fx.remove("static/d.txt");
    assert_eq!(
        serve.serve(b"static/d.txt", b"").status(),
        StatusCode::NOT_FOUND
    );
}

#[test]
fn cache_oversized_entry_not_stored() {
    let fx = Fixture::new("cache_oversize");
    let big = vec![b'z'; 8192];
    fx.write("static/huge.bin", &big);
    let serve = fx
        .serve()
        .cache_capacity(1024)
        .cache_valid(Duration::from_secs(3600));

    let first = serve.serve(b"static/huge.bin", b"");
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.body().len(), 8192);

    fx.remove("static/huge.bin");
    assert_eq!(
        serve.serve(b"static/huge.bin", b"").status(),
        StatusCode::NOT_FOUND
    );
}
