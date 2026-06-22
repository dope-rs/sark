use std::fs;
use std::path::PathBuf;

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
