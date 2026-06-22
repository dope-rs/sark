//! Per-request disk static handler (no cache/preload/mmap). Extension maps to
//! `Content-Type`. `precompressed_br` / `precompressed_gzip` serve sibling
//! `<file>.br` / `<file>.gz` when `Accept-Encoding` allows, sending the compressed
//! bytes with `Content-Encoding` + `Vary` and the original file's `Content-Type`.
//!
//! ```no_run
//! let serve = sark::fs::ServeDir::new("public")
//!     .precompressed_br()
//!     .precompressed_gzip();
//! let resp = serve.serve(b"static/app.css", b"br, gzip");
//! ```

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use o3::buffer::Owned;
use sark_core::http::Response;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Br,
    Gzip,
}

impl Encoding {
    fn token(self) -> &'static [u8] {
        match self {
            Encoding::Br => b"br",
            Encoding::Gzip => b"gzip",
        }
    }

    fn header(self) -> &'static str {
        match self {
            Encoding::Br => "br",
            Encoding::Gzip => "gzip",
        }
    }

    fn suffix(self) -> &'static str {
        match self {
            Encoding::Br => ".br",
            Encoding::Gzip => ".gz",
        }
    }
}

#[derive(Clone)]
pub struct ServeDir {
    root: PathBuf,
    precompressed_br: bool,
    precompressed_gzip: bool,
}

impl ServeDir {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            precompressed_br: false,
            precompressed_gzip: false,
        }
    }

    pub fn precompressed_br(mut self) -> Self {
        self.precompressed_br = true;
        self
    }

    pub fn precompressed_gzip(mut self) -> Self {
        self.precompressed_gzip = true;
        self
    }

    pub fn serve(&self, rel_path: &[u8], accept_encoding: &[u8]) -> Response {
        let Some(base) = self.resolve(rel_path) else {
            return Response::not_found();
        };
        let mime = Self::content_type(&base);

        if let Some((encoding, path)) = self.negotiate(&base, accept_encoding)
            && let Some(body) = Self::read_into_body(&path)
        {
            let mut response = Response::ok();
            response.content_type(mime);
            response.append_wire_header_static("content-encoding", encoding.header());
            response.append_wire_header_static("vary", "accept-encoding");
            response.set_body(body);
            return response;
        }

        match Self::read_into_body(&base) {
            Some(body) => {
                let mut response = Response::ok();
                response.content_type(mime);
                response.set_body(body);
                response
            }
            None => Response::not_found(),
        }
    }

    fn resolve(&self, rel: &[u8]) -> Option<PathBuf> {
        if rel.is_empty() || rel.iter().any(|&b| b == 0 || b == b'\\') {
            return None;
        }
        let rel = std::str::from_utf8(rel).ok()?;
        if rel.starts_with('/') {
            return None;
        }
        let mut path = self.root.clone();
        for segment in rel.split('/') {
            if segment.is_empty() || segment == "." {
                continue;
            }
            if segment == ".." {
                return None;
            }
            path.push(segment);
        }
        if path == self.root {
            return None;
        }
        Some(path)
    }

    fn negotiate(&self, base: &Path, accept_encoding: &[u8]) -> Option<(Encoding, PathBuf)> {
        let br = self
            .precompressed_br
            .then(|| Self::quality(accept_encoding, Encoding::Br.token()))
            .flatten();
        let gzip = self
            .precompressed_gzip
            .then(|| Self::quality(accept_encoding, Encoding::Gzip.token()))
            .flatten();

        let mut order: Vec<Encoding> = Vec::with_capacity(2);
        match (br, gzip) {
            (Some(b), Some(g)) if g > b => {
                order.push(Encoding::Gzip);
                order.push(Encoding::Br);
            }
            (Some(_), Some(_)) => {
                order.push(Encoding::Br);
                order.push(Encoding::Gzip);
            }
            (Some(_), None) => order.push(Encoding::Br),
            (None, Some(_)) => order.push(Encoding::Gzip),
            (None, None) => return None,
        }

        for encoding in order {
            let path = Self::sibling(base, encoding);
            if path.is_file() {
                return Some((encoding, path));
            }
        }
        None
    }

    fn sibling(base: &Path, encoding: Encoding) -> PathBuf {
        let mut name = base.as_os_str().to_owned();
        name.push(encoding.suffix());
        PathBuf::from(name)
    }

    fn quality(accept_encoding: &[u8], token: &[u8]) -> Option<u32> {
        let mut direct: Option<u32> = None;
        let mut star: Option<u32> = None;
        for entry in accept_encoding.split(|&b| b == b',') {
            let entry = entry.trim_ascii();
            let mut parts = entry.split(|&b| b == b';');
            let coding = parts.next().unwrap_or(b"").trim_ascii();
            let mut q = 1000u32;
            for param in parts {
                let param = param.trim_ascii();
                if param.len() >= 2 && param[0].eq_ignore_ascii_case(&b'q') && param[1] == b'=' {
                    q = Self::parse_q(&param[2..]);
                }
            }
            if coding.eq_ignore_ascii_case(token) {
                direct = Some(q);
            } else if coding == b"*" {
                star = Some(q);
            }
        }
        match direct.or(star)? {
            0 => None,
            q => Some(q),
        }
    }

    fn parse_q(value: &[u8]) -> u32 {
        let value = value.trim_ascii();
        let mut parts = value.splitn(2, |&b| b == b'.');
        let integer = parts.next().unwrap_or(b"");
        if integer == b"1" {
            return 1000;
        }
        let mut q = 0u32;
        let mut scale = 100u32;
        for &b in parts.next().unwrap_or(b"").iter().take(3) {
            if !b.is_ascii_digit() {
                break;
            }
            q += (b - b'0') as u32 * scale;
            scale /= 10;
        }
        q
    }

    fn content_type(base: &Path) -> &'static str {
        let Some(ext) = base.extension().and_then(|e| e.to_str()) else {
            return "application/octet-stream";
        };
        match ext.to_ascii_lowercase().as_str() {
            "css" => "text/css",
            "js" => "application/javascript",
            "html" => "text/html; charset=UTF-8",
            "json" => "application/json",
            "svg" => "image/svg+xml",
            "woff2" => "font/woff2",
            "woff" => "font/woff",
            "webp" => "image/webp",
            "png" => "image/png",
            "jpg" | "jpeg" => "image/jpeg",
            "txt" => "text/plain",
            _ => "application/octet-stream",
        }
    }

    fn read_into_body(path: &Path) -> Option<Owned> {
        let mut file = File::open(path).ok()?;
        let meta = file.metadata().ok()?;
        if !meta.is_file() {
            return None;
        }
        let len = meta.len() as usize;
        let mut body = Owned::with_capacity(len);
        let spare = body.spare_capacity_mut();
        // SAFETY: `spare` covers `len` reserved bytes; we write into them via read_exact before reading.
        let slot = unsafe { std::slice::from_raw_parts_mut(spare.as_mut_ptr().cast::<u8>(), len) };
        file.read_exact(slot).ok()?;
        // SAFETY: read_exact filled exactly `len` bytes, so the prefix is initialized.
        unsafe { body.set_len(len) };
        Some(body)
    }
}
