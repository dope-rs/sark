//! Canonical path validation and asset sibling resolution.

use std::path::{Path, PathBuf};

use super::encoding::Encoding;

pub(super) struct Resolver<'a> {
    root: &'a Path,
}

impl<'a> Resolver<'a> {
    pub(super) fn new(root: &'a Path) -> Self {
        Self { root }
    }

    pub(super) fn relative(bytes: &[u8]) -> Option<&str> {
        if bytes.is_empty() || bytes.iter().any(|&byte| byte == 0 || byte == b'\\') {
            return None;
        }
        let relative = std::str::from_utf8(bytes).ok()?;
        if relative.starts_with('/') {
            return None;
        }
        let mut found = false;
        for segment in relative.split('/') {
            if segment.is_empty() || segment == "." {
                continue;
            }
            if segment == ".." {
                return None;
            }
            found = true;
        }
        found.then_some(relative)
    }

    pub(super) fn resolve(&self, relative: &str) -> PathBuf {
        let mut path = self.root.to_path_buf();
        for segment in relative.split('/') {
            if !segment.is_empty() && segment != "." {
                path.push(segment);
            }
        }
        path
    }

    pub(super) fn sibling(base: &Path, encoding: Encoding) -> PathBuf {
        let mut name = base.as_os_str().to_owned();
        name.push(encoding.suffix());
        PathBuf::from(name)
    }

    pub(super) fn content_type(path: &Path) -> &'static str {
        let Some(extension) = path.extension().and_then(|extension| extension.to_str()) else {
            return "application/octet-stream";
        };
        match extension.to_ascii_lowercase().as_str() {
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
}
