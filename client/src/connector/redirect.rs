use std::collections::HashSet;

use http::{Method, Uri};

use crate::connector::error::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OriginRelation {
    Same,
    Cross,
}

pub(super) struct Resolve;

impl Resolve {
    pub(super) fn origin(a: &Uri, b: &Uri) -> OriginRelation {
        if a.scheme() == b.scheme() && a.host() == b.host() && a.port_u16() == b.port_u16() {
            OriginRelation::Same
        } else {
            OriginRelation::Cross
        }
    }

    pub(super) fn redirect(base: &Uri, location: &str) -> Result<Uri, Error> {
        if location.starts_with("http://") || location.starts_with("https://") {
            return location
                .parse()
                .map_err(|e| Error::Http(format!("invalid redirect URL: {e}")));
        }

        let scheme = base.scheme_str().unwrap_or("http");
        let authority = base
            .authority()
            .ok_or_else(|| Error::Http("missing authority in base URI".into()))?;

        let (raw_path, suffix) = Self::split_location(location);
        let base_prefix = if raw_path.starts_with('/') {
            String::new()
        } else {
            let base_path = base.path();
            match base_path.rfind('/') {
                Some(i) => base_path[..=i].to_string(),
                None => "/".to_string(),
            }
        };
        let normalized_path = Self::normalize_path(&(base_prefix + raw_path));
        let path = format!("{normalized_path}{suffix}");

        format!("{scheme}://{authority}{path}")
            .parse()
            .map_err(|e| Error::Http(format!("invalid redirect URL: {e}")))
    }

    fn split_location(location: &str) -> (&str, &str) {
        match location.find(['?', '#']) {
            Some(i) => (&location[..i], &location[i..]),
            None => (location, ""),
        }
    }

    fn normalize_path(input: &str) -> String {
        let absolute = input.starts_with('/');
        let mut segments: Vec<&str> = Vec::new();
        for seg in input.split('/') {
            if seg.is_empty() || seg == "." {
                continue;
            }
            if seg == ".." {
                segments.pop();
                continue;
            }
            segments.push(seg);
        }

        let mut out = String::new();
        if absolute {
            out.push('/');
        }
        out.push_str(&segments.join("/"));
        if out.is_empty() {
            out.push('/');
        }
        out
    }
}

pub(super) struct RedirectState {
    remaining: u32,
    base: Uri,
    current: Uri,
    visited: HashSet<Uri>,
}

impl RedirectState {
    pub(super) fn new(max_redirects: u32, base: Uri, first_path: &str) -> Self {
        let current = Self::join_path(&base, first_path);
        let mut visited = HashSet::new();
        visited.insert(current.clone());
        Self {
            remaining: max_redirects,
            base,
            current,
            visited,
        }
    }

    fn join_path(base: &Uri, path: &str) -> Uri {
        let scheme = base.scheme_str().unwrap_or("http");
        let authority = match base.authority() {
            Some(a) => a.as_str().to_string(),
            None => String::new(),
        };
        let p = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        };
        format!("{scheme}://{authority}{p}")
            .parse()
            .unwrap_or_else(|_| base.clone())
    }

    pub(super) fn path_and_query(&self) -> String {
        match self.current.path_and_query() {
            Some(pq) => pq.as_str().to_string(),
            None => self.current.path().to_string(),
        }
    }

    pub(super) fn advance(
        &mut self,
        status: u16,
        location: &str,
        method: &Method,
    ) -> Result<Method, Error> {
        if self.remaining == 0 {
            return Err(Error::Http("redirect limit exceeded".into()));
        }
        let new_uri = Resolve::redirect(&self.current, location)?;
        if Resolve::origin(&self.base, &new_uri) == OriginRelation::Cross {
            return Err(Error::Http(format!(
                "cross-host redirect to {new_uri} is not supported"
            )));
        }
        if !self.visited.insert(new_uri.clone()) {
            return Err(Error::Http("redirect loop detected".into()));
        }
        self.current = new_uri;
        self.remaining -= 1;
        let next_method = if matches!(status, 307 | 308) {
            method.clone()
        } else {
            Method::GET
        };
        Ok(next_method)
    }
}

#[cfg(test)]
mod tests {
    use super::{OriginRelation, RedirectState, Resolve};

    #[test]
    fn redirect_absolute_url() {
        let base: http::Uri = "http://example.com/old".parse().unwrap();
        let result = Resolve::redirect(&base, "http://other.com/new").unwrap();
        assert_eq!(result.to_string(), "http://other.com/new");
    }

    #[test]
    fn redirect_absolute_path() {
        let base: http::Uri = "http://example.com/old/page".parse().unwrap();
        let result = Resolve::redirect(&base, "/new/page").unwrap();
        assert_eq!(result.to_string(), "http://example.com/new/page");
    }

    #[test]
    fn redirect_relative_path() {
        let base: http::Uri = "http://example.com/dir/page".parse().unwrap();
        let result = Resolve::redirect(&base, "other").unwrap();
        assert_eq!(result.to_string(), "http://example.com/dir/other");
    }

    #[test]
    fn redirect_preserves_https() {
        let base: http::Uri = "https://example.com/page".parse().unwrap();
        let result = Resolve::redirect(&base, "/new").unwrap();
        assert_eq!(result.to_string(), "https://example.com/new");
    }

    #[test]
    fn redirect_preserves_port() {
        let base: http::Uri = "http://example.com:8080/page".parse().unwrap();
        let result = Resolve::redirect(&base, "/new").unwrap();
        assert_eq!(result.to_string(), "http://example.com:8080/new");
    }

    #[test]
    fn redirect_normalizes_dot_segments() {
        let base: http::Uri = "http://example.com/dir/sub/page".parse().unwrap();
        let result = Resolve::redirect(&base, "../other/./file").unwrap();
        assert_eq!(result.to_string(), "http://example.com/dir/other/file");
    }

    #[test]
    fn redirect_preserves_query_and_fragment() {
        let base: http::Uri = "http://example.com/dir/page".parse().unwrap();
        let result = Resolve::redirect(&base, "next?x=1#frag").unwrap();
        assert_eq!(result.to_string(), "http://example.com/dir/next?x=1");
    }

    #[test]
    fn same_origin_detection() {
        let a: http::Uri = "http://example.com/a".parse().unwrap();
        let b: http::Uri = "http://example.com/b".parse().unwrap();
        let c: http::Uri = "http://other.com/a".parse().unwrap();
        assert_eq!(Resolve::origin(&a, &b), OriginRelation::Same);
        assert_eq!(Resolve::origin(&a, &c), OriginRelation::Cross);
    }

    #[test]
    fn same_origin_redirect_chain_advances_path() {
        let base: http::Uri = "http://h.example/".parse().unwrap();
        let mut st = RedirectState::new(3, base, "/start");
        let m = st.advance(302, "/next", &http::Method::POST).unwrap();
        assert_eq!(st.path_and_query(), "/next");
        assert_eq!(m, http::Method::GET);
        let m = st.advance(307, "final?q=1", &http::Method::POST).unwrap();
        assert_eq!(st.path_and_query(), "/final?q=1");
        assert_eq!(m, http::Method::POST);
    }

    #[test]
    fn redirect_loop_detected() {
        let base: http::Uri = "http://h.example/".parse().unwrap();
        let mut st = RedirectState::new(5, base, "/a");
        st.advance(302, "/b", &http::Method::GET).unwrap();
        let err = st.advance(302, "/a", &http::Method::GET).unwrap_err();
        assert!(err.to_string().contains("loop"));
    }

    #[test]
    fn cross_host_redirect_rejected() {
        let base: http::Uri = "http://h.example/".parse().unwrap();
        let mut st = RedirectState::new(5, base, "/a");
        let err = st
            .advance(302, "http://other.example/x", &http::Method::GET)
            .unwrap_err();
        assert!(err.to_string().contains("cross-host"));
    }

    #[test]
    fn redirect_limit_enforced() {
        let base: http::Uri = "http://h.example/".parse().unwrap();
        let mut st = RedirectState::new(1, base, "/a");
        st.advance(302, "/b", &http::Method::GET).unwrap();
        let err = st.advance(302, "/c", &http::Method::GET).unwrap_err();
        assert!(err.to_string().contains("limit"));
    }
}
