use http::{Method, Uri};

use crate::connector::error::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OriginRelation {
    Same,
    Cross,
}

fn origin_relation(a: &Uri, b: &Uri) -> OriginRelation {
    if a.scheme() == b.scheme() && a.host() == b.host() && a.port_u16() == b.port_u16() {
        OriginRelation::Same
    } else {
        OriginRelation::Cross
    }
}

fn resolve(base: &Uri, location: &str) -> Result<Uri, Error> {
    let location = location
        .split_once('#')
        .map_or(location, |(location, _)| location);
    if location.is_empty() {
        return Ok(base.clone());
    }
    if location
        .get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("http://"))
        || location
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
    {
        return location
            .parse()
            .map_err(|error| Error::Http(format!("invalid redirect URL: {error}")));
    }
    if location.starts_with("//") {
        let scheme = base
            .scheme_str()
            .ok_or_else(|| Error::Http("missing scheme in base URI".into()))?;
        return format!("{scheme}:{location}")
            .parse()
            .map_err(|error| Error::Http(format!("invalid redirect URL: {error}")));
    }

    let (raw_path, query) = split_location(location);
    let path = if raw_path.is_empty() {
        base.path().to_owned()
    } else if raw_path.starts_with('/') {
        normalize_path(raw_path)
    } else {
        let base_path = base.path();
        let prefix = match base_path.rfind('/') {
            Some(index) => base_path[..=index].to_owned(),
            None => "/".to_owned(),
        };
        normalize_path(&(prefix + raw_path))
    };
    let target = match query {
        Some(query) => format!("{path}?{query}"),
        None => path,
    };
    let mut parts = base.clone().into_parts();
    parts.path_and_query = Some(
        target
            .parse()
            .map_err(|error| Error::Http(format!("invalid redirect target: {error}")))?,
    );
    Uri::from_parts(parts).map_err(|error| Error::Http(format!("invalid redirect URL: {error}")))
}

fn split_location(location: &str) -> (&str, Option<&str>) {
    match location.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (location, None),
    }
}

fn normalize_path(input: &str) -> String {
    let absolute = input.starts_with('/');
    let trailing = input.ends_with('/') || input.ends_with("/.") || input.ends_with("/..");
    let mut segments = Vec::new();
    for segment in input.split('/') {
        if segment.is_empty() || segment == "." {
            continue;
        }
        if segment == ".." {
            segments.pop();
        } else {
            segments.push(segment);
        }
    }
    let mut output = String::with_capacity(input.len());
    if absolute {
        output.push('/');
    }
    for (index, segment) in segments.into_iter().enumerate() {
        if index != 0 {
            output.push('/');
        }
        output.push_str(segment);
    }
    if output.is_empty() || (trailing && !output.ends_with('/')) {
        output.push('/');
    }
    output
}

pub(super) struct RedirectState {
    remaining: u32,
    base: Uri,
    current: Uri,
    visited: Vec<Uri>,
}

impl RedirectState {
    pub(super) fn new(max_redirects: u32, base: &Uri, first_path: &str) -> Result<Self, Error> {
        let current = resolve(base, first_path)?;
        let mut visited = Vec::with_capacity((max_redirects as usize).min(16) + 1);
        visited.push(current.clone());
        Ok(Self {
            remaining: max_redirects,
            base: base.clone(),
            current,
            visited,
        })
    }

    pub(super) fn path_and_query(&self) -> &str {
        match self.current.path_and_query() {
            Some(pq) => pq.as_str(),
            None => self.current.path(),
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
        let new_uri = resolve(&self.current, location)?;
        if origin_relation(&self.base, &new_uri) == OriginRelation::Cross {
            return Err(Error::Http(format!(
                "cross-host redirect to {new_uri} is not supported"
            )));
        }
        if self.visited.contains(&new_uri) {
            return Err(Error::Http("redirect loop detected".into()));
        }
        self.visited.push(new_uri.clone());
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
