use std::slice;

use http::{HeaderName, HeaderValue};

use super::{HeaderNameRef, IntoHeaderName};

#[derive(Clone, Default, PartialEq, Eq)]
pub struct HeaderList {
    entries: Vec<(HeaderName, HeaderValue)>,
    wire_len: usize,
}

impl std::fmt::Debug for HeaderList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_list();
        for (name, value) in self {
            list.entry(&(name, value));
        }
        list.finish()
    }
}

pub struct HeaderIter<'a> {
    inner: slice::Iter<'a, (HeaderName, HeaderValue)>,
}

impl<'a> Iterator for HeaderIter<'a> {
    type Item = (&'a HeaderName, &'a HeaderValue);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(name, value)| (name, value))
    }
}

impl HeaderList {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            wire_len: 0,
        }
    }

    pub(super) fn empty_static() -> &'static Self {
        use std::sync::OnceLock;
        static EMPTY: OnceLock<HeaderList> = OnceLock::new();
        EMPTY.get_or_init(HeaderList::new)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            wire_len: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn iter(&self) -> HeaderIter<'_> {
        HeaderIter {
            inner: self.entries.iter(),
        }
    }

    pub fn wire_len(&self) -> usize {
        self.wire_len
    }

    pub fn get<K>(&self, name: K) -> Option<&HeaderValue>
    where
        K: HeaderNameRef,
    {
        self.entries
            .iter()
            .find(|(n, _)| n.as_str().eq_ignore_ascii_case(name.as_header_name()))
            .map(|(_, v)| v)
    }

    pub fn contains_key<K>(&self, name: K) -> bool
    where
        K: HeaderNameRef,
    {
        self.get(name).is_some()
    }

    pub fn insert<N>(&mut self, name: N, value: HeaderValue) -> Option<HeaderValue>
    where
        N: IntoHeaderName,
    {
        let name = name.into_header_name();
        let added = HeaderEntryWire::len(name.as_str(), value.as_bytes());
        if let Some(index) = self.entries.iter().position(|(n, _)| *n == name) {
            let removed_old = HeaderEntryWire::len(
                self.entries[index].0.as_str(),
                self.entries[index].1.as_bytes(),
            );
            let old = std::mem::replace(&mut self.entries[index].1, value);
            self.wire_len = self.wire_len + added - removed_old;
            self.dedup_by_name(&name, index + 1);
            return Some(old);
        }
        self.entries.push((name, value));
        self.wire_len += added;
        None
    }

    pub fn remove<K>(&mut self, name: K) -> Option<HeaderValue>
    where
        K: HeaderNameRef,
    {
        let index = self
            .entries
            .iter()
            .position(|(n, _)| n.as_str().eq_ignore_ascii_case(name.as_header_name()))?;
        let (removed_name, removed_value) = self.entries.remove(index);
        self.wire_len -= HeaderEntryWire::len(removed_name.as_str(), removed_value.as_bytes());
        self.dedup_by_name(&removed_name, 0);
        Some(removed_value)
    }

    fn dedup_by_name(&mut self, name: &HeaderName, start: usize) {
        let mut scan = start;
        while scan < self.entries.len() {
            if self.entries[scan].0 == *name {
                self.wire_len -= HeaderEntryWire::len(
                    self.entries[scan].0.as_str(),
                    self.entries[scan].1.as_bytes(),
                );
                self.entries.remove(scan);
            } else {
                scan += 1;
            }
        }
    }
}

struct HeaderEntryWire;

impl HeaderEntryWire {
    fn len(name: &str, value: &[u8]) -> usize {
        name.len() + 2 + value.len() + 2
    }
}

impl From<Vec<(HeaderName, HeaderValue)>> for HeaderList {
    fn from(value: Vec<(HeaderName, HeaderValue)>) -> Self {
        let mut headers = Self::with_capacity(value.len());
        for (name, value) in value {
            let _ = headers.insert(name, value);
        }
        headers
    }
}

impl<'a> IntoIterator for &'a HeaderList {
    type Item = (&'a HeaderName, &'a HeaderValue);
    type IntoIter = HeaderIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
