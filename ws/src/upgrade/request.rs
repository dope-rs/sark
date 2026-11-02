use sark_http::{
    field,
    h1::{codec::decode::fields, field::Connection, framing},
};

use crate::upgrade::{Error, Header, Key, Need, state};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
enum Version {
    #[default]
    Missing,
    Thirteen,
}

const _: () = assert!(core::mem::size_of::<Version>() == core::mem::size_of::<bool>());
const _: () = assert!(core::mem::align_of::<Version>() == core::mem::align_of::<bool>());

#[derive(Default)]
pub struct Admission {
    upgrade: state::State,
    version: Version,
    key: Option<Key>,
}

impl<'input> fields::Sink<'input> for Admission {
    type Error = Error;

    fn push(&mut self, name: &'input [u8], value: &'input [u8]) -> Result<(), Self::Error> {
        if name.eq_ignore_ascii_case(b"upgrade") {
            self.upgrade.push(value)?;
        } else if name.eq_ignore_ascii_case(b"sec-websocket-version") {
            match self.version {
                Version::Missing => {}
                Version::Thirteen => return Err(Error::Duplicate(Header::Version)),
            }
            let actual = field::Error::trim_ows(value);
            if let Some((offset, actual, want)) = mismatch(actual, b"13") {
                return Err(Error::WsVersion {
                    offset,
                    actual,
                    want,
                });
            }
            self.version = Version::Thirteen;
        } else if name.eq_ignore_ascii_case(b"sec-websocket-key") {
            if self.key.is_some() {
                return Err(Error::Duplicate(Header::Key));
            }
            self.key = Some(Key::parse(field::Error::trim_ows(value)).map_err(Error::Key)?);
        }
        Ok(())
    }
}

impl Admission {
    pub fn finish(
        self,
        method: &[u8],
        target: &[u8],
        wire: u8,
        body: framing::RequestBody,
        conn: Connection,
        path: &[u8],
    ) -> Result<[u8; 28], Error> {
        if let Some((offset, actual, want)) = mismatch(method, b"GET") {
            return Err(Error::Method {
                offset,
                actual,
                want,
            });
        }
        if wire != 1 {
            return Err(Error::Version { actual: wire });
        }
        if !path.is_empty()
            && let Some((offset, actual, want)) = mismatch(target, path)
        {
            return Err(Error::Target {
                offset,
                actual,
                want,
            });
        }
        if body != framing::RequestBody::Empty {
            return Err(Error::Body(body));
        }
        if conn.is_close() {
            return Err(Error::Close);
        }
        if !conn.is_upgrade() {
            return Err(Error::Connection);
        }
        self.upgrade.require()?;
        match self.version {
            Version::Missing => return Err(Error::Missing(Need::Version)),
            Version::Thirteen => {}
        }
        self.key
            .as_ref()
            .map(Key::accept)
            .ok_or(Error::Missing(Need::Key))
    }
}

fn mismatch(actual: &[u8], want: &[u8]) -> Option<(usize, Option<u8>, Option<u8>)> {
    for (offset, (&actual, &want)) in actual.iter().zip(want).enumerate() {
        if actual != want {
            return Some((offset, Some(actual), Some(want)));
        }
    }
    let shared = actual.len().min(want.len());
    (actual.len() != want.len()).then_some((
        shared,
        actual.get(shared).copied(),
        want.get(shared).copied(),
    ))
}

const _: () = assert!(core::mem::size_of::<Admission>() <= 32);
