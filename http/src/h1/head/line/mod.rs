mod cursor;
pub mod error;
mod owned;
mod version;
use std::num::NonZero;

pub(in crate::h1::head) use cursor::Cursor;
use o3::buffer::{bytes::window::Snapshot, storage};
pub use owned::Owned;
pub use version::Version;

use crate::{
    field::{self, scan::Match},
    h1::target,
    method,
    payload::response::status::Code,
    request::span,
};

#[derive(Clone, Copy)]
pub(in crate::h1::head) enum Progress<T> {
    Complete(T),
    Partial(Cursor),
}

#[derive(Clone, Copy)]
pub struct Request<'a> {
    source: &'a [u8],
    parts: owned::Parts,
}

#[derive(Clone, Copy)]
pub struct Response<'a> {
    reason: &'a [u8],
    end: usize,
    version: Version,
    status: Code,
}

impl Request<'_> {
    pub fn parse(bytes: &[u8]) -> Result<Option<Request<'_>>, error::Request> {
        Self::parse_with(bytes, method::Known::ALL)
    }

    #[doc(hidden)]
    pub fn parse_with<'a>(
        bytes: &'a [u8],
        methods: u8,
    ) -> Result<Option<Request<'a>>, error::Request> {
        let Some((mend, matched)) = parse_method(bytes, methods)? else {
            return Ok(None);
        };
        let Some(start) = mend.checked_add(1) else {
            return Ok(None);
        };
        let Some(tail) = bytes.get(start..) else {
            return Ok(None);
        };
        let Some(method) = bytes.get(..mend) else {
            return Ok(None);
        };
        let Some((len, plan)) =
            target::Target::parse_prefix(method, tail, start).map_err(error::Request::Target)?
        else {
            return Ok(None);
        };
        let target = match span::Span::checked(start, len) {
            Ok(target) => target,
            Err(cause) => return Err(error::Request::Span(cause)),
        };
        let Some(off) = target.end().checked_add(1) else {
            return Ok(None);
        };
        let Some(suffix) = bytes.get(off..off + 10) else {
            return Ok(None);
        };
        let Some(version) = Version::parse(suffix) else {
            return Err(error::Request::Syntax);
        };
        if suffix[8] != b'\r' || suffix[9] != b'\n' {
            return Err(error::Request::Syntax);
        }
        Ok(Some(Request {
            source: bytes,
            parts: owned::Parts {
                target,
                plan,
                version,
                matched,
            },
        }))
    }

    pub(in crate::h1::head) fn progress(
        bytes: &[u8],
    ) -> Result<Progress<Request<'_>>, error::Request> {
        match Self::parse(bytes)? {
            Some(request) => Ok(Progress::Complete(request)),
            None => Cursor::pending(bytes).map(Progress::Partial),
        }
    }
}

fn parse_method(
    bytes: &[u8],
    methods: u8,
) -> Result<Option<(usize, Option<method::Known>)>, error::Request> {
    if let Some(known) = parse_known(bytes, methods) {
        return Ok(Some((known.0, Some(known.1))));
    }
    let Some(end) = Match::find2(b' ', b'\r', bytes) else {
        return Ok(None);
    };
    if end.start() == 0
        || end.byte() == b'\r'
        || !end.prefix().iter().copied().all(field::Error::is_name_byte)
    {
        return Err(error::Request::Syntax);
    }
    Ok(Some((end.start(), None)))
}

fn parse_known(bytes: &[u8], methods: u8) -> Option<(usize, method::Known)> {
    use method::Known;

    if methods & Known::Get.bit() != 0 && bytes.starts_with(b"GET ") {
        Some((3, Known::Get))
    } else if methods & Known::Post.bit() != 0 && bytes.starts_with(b"POST ") {
        Some((4, Known::Post))
    } else if methods & Known::Put.bit() != 0 && bytes.starts_with(b"PUT ") {
        Some((3, Known::Put))
    } else if methods & Known::Patch.bit() != 0 && bytes.starts_with(b"PATCH ") {
        Some((5, Known::Patch))
    } else if methods & Known::Delete.bit() != 0 && bytes.starts_with(b"DELETE ") {
        Some((6, Known::Delete))
    } else if methods & Known::Head.bit() != 0 && bytes.starts_with(b"HEAD ") {
        Some((4, Known::Head))
    } else if methods & Known::Options.bit() != 0 && bytes.starts_with(b"OPTIONS ") {
        Some((7, Known::Options))
    } else if methods & Known::Connect.bit() != 0 && bytes.starts_with(b"CONNECT ") {
        Some((7, Known::Connect))
    } else {
        None
    }
}

impl<'a> Request<'a> {
    pub(in crate::h1::head) const fn source(self) -> &'a [u8] {
        self.source
    }

    pub fn method(&self) -> &'a [u8] {
        self.parts().0
    }

    pub fn target(&self) -> &'a [u8] {
        self.parts().1
    }

    fn parts(&self) -> (&'a [u8], &'a [u8]) {
        let target = self.parts.target;
        // SAFETY: Request's private constructors admit a nonempty method, its
        // space, the target, and the following version in this exact source.
        // Owned retains these parts only with its unchanged Source prefix;
        // snapshots copy that prefix. Both ranges remain within this loan.
        unsafe {
            (
                self.source.get_unchecked(..target.start() - 1),
                self.source.get_unchecked(target.start()..target.end()),
            )
        }
    }

    pub const fn target_span(&self) -> span::Span {
        self.parts.target
    }

    pub const fn version(&self) -> Version {
        self.parts.version
    }

    pub const fn end(&self) -> usize {
        self.parts.target.end() + 11
    }

    pub const fn matched(&self) -> Option<method::Known> {
        self.parts.matched
    }

    pub const fn plan(&self) -> target::Target {
        self.parts.plan
    }

    pub fn path(&self) -> Option<&'a [u8]> {
        self.parts.plan.route_path(self.source)
    }

    pub fn query(&self) -> Option<&'a [u8]> {
        self.parts
            .plan
            .query_span()
            .and_then(|span| span.get(self.source))
    }

    pub fn authority(&self) -> Option<&'a [u8]> {
        self.parts
            .plan
            .authority()
            .and_then(|span| span.get(self.source))
    }

    /// Retains this exact input and line proof in one append-only allocation.
    pub fn try_snapshot<const MAX: usize>(
        &self,
        capacity: NonZero<usize>,
    ) -> Result<Owned<MAX>, storage::AllocError> {
        let source = Snapshot::try_copy(self.source, capacity)?;
        Ok(Owned {
            source,
            parts: self.parts,
        })
    }
}

impl Response<'_> {
    pub fn parse(bytes: &[u8]) -> Result<Option<Response<'_>>, error::Response> {
        Self::progress(bytes).map(|progress| match progress {
            Progress::Complete(response) => Some(response),
            Progress::Partial(_) => None,
        })
    }

    pub(in crate::h1::head) fn progress(
        bytes: &[u8],
    ) -> Result<Progress<Response<'_>>, error::Response> {
        let Some(end) = Match::find(b'\r', bytes) else {
            return Ok(Progress::Partial(Cursor::at(bytes.len())));
        };
        let Some(next) = end.suffix().first() else {
            return Ok(Progress::Partial(Cursor::at(end.start())));
        };
        if *next != b'\n' {
            return Err(error::Response::Syntax);
        }
        parse_response(end.prefix(), end.start()).map(Progress::Complete)
    }
}

impl<'a> Response<'a> {
    pub const fn version(&self) -> Version {
        self.version
    }

    pub const fn status(&self) -> Code {
        self.status
    }

    pub fn reason(&self) -> &'a [u8] {
        self.reason
    }

    pub const fn end(&self) -> usize {
        self.end
    }
}

fn parse_response(line: &[u8], end: usize) -> Result<Response<'_>, error::Response> {
    let version = Version::parse(line).ok_or(error::Response::Syntax)?;
    if line.get(8) != Some(&b' ') || line.get(12) != Some(&b' ') {
        return Err(error::Response::Syntax);
    }
    let digits = line.get(9..12).ok_or(error::Response::Syntax)?;
    let text = line.get(13..).ok_or(error::Response::Syntax)?;
    if !digits.iter().all(u8::is_ascii_digit)
        || !text.iter().copied().all(crate::field::Error::is_text)
    {
        return Err(error::Response::Syntax);
    }
    let status = u16::from(digits[0] - b'0') * 100
        + u16::from(digits[1] - b'0') * 10
        + u16::from(digits[2] - b'0');
    let status = Code::from_u16(status).map_err(error::Response::Status)?;
    let end = end.checked_add(2).ok_or(error::Response::Syntax)?;
    Ok(Response {
        reason: text,
        end,
        version,
        status,
    })
}

const _: () = assert!(core::mem::size_of::<Request<'_>>() <= 10 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Response<'_>>() <= 4 * core::mem::size_of::<usize>());
