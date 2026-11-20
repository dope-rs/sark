use core::{convert::Infallible, ops};

use o3::{alloc, buffer::storage::Shared};
use sark_h2::{
    Role,
    conn::{config, config::Config, machine::Connection},
    frame::{
        self,
        controls::RstStream,
        layout::Header,
        numbers::{ErrorCode, ParseError, Type},
    },
    stream::Id,
};
use sark_http::field::metadata::{
    block::{self, Connect},
    map::record::Entry,
    section::{
        self,
        rules::{self, Disposition, Push},
        storage::{Fault, Fields, Tag},
    },
};

use crate::common::{
    hpack,
    protocol::{ReplyError, Session},
};

pub const INVALID_PATH: &[u8] = b"x";

pub const INVALID_REQUESTS: &[&[Entry<'static>]] = {
    let [method, scheme, path, authority] = hpack::GET;
    &[
        &[method, scheme, path, Entry::new(b":foo", b"bar")],
        &[method, scheme, path, Entry::new(b":status", b"200")],
        &[method, scheme, Entry::new(b"content-type", b"text/plain"), path],
        &[method, scheme, path, authority, Entry::new(b"connection", b"keep-alive")],
        &[method, scheme, path, authority, Entry::new(b"keep-alive", b"timeout=5")],
        &[method, scheme, path, authority, Entry::new(b"transfer-encoding", b"chunked")],
        &[method, scheme, path, authority, Entry::new(b"te", b"gzip")],
        &[
            method,
            Entry::new(b":scheme", b"https"),
            Entry::new(b":authority", b"example.test"),
            Entry::new(b":path", INVALID_PATH),
        ],
        &[method, scheme, path, authority, Entry::new(b"Host", b"x")],
        &[method, scheme, Entry::new(b":path", b""), authority],
        &[method, Entry::new(b":method", b"POST"), scheme, path, authority],
    ]
};

pub const INVALID_RESPONSES: &[&[Entry<'static>]] = &[
    &[Entry::new(b"content-type", b"text/plain")],
    &[
        Entry::new(b":status", b"200"),
        Entry::new(b"Content-Type", b"text/plain"),
    ],
    &[
        Entry::new(b":status", b"200"),
        Entry::new(b":method", b"GET"),
    ],
    &[Entry::new(b":status", b"200"), Entry::new(b":foo", b"bar")],
];

#[derive(Debug)]
pub enum Build {
    Encode(alloc::Error),
    Parse(ParseError),
    Increment(u32),
    Settings {
        ack: frame::Ack,
        bytes: usize,
    },
    GoAway {
        stream: Id,
        bytes: usize,
    },
    Continuation {
        stream: Id,
        bytes: usize,
    },
    Length {
        bytes: usize,
        padding: u8,
    },
    Stream(u32),
    Push {
        stream: Id,
        promised: Id,
        bytes: usize,
    },
    Data {
        stream: Id,
        bytes: usize,
    },
}

#[derive(Default)]
pub struct Declared {
    bytes: usize,
}

impl section::Policy for Declared {
    type Selection = usize;
    type Block = Fields;

    fn finish(self) -> usize {
        self.bytes
    }
}

impl rules::Plan for Declared {
    type Inspect = Infallible;
    const SKIP_DISCARDED: bool = false;

    fn disposition(input: rules::Candidate<'_, Self>) -> Disposition<Self::Inspect> {
        let name = input.name();
        if name.starts_with(b":") || name.is_equal(b"content-length") || name.is_equal(b"x-name") {
            Disposition::Tagged(Tag::USER)
        } else {
            Disposition::Discard
        }
    }

    fn inspect(token: Self::Inspect, _: rules::Observation<'_, Self>) -> block::Validation {
        match token {}
    }

    /// Every committed field retains the USER tag admitted by disposition.
    fn commit(input: rules::Commit<'_, Self>) -> block::Validation {
        let (this, _, range) = input.store();
        this.bytes += range.len();
        block::Validation::Valid
    }
}

#[derive(Clone, Copy)]
pub struct Authority;

impl Push for Authority {
    const SCHEMES: Option<&[&[u8]]> = Some(&[b"https"]);
    const AUTHORITIES: Option<&[&[u8]]> = Some(&[b"example.test"]);
}

#[derive(Default)]
pub struct Stable {
    path: Option<ops::Range<usize>>,
    name: Option<ops::Range<usize>>,
}

impl section::Policy for Stable {
    type Selection = (Option<ops::Range<usize>>, Option<ops::Range<usize>>);
    type Block = Fields;

    fn finish(self) -> Self::Selection {
        (self.path, self.name)
    }
}

impl rules::Plan for Stable {
    type Inspect = Infallible;
    const SKIP_DISCARDED: bool = true;

    fn disposition(input: rules::Candidate<'_, Self>) -> Disposition<Self::Inspect> {
        use sark_http::field::head::Name;
        let (name, known) = input.identify();
        match known {
            Name::Path => Disposition::Tagged(Tag::PATH),
            Name::Regular if name.is_equal(b"x-name") => Disposition::Tagged(Tag::USER),
            _ => Disposition::Discard,
        }
    }

    fn inspect(token: Self::Inspect, _: rules::Observation<'_, Self>) -> block::Validation {
        match token {}
    }

    /// Admission carries PATH or USER; CONNECT alone adds intrinsic AUTHORITY.
    fn commit(input: rules::Commit<'_, Self>) -> block::Validation {
        let (this, mode, range) = input.store();
        match mode {
            Tag::PATH => this.path = Some(range),
            Tag::USER => this.name = Some(range),
            _ => {}
        }
        block::Validation::Valid
    }
}

pub struct Retained<'a> {
    fields: &'a Fields,
}

impl<'a> Retained<'a> {
    pub const fn new(fields: &'a Fields) -> Self {
        Self { fields }
    }
    pub fn try_collect(self) -> Result<Vec<(Tag, &'a [u8])>, Fault> {
        let mut retained = Vec::new();
        for field in self.fields.iter_from(0)? {
            let (tag, _, value) = field?;
            retained.push((tag, value));
        }
        Ok(retained)
    }
}

#[derive(Default)]
pub struct Settings {
    config: Config,
}

impl Settings {
    pub fn limited(max: u32) -> Self {
        Self {
            config: Config {
                local_settings: config::Settings {
                    max_header_list_size: Some(max),
                    ..config::Settings::DEFAULT
                },
                recv_window_target: 65_535,
                ..Config::default()
            },
        }
    }
    pub fn extended() -> Self {
        Self {
            config: Config {
                local_settings: config::Settings {
                    enable_connect_protocol: Connect::Enabled,
                    ..config::Settings::DEFAULT
                },
                recv_window_target: 65_535,
                ..Config::default()
            },
        }
    }
    pub fn try_connect<R: Role, A: Push>(
        self,
    ) -> Result<Connection<R, rules::RetainRaw, Shared, A>, ReplyError> {
        let mut conn = Connection::<R>::try_push::<A>(self.config).map_err(ReplyError::Config)?;
        Session::new(&mut conn)
            .try_prime()
            .map_err(ReplyError::Protocol)?;
        Ok(conn)
    }
}

pub struct Wire<'a> {
    bytes: &'a [u8],
}

impl<'a> Wire<'a> {
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
    pub fn try_reset(self) -> Result<Option<(Id, ErrorCode)>, ParseError> {
        let mut out = self.bytes;
        while !out.is_empty() {
            let header = Header::parse(out)?;
            let total = frame::HEADER_LEN + header.length.into_usize();
            let (wire, tail) = out.split_at_checked(total).ok_or(ParseError::Need {
                actual: out.len(),
                required: total,
            })?;
            if header.kind == Type::RstStream {
                let reset = RstStream::parse(header, &wire[frame::HEADER_LEN..])?;
                return Ok(Some((reset.stream_id(), reset.error())));
            }
            out = tail;
        }
        Ok(None)
    }
}

pub struct Request<'a> {
    extra: &'a [(&'a [u8], &'a [u8])],
}

impl<'a> Request<'a> {
    pub const fn new(extra: &'a [(&'a [u8], &'a [u8])]) -> Self {
        Self { extra }
    }
    pub fn try_encode(self) -> Result<Vec<u8>, hpack::Build> {
        let mut fields = hpack::GET.to_vec();
        for (name, value) in self.extra {
            fields.push(Entry::new(name, value));
        }
        hpack::Fields::new(&fields).try_encode()
    }
    pub fn size(self) -> u32 {
        hpack::GET
            .into_iter()
            .chain(
                self.extra
                    .iter()
                    .map(|(name, value)| Entry::new(name, value)),
            )
            .map(|field| field.name.len() + field.value.len() + 32)
            .sum::<usize>() as u32
    }
}
