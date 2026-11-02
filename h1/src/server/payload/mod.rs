use std::alloc;

use data::{Inline, compose};
use dispatch::chunk::Chunk;
use dope::net::link::egress::data;
use o3::buffer::{Residency, hold, pool, pool::owners, resident, segments};
use sark_http::payload::{compress::Gzip, owned, response::dispatch};

mod raw;
pub mod write;

/// Final-link HTTP/1 payload retaining its physical owners and transport tag.
#[doc(hidden)]
pub enum Body<'d, K: 'd = ()> {
    Shared {
        owner: resident::Shared<'d>,
        tag: K,
    },
    Segments {
        owners: segments::Many<resident::Shared<'d>>,
        tag: K,
    },
    Frozen {
        owner: pool::Span<
            owners::Plain<pool::Fixed<{ Gzip::SLOTS.get() }, { Gzip::CAPACITY.get() }>>,
            pool::Owned,
            pool::Frozen,
        >,
        tag: K,
    },
}

impl<'d, K> Body<'d, K> {
    /// Lowers a duplex owner sum without copying bytes or segment metadata.
    #[doc(hidden)]
    pub fn duplex(chunk: owned::Segments<'d>, tag: K) -> Self {
        match chunk.into_shape() {
            segments::Shape::Empty => Self::Shared {
                owner: resident::Shared::new(),
                tag,
            },
            segments::Shape::One(owner) => Self::Shared {
                owner: owner.into_inner(),
                tag,
            },
            segments::Shape::Many(owners) => Self::Segments { owners, tag },
        }
    }

    /// Returns every physical owner from a publication rejected before the
    /// final link accepted it.
    #[doc(hidden)]
    pub fn release(self) -> K {
        match self {
            Self::Shared { tag, .. } | Self::Segments { tag, .. } | Self::Frozen { tag, .. } => tag,
        }
    }
}

const _: () = {
    use sark_core::life::sequence::observed::Tag;

    let direct = alloc::Layout::new::<
        compose::Either<resident::Shared<'_>, segments::Chain<resident::Shared<'_>>>,
    >();
    assert!(core::mem::size_of::<Body<'_, ()>>() == 4 * core::mem::size_of::<usize>());
    assert!(core::mem::align_of::<Body<'_, ()>>() == direct.align());
    assert!(
        core::mem::size_of::<Body<'_, Tag>>()
            == core::mem::size_of::<Body<'_, ()>>() + core::mem::size_of::<Tag>()
    );
    assert!(
        core::mem::size_of::<Stream<'_, Tag>>()
            == core::mem::size_of::<Frame<'_>>() + core::mem::size_of::<Tag>()
    );
    assert!(
        core::mem::size_of::<compose::Either<Body<'_, Tag>, Stream<'_, Tag>>>()
            == core::mem::size_of::<
                compose::Tagged<
                    compose::Either<
                        compose::Either<
                            resident::Shared<'_>,
                            segments::Chain<resident::Shared<'_>>,
                        >,
                        Frame<'_>,
                    >,
                    Tag,
                >,
            >()
    );
};

const PREFIX_CAPACITY: usize = size_of::<usize>() * 2 + 2;
const SUFFIX_CAPACITY: usize = 2;
const TERMINATOR_CAPACITY: usize = 5;

struct Parts<'d> {
    prefix: Inline<PREFIX_CAPACITY>,
    body: Chunk<'d>,
    suffix: Inline<SUFFIX_CAPACITY>,
}

enum Inner<'d> {
    Chunk(Parts<'d>),
    Terminator(Inline<TERMINATOR_CAPACITY>),
}

/// One allocation-free H1 chunk frame retaining the original body owner.
#[doc(hidden)]
pub struct Frame<'d> {
    inner: Inner<'d>,
    charge: hold::Charge,
}

/// Stream frame paired with its final-link retirement tag.
#[doc(hidden)]
#[repr(C)]
pub struct Stream<'d, K: 'd> {
    frame: Frame<'d>,
    tag: K,
}

/// Whether the owned wire frame terminates its response body.
#[repr(u8)]
#[doc(hidden)]
pub enum End {
    More,
    Last,
}

impl<'d, K> Stream<'d, K> {
    #[doc(hidden)]
    pub const fn new(frame: Frame<'d>, tag: K) -> Self {
        Self { frame, tag }
    }

    pub const fn end(&self) -> End {
        match &self.frame.inner {
            Inner::Chunk(_) => End::More,
            Inner::Terminator(_) => End::Last,
        }
    }

    #[doc(hidden)]
    pub fn into_parts(self) -> (Frame<'d>, K) {
        (self.frame, self.tag)
    }

    #[doc(hidden)]
    pub fn release(self) -> K {
        self.tag
    }
}

impl<'d> Frame<'d> {
    #[doc(hidden)]
    pub fn chunk(body: Chunk<'d>) -> Self {
        let body_len = body.owner().len();
        let prefix = prefix(body_len);
        let suffix = Inline::<SUFFIX_CAPACITY>::from(*b"\r\n");
        let len = prefix.len() + body_len + suffix.len();
        let resident = body.resident();
        let charge = hold::Charge::from_commit(
            len,
            PREFIX_CAPACITY + resident + SUFFIX_CAPACITY,
            2 + usize::from(resident != 0),
        );
        Self {
            inner: Inner::Chunk(Parts {
                prefix,
                body,
                suffix,
            }),
            charge,
        }
    }

    #[doc(hidden)]
    pub fn terminator() -> Self {
        let end = Inline::<TERMINATOR_CAPACITY>::from(*b"0\r\n\r\n");
        let len = end.len();
        let charge = hold::Charge::from_owner(len, end.resident());
        Self {
            inner: Inner::Terminator(end),
            charge,
        }
    }

    #[doc(hidden)]
    pub const fn len(&self) -> usize {
        self.charge.logical_bytes
    }

    pub const fn is_empty(&self) -> bool {
        self.charge.logical_bytes == 0
    }

    #[doc(hidden)]
    pub const fn retention(&self) -> hold::Charge {
        self.charge
    }

    #[doc(hidden)]
    pub fn into_chunk(self) -> Option<Chunk<'d>> {
        match self.inner {
            Inner::Chunk(parts) => Some(parts.body),
            Inner::Terminator(_) => None,
        }
    }
}

#[derive(Clone, Copy)]
enum State<'a, 'd> {
    Prefix {
        parts: &'a Parts<'d>,
        bytes: &'a [u8],
    },
    Body {
        suffix: &'a Inline<SUFFIX_CAPACITY>,
        bytes: &'a [u8],
    },
    Tail(&'a [u8]),
}

/// Cursor over one fixed-shape H1 chunk frame.
#[doc(hidden)]
pub struct Cursor<'a, 'd>(State<'a, 'd>);

const _: () = assert!(size_of::<Cursor<'static, 'static>>() == 4 * size_of::<usize>());

fn prefix(len: usize) -> Inline<PREFIX_CAPACITY> {
    let mut bytes = [0; PREFIX_CAPACITY];
    let [digits @ .., cr, lf] = &mut bytes;
    *cr = b'\r';
    *lf = b'\n';
    let mut value = len;
    let mut left = digits.len();
    for slot in digits.iter_mut().rev() {
        if value == 0 {
            break;
        }
        let digit = (value & 0xf) as u8;
        *slot = if digit < 10 {
            b'0' + digit
        } else {
            b'a' + digit - 10
        };
        left -= 1;
        value >>= 4;
    }
    let mut bytes = bytes.into_iter();
    if left != 0 {
        bytes.nth(left - 1);
    }
    Inline::from(bytes)
}
