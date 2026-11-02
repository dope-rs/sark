use std::{convert::Infallible, ops};

use o3::buffer::pool;
use sark_http::field::{
    head,
    metadata::{
        block::{self, Validation},
        lower::{self, Lower},
        map::record::{Entry, Span},
        section,
        section::{
            rules,
            rules::{Disposition, Kind, Push, Retention},
            state::{Decoder, complete, fixed},
            storage::{Fields, Scratch, Tag},
        },
    },
};

pub const PATH: &[u8] = b"/asset";
pub const REQUEST: [Entry<'static>; 4] = [
    Entry::new(b":method", b"GET"),
    Entry::new(b":scheme", b"https"),
    Entry::new(b":path", PATH),
    Entry::new(b":authority", b"example.test"),
];
pub const HOST: Entry<'static> = Entry::new(b"host", b"example.test");
pub const REPLACEMENT: Entry<'static> = Entry::new(b":path", b"/other");
pub const PENDING: Entry<'static> = Entry::new(b"x-pending", b"");
pub const INVALID: Entry<'static> = Entry::new(b"x-invalid", b"\0");
pub const PATH_VALUES: &[(&[u8], Validation)] = &[
    (b"x", Validation::Malformed),
    (b"/a#frag", Validation::Malformed),
    (b"/%", Validation::Malformed),
    (b"/%2", Validation::Malformed),
    (b"/%xz", Validation::Malformed),
    (b"/a b", Validation::Malformed),
    (b"/a\tb", Validation::Malformed),
    (b"/", Validation::Valid),
    (b"//authority/path", Validation::Valid),
    (b"/a%23frag?x=%25&y=?", Validation::Valid),
    (b"*", Validation::Valid),
];
pub const TE_VALUES: &[(&[u8], Validation)] = &[
    (b"", Validation::Valid),
    (b",,", Validation::Valid),
    (b"trailers", Validation::Valid),
    (b"TrAiLeRs", Validation::Valid),
    (b"trailers, trailers", Validation::Valid),
    (b"trailers,\t, Trailers", Validation::Valid),
    (b",trailers,", Validation::Valid),
    (b"gzip", Validation::Malformed),
    (b"trailers;q=1", Validation::Malformed),
    (b"trailer", Validation::Malformed),
    (b"trailersx", Validation::Malformed),
    (b"trail ers", Validation::Malformed),
    (b" trailers", Validation::Malformed),
    (b"trailers\t", Validation::Malformed),
];
pub const LENGTH_VALUES: &[(&[&[u8]], Option<u64>)] = &[
    (&[], None),
    (&[b"0"], Some(0)),
    (&[b"18446744073709551615"], Some(u64::MAX)),
    (&[b"5", b"005", b"5 \t,\t005"], Some(5)),
];
pub const LENGTH_INVALID: &[&[&[u8]]] = &[
    &[b""],
    &[b"+5"],
    &[b"5x"],
    &[b"5 5"],
    &[b"5\t5"],
    &[b",5"],
    &[b"5,"],
    &[b"5,,5"],
    &[b"18446744073709551616"],
    &[b"5, 9"],
    &[b"5", b"9"],
];
pub const LENGTH_CONFLICT: [Entry<'static>; 2] = [
    Entry::new(b"content-length", b"5"),
    Entry::new(b"content-length", b"9"),
];
pub const LENGTH_ZERO: Entry<'static> = Entry::new(b"content-length", b"0");
pub const LENGTH_RESPONSES: &[(&[u8], u16, Entry<'static>, Validation)] = &[
    (b"103", 103, LENGTH_ZERO, Validation::Malformed),
    (b"204", 204, LENGTH_CONFLICT[0], Validation::Valid),
    (b"205", 205, LENGTH_CONFLICT[0], Validation::Valid),
    (b"304", 304, LENGTH_CONFLICT[0], Validation::Valid),
];

#[derive(Debug)]
pub enum Error {
    Storage(pool::CreateError),
    Lower(lower::Error),
}

pub struct Authority;

impl Push for Authority {
    const SCHEMES: Option<&[&[u8]]> = Some(&[b"https"]);
    const AUTHORITIES: Option<&[&[u8]]> = Some(&[b"example.test"]);
}

#[derive(Default)]
pub struct CompactPlan {
    path: Option<ops::Range<usize>>,
}

impl section::Policy for CompactPlan {
    type Selection = Option<ops::Range<usize>>;
    type Block = Fields;

    fn finish(self) -> Self::Selection {
        self.path
    }
}

impl rules::Plan for CompactPlan {
    type Inspect = Infallible;
    const SKIP_DISCARDED: bool = false;

    fn disposition(input: rules::Candidate<'_, Self>) -> Disposition<Self::Inspect> {
        let (_, known) = input.identify();
        if known == head::Name::Path {
            Disposition::Tagged(Tag::PATH)
        } else {
            Disposition::Discard
        }
    }

    fn inspect(token: Self::Inspect, _: rules::Observation<'_, Self>) -> Validation {
        match token {}
    }

    fn commit(input: rules::Commit<'_, Self>) -> Validation {
        let (this, tag, range) = input.store();
        if tag == Tag::PATH {
            this.path = Some(range);
        }
        Validation::Valid
    }
}

impl CompactPlan {
    pub fn try_complete(
        scratch: &mut Scratch,
    ) -> Result<complete::Decoder<'_, CompactPlan>, Error> {
        let lease = pool::Span::try_new(128).map_err(Error::Storage)?;
        let mut decoder = complete::Decoder::try_new(
            lease,
            scratch,
            rules::Kind::Request,
            block::Connect::Disabled,
        )
        .map_err(Error::Lower)?;
        for field in REQUEST {
            Self::try_push(&mut decoder, field).map_err(Error::Lower)?;
        }
        Ok(decoder)
    }

    pub fn try_push(
        decoder: &mut complete::Decoder<'_, CompactPlan>,
        field: Entry<'_>,
    ) -> Result<(), lower::Error> {
        use complete::Selection;
        use sark_http::field::metadata::map::record::Span;

        let name = block::Named::from_span(Span::from_slice(field.name));
        let value = block::Valued::from_span(Span::from_slice(field.value));
        match decoder.select(name) {
            Selection::Skipped(field) => field.validate(value.validation()),
            Selection::Buffered(selected) => selected.push::<Authority>(value, field.indexing)?,
        }
        Ok(())
    }
}

impl Authority {
    pub fn validation(kind: Kind, fields: &[Entry<'_>]) -> Validation {
        let mut section = fixed::Raw::new(kind);
        for field in fields {
            let value = block::Valued::from_span(Span::from_slice(field.value));
            rules::Validate::value(&mut section, value.validation());
            let proof = block::Name::classify(field.name);
            if section.accept(proof) == block::Validation::Valid {
                match section.retain::<Authority>(proof.head(), (*field).into(), &[], &[]) {
                    Retention::Retain | Retention::Discard => {}
                }
            }
        }
        section.finish().1
    }

    pub fn is_valid(kind: Kind, fields: &[Entry<'_>]) -> bool {
        Self::validation(kind, fields) == Validation::Valid
    }

    pub fn request<'a>(method: &'a [u8], extra: Option<Entry<'a>>) -> Vec<Entry<'a>> {
        let mut fields = vec![
            Entry::new(b":method", method),
            Entry::new(b":scheme", b"https"),
            Entry::new(b":path", b"/asset"),
            Entry::new(b":authority", b"example.test"),
        ];
        fields.extend(extra);
        fields
    }
}

pub trait Apply {
    fn try_lower<A: Push>(
        &mut self,
        lease: &mut pool::Span,
        scratch: &mut Scratch,
        field: Entry<'_>,
    ) -> Result<(), lower::Error>;
}

impl<P: rules::Plan> Apply for Decoder<P> {
    fn try_lower<A: Push>(
        &mut self,
        lease: &mut pool::Span,
        scratch: &mut Scratch,
        field: Entry<'_>,
    ) -> Result<(), lower::Error> {
        let name = Span::from_slice(field.name);
        let value = Span::from_slice(field.value);
        let mut lower = Lower::new(lease, scratch, self);
        let decision = lower.disposition(name, block::Name::classify(field.name));
        let push = lower.try_push_as(decision, name, value, field.indexing)?;
        push.commit::<A>();
        Ok(())
    }
}
