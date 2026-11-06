use o3::buffer::pool;
use sark_core::decode::Decode;
use sark_http::field::{
    Indexing,
    metadata::{
        block, lower,
        map::record::Span,
        packed::prefix,
        section::{
            self, rules,
            state::{
                RetainedHead, Selected, complete,
                decode::{Indexed, Session},
            },
            storage,
        },
    },
};

use crate::common::router::ProbeRouter;

pub const UNMATCHED_METHODS: [&[u8]; 2] = [b"GET", b"FROB"];
pub const INVALID_METHOD: &[u8] = b"bad method";
pub const PATH: &[u8] = b"/probe";
pub const INVALID_PATH: &[u8] = b"probe";

type Plan = <ProbeRouter as Decode>::Plan;
type Head<P = Plan> = RetainedHead<<P as section::Policy>::Block, Selected<P>, Option<block::Plan>>;

#[derive(Debug)]
pub enum Error {
    Pool(pool::CreateError),
    Decode(lower::Error),
}

pub fn try_head(version: block::Version, method: &[u8], path: &[u8]) -> Result<Head, Error> {
    try_fields::<Plan>(version, method, path, &[])
}

pub fn try_fields<P: rules::Policy>(
    version: block::Version,
    method: &[u8],
    path: &[u8],
    lengths: &[&[u8]],
) -> Result<Head<P>, Error> {
    let fields: [(&[u8], &[u8]); 4] = [
        (b":method", method),
        (b":scheme", b"https"),
        (b":authority", b"example.test"),
        (b":path", path),
    ];
    let fields = fields.into_iter().chain(
        lengths
            .iter()
            .map(|value| (b"content-length".as_slice(), *value)),
    );
    let capacity = fields
        .clone()
        .map(|(name, value)| prefix::Prefix::LEN + name.len() + value.len())
        .sum::<usize>()
        + <P::Block as storage::Block>::PREFIX.len();
    let fields = fields.map(|(name, value)| {
        (
            block::Named::from_slice(name),
            block::Valued::from_span(Span::from_slice(value)),
        )
    });
    let lease = pool::Span::try_new(capacity).map_err(Error::Pool)?;
    let decoded = match version {
        block::Version::Two => {
            let mut decoder = Session::<P>::try_new(
                lease,
                rules::Kind::Request,
                block::Connect::Disabled,
                storage::Scratch::default(),
            )
            .map_err(|(_, error)| Error::Decode(error))?;
            for (name, value) in fields {
                let indexed = decoder.indexed(Some(name)).map_err(Error::Decode)?;
                match indexed {
                    Indexed::Skip => {
                        decoder.validate(value.validation());
                    }
                    Indexed::Buffered(()) => decoder
                        .field::<rules::RejectPush>(name, value, Indexing::Allowed)
                        .map_err(Error::Decode)?,
                }
            }
            decoder.finish().1.map_err(Error::Decode)?
        }
        block::Version::Three => {
            let mut scratch = storage::Scratch::default();
            let mut decoder = complete::Decoder::<P>::try_new(
                lease,
                &mut scratch,
                rules::Kind::Request,
                block::Connect::Disabled,
            )
            .map_err(Error::Decode)?;
            for (name, value) in fields {
                match decoder.select(name) {
                    complete::Selection::Skipped(field) => field.validate(value.validation()),
                    complete::Selection::Buffered(field) => field
                        .push::<rules::RejectPush>(value, Indexing::Allowed)
                        .map_err(|error| Error::Decode(error.into()))?,
                }
            }
            decoder.finish().map_err(Error::Decode)?
        }
    };
    Ok(decoded.into_parts().0)
}
