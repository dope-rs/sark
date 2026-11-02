use o3::buffer::{pool, write::consume};
use sark_core::decode::Decode;
use sark_http::field::metadata::{
    block, lower, map,
    map::record::{Entry, Span},
    packed::Decoded,
    section::{
        rules,
        state::decode::{Head, Indexed, Session},
        storage,
    },
};

#[derive(Debug)]
pub enum Error {
    Field(map::Error),
    Pool(pool::CreateError),
    Write(consume::Error),
    Format(storage::Fault),
    Decode(lower::Error),
}

pub trait EntriesExt {
    fn try_decoded(&self) -> Result<Decoded, Error>;

    fn try_raw(
        &self,
        connect: block::Connect,
    ) -> Result<Head<rules::RetainRaw, Option<block::Plan>>, Error> {
        select::<rules::RetainRaw>(self.try_decoded()?, connect)
    }
}

impl EntriesExt for [Entry<'_>] {
    fn try_decoded(&self) -> Result<Decoded, Error> {
        let mut fields = map::Fields::new();
        for entry in self {
            fields.push(entry.name, entry.value).map_err(Error::Field)?;
        }
        let bytes = fields.as_bytes();
        let mut owner = pool::Span::try_new(bytes.len().max(1)).map_err(Error::Pool)?;
        owner.try_append(bytes).map_err(Error::Write)?.commit();
        Decoded::try_from(owner).map_err(Error::Format)
    }
}

pub trait DecodeExt: Decode {
    fn try_select(&self, fields: Decoded) -> Result<Head<Self::Plan, Option<block::Plan>>, Error> {
        select::<Self::Plan>(fields, block::Connect::Disabled)
    }
}

impl<R: Decode> DecodeExt for R {}

fn select<P: rules::Policy>(
    fields: Decoded,
    connect: block::Connect,
) -> Result<Head<P, Option<block::Plan>>, Error> {
    let lease = pool::Span::try_new(fields.as_bytes().len().max(1)).map_err(Error::Pool)?;
    let mut decoder = Session::<P>::try_new(
        lease,
        rules::Kind::Request,
        connect,
        storage::Scratch::default(),
    )
    .map_err(|(_, error)| Error::Decode(error))?;
    for field in fields.iter() {
        let name = block::Named::from_span(Span::from_slice(field.name));
        decoder.validate(block::Validation::Valid);
        let indexed = decoder.indexed(Some(name)).map_err(Error::Decode)?;
        if let Indexed::Buffered(()) = indexed {
            decoder
                .field::<rules::RejectPush>(
                    name,
                    block::Valued::from_span(Span::from_slice(field.value)),
                    field.indexing,
                )
                .map_err(Error::Decode)?;
        }
    }
    let (_, result) = decoder.finish();
    let (head, _, _, _) = result.map_err(Error::Decode)?.into_parts();
    Ok(head)
}
