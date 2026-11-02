use o3::buffer::{pool, write::consume};
use sark_http::field::metadata::{map, map::record, packed, section::storage};

#[derive(Debug)]
pub enum Error {
    Fields(map::Error),
    Pool(pool::CreateError),
    Write(consume::Error),
    Format(storage::Fault),
}

pub trait Decode: Sized {
    fn try_decode(entries: &[record::Entry<'_>]) -> Result<Self, Error>;
}

impl Decode for packed::Decoded {
    fn try_decode(entries: &[record::Entry<'_>]) -> Result<Self, Error> {
        let mut fields = map::Fields::new();
        for field in entries {
            fields.push_entry(*field).map_err(Error::Fields)?;
        }
        let bytes = fields.as_bytes();
        let mut lease = pool::Span::try_new(bytes.len().max(1)).map_err(Error::Pool)?;
        lease.try_append(bytes).map_err(Error::Write)?.commit();
        Ok(<packed::Decoded as storage::Block>::from(
            storage::Finished::try_from(lease).map_err(Error::Format)?,
        ))
    }
}
