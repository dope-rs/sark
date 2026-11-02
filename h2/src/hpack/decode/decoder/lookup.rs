use dynamic::{Field, mode, table};
use sark_http::payload::compress::dynamic;

use crate::hpack::{decode, statics};

pub(in crate::hpack::decode) fn get<'a>(
    dynamic: &'a table::Table<mode::Staged>,
    index: usize,
) -> Result<Field<'a>, decode::Error> {
    let fixed = statics::TABLE.len();
    if index <= fixed {
        return statics::TABLE.get(index).ok_or(decode::Error::BadIndex);
    }
    dynamic
        .field(index - fixed - 1)
        .ok_or(decode::Error::BadIndex)
}
