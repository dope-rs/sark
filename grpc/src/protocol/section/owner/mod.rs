use o3::buffer::{pool, pool::owners};
use sark_http::field::metadata::section::{rules, storage};

use crate::{
    details::Fault,
    protocol::section::{self, seal},
};

pub struct Owner {
    storage: pool::Span,
}

/// Response spans retain decoder-established UTF-8 through folding and freezing.
#[repr(transparent)]
#[derive(Debug)]
pub(crate) struct Valid {
    storage: pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>,
}

mod check;
pub(super) mod fold;
use check::Check;

impl Owner {
    pub(crate) fn validate(mut self, kind: rules::Kind) -> Result<Valid, seal::Error> {
        let mut check = Check::new(kind);
        fold::Fold::new(&mut self.storage)
            .finish(&mut check)
            .map_err(seal::Error::Fold)?;
        check.require();
        if check.seal.write(&mut self.storage) != seal::Write::Complete {
            return Err(seal::Error::Prefix {
                actual: self.storage.len(),
                required: seal::LEN,
            });
        }
        let fault = check.seal.into_fault();
        if fault != Fault::None {
            return Err(seal::Error::Stored(fault));
        }
        Ok(Valid {
            storage: self.storage.freeze(),
        })
    }
}

impl Valid {
    pub(crate) fn bytes(&self) -> &[u8] {
        self.storage.as_ref()
    }
}

impl storage::Block for Owner {
    type Format = storage::Raw;
    const PREFIX: &'static [u8] = &[0; seal::LEN];

    fn from(storage: storage::Finished<Self>) -> Self {
        Self {
            storage: storage.into_lease(),
        }
    }

    fn into_bytes(self) -> storage::Bytes {
        storage::Bytes::new(self.storage.freeze())
    }
}

impl PartialEq for Valid {
    fn eq(&self, other: &Self) -> bool {
        section::Iter::from(self).eq(section::Iter::from(other))
    }
}

impl Eq for Valid {}

const _: () = {
    assert!(
        core::mem::size_of::<Owner>()
            == core::mem::size_of::<
                pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>,
            >()
    );
    assert!(core::mem::size_of::<Option<Owner>>() == core::mem::size_of::<Owner>());
    assert!(core::mem::size_of::<Valid>() == core::mem::size_of::<Owner>());
    assert!(core::mem::size_of::<Option<Valid>>() == core::mem::size_of::<Valid>());
};
