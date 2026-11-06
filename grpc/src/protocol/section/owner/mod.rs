use o3::buffer::{pool, pool::owners};
use sark_http::field::metadata::section::rules;

use crate::{
    details::Fault,
    protocol::section::{self, Owner, Seal, seal},
};

/// Response spans retain decoder-established UTF-8 through folding and freezing.
#[repr(transparent)]
#[derive(Debug)]
pub(crate) struct Valid {
    storage: pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>,
}

mod check;
pub(super) mod fold;

impl Valid {
    pub(crate) fn try_from(owner: Owner, kind: rules::Kind) -> Result<Self, seal::Error> {
        let mut storage = owner.into_lease();
        let mut seal = Seal::new(kind);
        fold::Fold::new(&mut storage)
            .finish(&mut seal)
            .map_err(seal::Error::Fold)?;
        seal.require();
        if seal.write(&mut storage) != seal::Write::Complete {
            return Err(seal::Error::Prefix {
                actual: storage.len(),
                required: seal::LEN,
            });
        }
        let fault = seal.into_fault();
        if fault != Fault::None {
            return Err(seal::Error::Stored(fault));
        }
        Ok(Self {
            storage: storage.freeze(),
        })
    }
    pub(crate) fn bytes(&self) -> &[u8] {
        self.storage.as_ref()
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
