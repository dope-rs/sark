use o3::{
    alloc,
    buffer::{hold, resident, storage::Owned, write::SpareExt as _},
};

use crate::protocol::{Error, codec::output};

pub(super) struct Pending<'d> {
    owner: Option<Owned<resident::Budget<'d>>>,
}

impl<'d> Pending<'d> {
    pub(super) const fn new() -> Self {
        Self { owner: None }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.owner.is_none()
    }

    pub(super) fn as_slice(&self) -> &[u8] {
        self.owner.as_ref().map_or(&[], |owner| owner.as_slice())
    }

    pub(super) fn remaining(&self) -> usize {
        self.owner
            .as_ref()
            .map_or(output::DATA_LIMIT, |owner| output::DATA_LIMIT - owner.len())
    }

    pub(super) fn extend(
        &mut self,
        budget: resident::Budget<'d>,
        bytes: &[u8],
    ) -> Result<(), Error> {
        let owner = match self.owner.as_mut() {
            Some(owner) => owner,
            None => self.owner.insert(
                Owned::try_allocate(&budget, output::DATA_LIMIT)
                    .map_err(|error| Error::Retention(hold::Error::Resident(error)))?,
            ),
        };
        let mut spare = owner.spare();
        spare.write(bytes);
        spare.commit();
        Ok(())
    }

    pub(super) fn take_full(&mut self) -> Option<resident::Shared<'d>> {
        self.owner
            .take_if(|owner| owner.len() == output::DATA_LIMIT)
            .map(<Owned<resident::Budget<'d>>>::freeze)
    }

    pub(super) fn finish(&mut self) -> Result<Option<resident::Shared<'d>>, Error> {
        if let Some(owner) = self.owner.as_mut()
            && let Err(error) = owner.try_compact()
        {
            return Err(Error::Compact(alloc::Error::from(error)));
        }
        Ok(self.owner.take().map(<Owned<resident::Budget<'d>>>::freeze))
    }
}

const _: () = assert!(
    core::mem::size_of::<Pending<'static>>()
        == core::mem::size_of::<Option<Owned<resident::Budget<'static>>>>()
);
