use o3::buffer::{resident, storage::Owned};

pub struct Block<'d> {
    owner: Owned<resident::Lease<'d>>,
}

impl<'d> Block<'d> {
    pub fn try_new(budget: resident::Budget<'d>) -> Result<Self, resident::AllocError> {
        let bytes = budget.available();
        let owner = Owned::try_allocate(&budget, bytes)?;
        Ok(Self { owner })
    }

    pub fn with<R>(self, op: impl FnOnce() -> R) -> R {
        let result = op();
        drop(self.owner);
        result
    }
}
