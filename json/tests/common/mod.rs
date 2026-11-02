use o3::buffer::{
    resident,
    storage::{AllocError, Shared},
};

pub mod decode;
pub mod encode;
pub mod members;
pub mod nested;
pub mod schema;
pub mod segment;
pub mod sequence;
pub mod values;

#[derive(Debug)]
pub enum SplitError {
    Position { split: usize, len: usize },
    Allocation(AllocError),
}

pub trait Bytes {
    fn is_within(&self, owner: &[u8]) -> bool;
    fn try_segments(&self) -> Result<Vec<Shared>, AllocError>;
    fn try_scoped<'d>(
        &self,
        budget: &resident::Budget<'d>,
    ) -> Result<Vec<resident::Shared<'d>>, resident::AllocError>;
    fn try_split(&self, split: usize) -> Result<Vec<Shared>, SplitError>;
}

impl Bytes for [u8] {
    fn is_within(&self, owner: &[u8]) -> bool {
        let base = owner.as_ptr().addr();
        let end = base + owner.len();
        let start = self.as_ptr().addr();
        base <= start && start + self.len() <= end
    }

    fn try_segments(&self) -> Result<Vec<Shared>, AllocError> {
        self.chunks(1).map(Shared::try_copy).collect()
    }

    fn try_scoped<'d>(
        &self,
        budget: &resident::Budget<'d>,
    ) -> Result<Vec<resident::Shared<'d>>, resident::AllocError> {
        self.chunks(1)
            .map(|bytes| resident::Exact::try_copy(budget, bytes).map(resident::Exact::into_shared))
            .collect()
    }

    fn try_split(&self, split: usize) -> Result<Vec<Shared>, SplitError> {
        let (before, after) = self.split_at_checked(split).ok_or(SplitError::Position {
            split,
            len: self.len(),
        })?;
        [before, after]
            .into_iter()
            .filter(|segment| !segment.is_empty())
            .map(Shared::try_copy)
            .collect::<Result<_, _>>()
            .map_err(SplitError::Allocation)
    }
}
