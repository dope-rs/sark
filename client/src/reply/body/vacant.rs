use o3::{
    alloc,
    buffer::{segments, storage},
};

pub(in crate::reply) enum Vacant {
    Empty,
    One,
    Many(segments::Chain<storage::Shared>),
}

impl Vacant {
    pub(in crate::reply) fn try_new(segments: segments::Count) -> Result<Self, alloc::Error> {
        Ok(match segments.into_raw() {
            0 => Self::Empty,
            1 => Self::One,
            _ => {
                let pool = segments::Pool::try_new(segments::Plan {
                    segments: segments.into_raw() as usize,
                    chains: 1,
                })?;
                Self::Many(pool.into_chain())
            }
        })
    }
}
