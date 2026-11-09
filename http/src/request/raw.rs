use core::ops;

use crate::{
    field::metadata::{
        packed,
        section::{
            self, rules,
            state::{RetainedHead, Selected},
            storage::{self, Block},
        },
    },
    request::{Bound, Error, Layout, Source},
};

/// Consumes the exact retained request admission and its immutable owner.
pub trait Bind: Sized {
    type Owner: Source;

    ///
    /// # Safety
    /// Self must come from the native decoder and a closed policy preserving native value and final section admission; these exact original field ranges must come from its native iteration or private selection populated only by that decoder's Commit values tagging actual wire names.
    unsafe fn try_bind(
        self,
        path: Option<ops::Range<usize>>,
        path_len: Option<usize>,
        pseudo: Option<ops::Range<usize>>,
        host: Option<ops::Range<usize>>,
    ) -> Result<Bound<Self::Owner>, Error>;
}

impl<const PREFIX: usize> Bind
    for RetainedHead<
        packed::Decoded<o3::buffer::pool::Span, PREFIX>,
        Selected<rules::RetainRaw<PREFIX>>,
    >
{
    type Owner = packed::Decoded<o3::buffer::pool::Span, PREFIX>;

    unsafe fn try_bind(
        self,
        path: Option<ops::Range<usize>>,
        path_len: Option<usize>,
        pseudo: Option<ops::Range<usize>>,
        host: Option<ops::Range<usize>>,
    ) -> Result<Bound<Self::Owner>, Error> {
        let (owner, _, plan) = self.into_parts();
        let layout = Layout::from_kept(owner.as_bytes(), path, path_len, pseudo, host, plan)?;
        Ok(Bound { owner, layout })
    }
}

impl<P: section::Policy<Block = storage::Fields>> Bind
    for RetainedHead<storage::Fields, Selected<P>>
{
    type Owner = storage::Bytes;

    unsafe fn try_bind(
        self,
        path: Option<ops::Range<usize>>,
        path_len: Option<usize>,
        pseudo: Option<ops::Range<usize>>,
        host: Option<ops::Range<usize>>,
    ) -> Result<Bound<Self::Owner>, Error> {
        let (fields, _, plan) = self.into_parts();
        let owner = fields.into_bytes();
        let layout = Layout::from_kept(owner.as_bytes(), path, path_len, pseudo, host, plan)?;
        Ok(Bound { owner, layout })
    }
}
