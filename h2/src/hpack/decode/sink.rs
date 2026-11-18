use sark_http::field::{
    Indexing,
    metadata::{
        block,
        section::state::decode::{Indexed, Start},
        strings,
    },
};

use crate::hpack::decode;

pub(crate) type Output<S, T> = Result<(S, T), (S, <S as Direct>::Error)>;

const _: () = assert!(core::mem::size_of::<Indexed<()>>() <= core::mem::size_of::<Start<(), ()>>());

pub(crate) trait Direct {
    type Error: From<decode::Error>;

    fn string(&mut self, cursor: &mut strings::Cursor, input: &[u8]) -> Result<(), Self::Error>;

    fn rollback(&mut self);
}

pub(crate) trait Store: Direct + Sized {
    fn validate(&mut self, value: block::Validation) -> Result<(), Self::Error>;

    fn begin(
        self,
        name: Option<block::Named<'_>>,
        indexing: Indexing,
    ) -> Output<Self, Start<(), ()>>;

    fn indexed(self, name: Option<block::Named<'_>>) -> Output<Self, Indexed<()>>;

    fn field(
        self,
        value: block::Valued<'_>,
        indexing: Indexing,
        name: block::Named<'_>,
    ) -> Result<Self, (Self, Self::Error)>;

    fn direct(self) -> Result<Self, (Self, Self::Error)>;
}
