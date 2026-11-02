use sark_http::field::{
    Indexing,
    metadata::{block, strings},
};

use crate::hpack::decode;

pub(crate) type Output<S, T> = Result<(S, T), (S, <S as Direct>::Error)>;

pub(crate) enum Begin<R, C> {
    Skip,
    Buffered(R),
    Direct(C),
}

pub(crate) enum Indexed<R> {
    Skip,
    Buffered(R),
}

const _: () =
    assert!(core::mem::size_of::<Indexed<usize>>() <= core::mem::size_of::<Begin<usize, ()>>());

pub(crate) trait Direct {
    type Error: From<decode::Error>;

    fn string(&mut self, cursor: &mut strings::Cursor, input: &[u8]) -> Result<(), Self::Error>;

    fn rollback(&mut self);
}

pub(crate) trait Store: Direct + Sized {
    type Rule: Copy;
    type Completion: Copy;

    fn validate(&mut self, value: block::Validation) -> Result<(), Self::Error>;

    fn begin(
        self,
        name: Option<block::Named<'_>>,
        indexing: Indexing,
    ) -> Output<Self, Begin<Self::Rule, Self::Completion>>;

    fn indexed(self, name: Option<block::Named<'_>>) -> Output<Self, Indexed<Self::Rule>>;

    fn field(
        self,
        rule: Self::Rule,
        value: block::Valued<'_>,
        indexing: Indexing,
        name: block::Named<'_>,
    ) -> Result<Self, (Self, Self::Error)>;

    fn direct(self, done: Self::Completion) -> Result<Self, (Self, Self::Error)>;
}
