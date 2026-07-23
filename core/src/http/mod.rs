pub mod body_kind;
pub mod compress;
pub mod field;
pub mod huffman;
mod prefixed_int;
mod request;
mod response;
pub mod scan;
pub mod varint;

pub mod codec;
pub mod head;

#[doc(hidden)]
pub mod __private {
    pub trait GeneratedResponse: 'static {
        type Shape: super::Shape<'static>;

        const BODY_KIND: super::body_kind::ResponseKind;

        fn into_owned_shape(self) -> Self::Shape;
    }
}

pub use field::{
    Field, FieldBlock, FieldStorage, FieldValueWriter, OwnedField, OwnedFieldBlock,
    PackedFieldIter, PackedFields, PooledFieldBlock, VecFieldBlock,
};
pub use http::{Method, StatusCode};
pub use huffman::{HpackHuffman, HpackHuffmanError};
pub use o3::buffer::{Borrowed, Bytes, Retained};
pub use prefixed_int::{PrefixedInt, PrefixedIntError};
pub use request::PathParamRanges;
pub use response::{
    Body, CHUNK_TERMINATOR, CacheTemplate, Chunked, DEFAULT_HEADER_CAPACITY, Egress, EncodedBody,
    EncodedResponse, FixedResponse, HeadInner, HeaderItem, HeaderList, HeaderNameRef,
    HeaderNameToken, HeaderStaticValueToken, HeaderValueInner, Headers, HotBodyInner, HotHeadInner,
    InlineHeaderValue, IterStream, MonoResponseInner, NeverStream, OwnedShape, Preparation,
    Prepared, Response, ResponsePlan, ResponseView, Serve, Shape, StaticResponseInner, Stream,
    TextBody, TextItem,
};
pub use varint::VarInt;
