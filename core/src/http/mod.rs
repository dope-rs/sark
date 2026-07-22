pub mod body_kind;
pub mod compress;
pub mod field;
pub mod huffman;
mod request;
mod response;
pub mod varint;

pub mod codec;
pub mod head;

#[doc(hidden)]
pub mod __private {
    pub trait GeneratedResponse: 'static {
        type Fields;
        type Shape: super::Shape<'static>;

        const BODY_KIND: super::body_kind::ResponseKind;

        fn into_owned_shape(self) -> Self::Shape;
    }

    pub trait OwnedShape {}

    pub trait OwnedValue: 'static {}
}

pub use field::{Field, OwnedField};
pub use http::{Method, StatusCode};
pub use huffman::{HpackHuffman, HpackHuffmanError};
pub use o3::buffer::{Borrowed, Bytes, Retained};
pub use request::PathParamRanges;
pub use response::{
    Body, BodyInner, CHUNK_TERMINATOR, Chunked, DEFAULT_HEADER_CAPACITY, EncodedBody,
    EncodedResponse, EncodedResponseInner, FixedResponse, FixedResponseInner, HeadInner,
    HeaderItem, HeaderItemInner, HeaderList, HeaderNameRef, HeaderNameToken,
    HeaderStaticValueToken, HeaderValueInner, Headers, HeadersInner, HotBodyInner, HotHeadInner,
    HotTextInner, InlineHeaderValue, IntoBody, IntoHeaderName, IntoHeaderValue, IntoServeResponse,
    IterStream, MonoResponseInner, NeverStream, OwnedShape, Response, ResponsePlan,
    ResponsePlanInner, Serve, ServeInner, Shape, StaticResponseInner, Stream, TextBody, TextItem,
    TextItemInner, TextSpec, apply_head_skip,
};
pub use varint::VarInt;
