use sark_core::route::Schema;
use sark_h1::{
    Input,
    wire::{head, parsing},
};
use sark_http::{field::head::AcceptEncoding, h1::head::line::error, request::span::Span};

#[sark_fiber::request]
pub struct HdrReq {
    #[header("x-token", default = "none")]
    pub x_token: Shared,
    #[header("accept-encoding")]
    pub encoding: Option<AcceptEncoding>,
}

#[sark_fiber::request]
pub struct ShortHdrReq {
    #[header("x", default = "")]
    pub x: Shared,
}

#[sark_fiber::request]
pub struct EncodingReq {
    #[header("accept-encoding", default = "")]
    pub accept_encoding: Shared,
}

#[sark_fiber::request]
pub struct Minimal {}

#[sark_fiber::request(full)]
pub struct Full {}

#[sark_fiber::request]
pub struct QueryReq {
    #[query("count", default = "0")]
    pub count: usize,
    #[query("limit", default = "0")]
    pub limit: u64,
    #[query("flag", default = "false")]
    pub flag: bool,
}

#[sark_fiber::request(ordered)]
pub struct Ordered {
    #[query("count", default = "0")]
    pub count: usize,
    #[query("limit", default = "0")]
    pub limit: u64,
}

pub enum Failure<R: Schema> {
    Line(error::Request),
    Head(head::Error<R>),
}

pub struct Wire<'a> {
    bytes: &'a [u8],
}

impl<'a> Wire<'a> {
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub fn try_request<R: Schema>(
        self,
        max: usize,
    ) -> Result<Option<head::Parsed<R::RawHeaders>>, Failure<R>> {
        let Some(request) =
            parsing::Head::parse(Input::Borrowed(self.bytes), u8::MAX).map_err(Failure::Line)?
        else {
            return Ok(None);
        };
        head::Parsed::<R::RawHeaders>::parse::<R>(&request, max).map_err(Failure::Head)
    }
}

pub mod agnostic;

pub mod hello;
pub mod timer;

pub mod storage;

pub mod guard;

pub mod hygiene;

/// Observes values retained by the generated schema without exposing its raw fields.
pub trait Captured {
    type Output;
    fn captured(&self) -> Self::Output;
}

impl<'req> Captured for HdrReqHeaders<'req> {
    type Output = (&'req [u8], Option<AcceptEncoding>);
    fn captured(&self) -> Self::Output {
        (self.x_token, self.encoding)
    }
}

impl Captured for EncodingReqHeadersRaw {
    type Output = Option<Span>;
    fn captured(&self) -> Self::Output {
        self.accept_encoding
    }
}

impl Captured for ShortHdrReqHeadersRaw {
    type Output = Option<Span>;
    fn captured(&self) -> Self::Output {
        self.x
    }
}

pub mod empty;
pub mod task;
