use http::StatusCode;
use o3::buffer::{Owned, Shared};

use super::super::{HotBodyInner, HotHeadInner, IntoBody, MonoResponseInner};
use super::head::HeadInner;
use super::headers::{HeaderAssert, HeaderNameToken, HeaderStaticValueToken, HeadersInner};
use super::value::{InlineHeaderValue, TextSpec};
use crate::http::request::LocalFrameBytesRef;

#[derive(Clone, Debug)]
pub struct ResponsePlanInner<'req> {
    pub(in crate::http::response) status: StatusCode,
    pub(in crate::http::response) head: HeadInner<'req>,
}

pub type ResponsePlan = ResponsePlanInner<'static>;

impl<'req> ResponsePlanInner<'req> {
    pub fn with_capacity(status: StatusCode) -> Self {
        Self {
            status,
            head: HeadInner::new(b"", HeadersInner::new()),
        }
    }

    pub fn from_static(status: StatusCode, static_headers: &'static [u8]) -> Self {
        Self {
            status,
            head: HeadInner::new(static_headers, HeadersInner::new()),
        }
    }

    pub fn new(status: StatusCode) -> Self {
        Self::with_capacity(status)
    }

    pub fn ok() -> Self {
        Self::new(StatusCode::OK)
    }

    pub fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND)
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn wire_headers(&self) -> Shared {
        let mut out = Owned::with_capacity(self.head.wire_len());
        self.head.write_into(&mut out);
        out.freeze()
    }

    pub fn push_static(&mut self, name: &'static str, value: &'static str) -> &mut Self {
        HeaderAssert::name(name);
        HeaderAssert::value(value);
        let token = HeaderStaticValueToken::new(value);
        self.head.headers_mut().push_static(name, token);
        self
    }

    pub fn push(&mut self, name: &'static str, value: &str) -> &mut Self {
        HeaderAssert::name(name);
        self.push_str_value(name, value)
    }

    pub fn push_token_static(
        &mut self,
        name: HeaderNameToken,
        value: HeaderStaticValueToken,
    ) -> &mut Self {
        self.head.headers_mut().push_static(name.as_str(), value);
        self
    }

    pub fn push_token(&mut self, name: HeaderNameToken, value: &str) -> &mut Self {
        self.push_str_value(name.as_str(), value)
    }

    fn push_str_value(&mut self, name: &'static str, value: &str) -> &mut Self {
        HeaderAssert::value(value);
        if value.len() <= 31 {
            self.head
                .headers_mut()
                .push_inline(name, InlineHeaderValue::new(value.as_bytes()));
        } else {
            self.head
                .headers_mut()
                .push_shared(name, Shared::copy_from_slice(value.as_bytes()));
        }
        self
    }

    pub fn push_local_token(
        &mut self,
        name: HeaderNameToken,
        value: LocalFrameBytesRef<'req>,
    ) -> &mut Self {
        HeaderAssert::value_bytes(value.as_bytes());
        self.head.headers_mut().push_local(name.as_str(), value);
        self
    }

    pub fn respond_mono<B>(self, body: B) -> MonoResponseInner<'req>
    where
        B: IntoBody<'req>,
    {
        let status = self.status;
        MonoResponseInner {
            status,
            headers: None,
            head: HotHeadInner::Direct(self.head),
            body: HotBodyInner::from(body.into_response_body()),
        }
    }

    pub fn respond_text<T>(self, body: T) -> MonoResponseInner<'req>
    where
        T: TextSpec<'req>,
    {
        let status = self.status;
        MonoResponseInner {
            status,
            headers: None,
            head: HotHeadInner::Direct(self.head),
            body: HotBodyInner::Text(body.into_hot_text()),
        }
    }
}
