use http::StatusCode;
use o3::buffer::{Borrowed, Bytes, Owned, Retained, Shared};

use super::super::{HotBodyInner, HotHeadInner, IntoBody, MonoResponseInner};
use super::head::HeadInner;
use super::headers::{HeaderAssert, HeaderNameToken, HeaderStaticValueToken, HeadersInner};
use super::value::{InlineHeaderValue, TextSpec};

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
        self.head.write_into_owned(&mut out);
        out.freeze()
    }

    pub fn push_static(&mut self, name: &'static str, value: &'static str) -> &mut Self {
        let name = HeaderNameToken::new(name);
        let value = HeaderStaticValueToken::new(value);
        self.head.headers_mut().push_static(name, value);
        self
    }

    pub fn push(&mut self, name: &'static str, value: &str) -> &mut Self {
        self.push_str_value(HeaderNameToken::new(name), value)
    }

    pub fn push_token_static(
        &mut self,
        name: HeaderNameToken,
        value: HeaderStaticValueToken,
    ) -> &mut Self {
        self.head.headers_mut().push_static(name, value);
        self
    }

    pub fn push_token(&mut self, name: HeaderNameToken, value: &str) -> &mut Self {
        self.push_str_value(name, value)
    }

    fn push_str_value(&mut self, name: HeaderNameToken, value: &str) -> &mut Self {
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

    pub fn push_borrowed_token(
        &mut self,
        name: HeaderNameToken,
        value: Bytes<Borrowed<'req>>,
    ) -> &mut Self {
        HeaderAssert::value_bytes(value.as_slice());
        self.head.headers_mut().push_borrowed(name, value);
        self
    }

    pub fn push_retained_token(
        &mut self,
        name: HeaderNameToken,
        value: Bytes<Retained>,
    ) -> &mut Self {
        HeaderAssert::value_bytes(value.as_slice());
        self.head.headers_mut().push_retained(name, value);
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
