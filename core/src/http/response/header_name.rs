use http::HeaderName;

pub trait HeaderNameRef: sealed::SealedHeaderNameRef {
    fn as_header_name(&self) -> &str;
}

impl HeaderNameRef for HeaderName {
    fn as_header_name(&self) -> &str {
        self.as_str()
    }
}

impl HeaderNameRef for &HeaderName {
    fn as_header_name(&self) -> &str {
        self.as_str()
    }
}

impl HeaderNameRef for str {
    fn as_header_name(&self) -> &str {
        self
    }
}

impl HeaderNameRef for &str {
    fn as_header_name(&self) -> &str {
        self
    }
}

impl HeaderNameRef for String {
    fn as_header_name(&self) -> &str {
        self.as_str()
    }
}

pub trait IntoHeaderName: sealed::SealedIntoHeaderName {
    fn into_header_name(self) -> HeaderName;
}

impl IntoHeaderName for HeaderName {
    fn into_header_name(self) -> HeaderName {
        self
    }
}

impl IntoHeaderName for &HeaderName {
    fn into_header_name(self) -> HeaderName {
        self.clone()
    }
}

impl IntoHeaderName for &str {
    fn into_header_name(self) -> HeaderName {
        HeaderName::from_bytes(self.as_bytes()).expect("invalid header name")
    }
}

impl IntoHeaderName for String {
    fn into_header_name(self) -> HeaderName {
        HeaderName::from_bytes(self.as_bytes()).expect("invalid header name")
    }
}

mod sealed {
    pub trait SealedHeaderNameRef {}
    pub trait SealedIntoHeaderName {}

    impl SealedHeaderNameRef for http::HeaderName {}
    impl SealedHeaderNameRef for &http::HeaderName {}
    impl SealedHeaderNameRef for str {}
    impl SealedHeaderNameRef for &str {}
    impl SealedHeaderNameRef for String {}

    impl SealedIntoHeaderName for http::HeaderName {}
    impl SealedIntoHeaderName for &http::HeaderName {}
    impl SealedIntoHeaderName for &str {}
    impl SealedIntoHeaderName for String {}
}
