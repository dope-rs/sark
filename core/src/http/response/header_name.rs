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

mod sealed {
    pub trait SealedHeaderNameRef {}

    impl SealedHeaderNameRef for http::HeaderName {}
    impl SealedHeaderNameRef for &http::HeaderName {}
    impl SealedHeaderNameRef for str {}
    impl SealedHeaderNameRef for &str {}
    impl SealedHeaderNameRef for String {}
}
