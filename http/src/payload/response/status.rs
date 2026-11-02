use std::num::NonZero;

/// An admitted three-digit HTTP response status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Code(NonZero<u16>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Error {
    actual: u16,
}

impl Code {
    pub const CONTINUE: Self = Self::exact::<100>();
    pub const SWITCHING_PROTOCOLS: Self = Self::exact::<101>();
    pub const PROCESSING: Self = Self::exact::<102>();
    pub const EARLY_HINTS: Self = Self::exact::<103>();
    pub const OK: Self = Self::exact::<200>();
    pub const CREATED: Self = Self::exact::<201>();
    pub const ACCEPTED: Self = Self::exact::<202>();
    pub const NON_AUTHORITATIVE_INFORMATION: Self = Self::exact::<203>();
    pub const NO_CONTENT: Self = Self::exact::<204>();
    pub const RESET_CONTENT: Self = Self::exact::<205>();
    pub const PARTIAL_CONTENT: Self = Self::exact::<206>();
    pub const MULTI_STATUS: Self = Self::exact::<207>();
    pub const ALREADY_REPORTED: Self = Self::exact::<208>();
    pub const IM_USED: Self = Self::exact::<226>();
    pub const MULTIPLE_CHOICES: Self = Self::exact::<300>();
    pub const MOVED_PERMANENTLY: Self = Self::exact::<301>();
    pub const FOUND: Self = Self::exact::<302>();
    pub const SEE_OTHER: Self = Self::exact::<303>();
    pub const NOT_MODIFIED: Self = Self::exact::<304>();
    pub const USE_PROXY: Self = Self::exact::<305>();
    pub const TEMPORARY_REDIRECT: Self = Self::exact::<307>();
    pub const PERMANENT_REDIRECT: Self = Self::exact::<308>();
    pub const BAD_REQUEST: Self = Self::exact::<400>();
    pub const UNAUTHORIZED: Self = Self::exact::<401>();
    pub const PAYMENT_REQUIRED: Self = Self::exact::<402>();
    pub const FORBIDDEN: Self = Self::exact::<403>();
    pub const NOT_FOUND: Self = Self::exact::<404>();
    pub const METHOD_NOT_ALLOWED: Self = Self::exact::<405>();
    pub const NOT_ACCEPTABLE: Self = Self::exact::<406>();
    pub const PROXY_AUTHENTICATION_REQUIRED: Self = Self::exact::<407>();
    pub const REQUEST_TIMEOUT: Self = Self::exact::<408>();
    pub const CONFLICT: Self = Self::exact::<409>();
    pub const GONE: Self = Self::exact::<410>();
    pub const LENGTH_REQUIRED: Self = Self::exact::<411>();
    pub const PRECONDITION_FAILED: Self = Self::exact::<412>();
    pub const PAYLOAD_TOO_LARGE: Self = Self::exact::<413>();
    pub const URI_TOO_LONG: Self = Self::exact::<414>();
    pub const UNSUPPORTED_MEDIA_TYPE: Self = Self::exact::<415>();
    pub const RANGE_NOT_SATISFIABLE: Self = Self::exact::<416>();
    pub const EXPECTATION_FAILED: Self = Self::exact::<417>();
    pub const IM_A_TEAPOT: Self = Self::exact::<418>();
    pub const MISDIRECTED_REQUEST: Self = Self::exact::<421>();
    pub const UNPROCESSABLE_ENTITY: Self = Self::exact::<422>();
    pub const LOCKED: Self = Self::exact::<423>();
    pub const FAILED_DEPENDENCY: Self = Self::exact::<424>();
    pub const TOO_EARLY: Self = Self::exact::<425>();
    pub const UPGRADE_REQUIRED: Self = Self::exact::<426>();
    pub const PRECONDITION_REQUIRED: Self = Self::exact::<428>();
    pub const TOO_MANY_REQUESTS: Self = Self::exact::<429>();
    pub const REQUEST_HEADER_FIELDS_TOO_LARGE: Self = Self::exact::<431>();
    pub const UNAVAILABLE_FOR_LEGAL_REASONS: Self = Self::exact::<451>();
    pub const INTERNAL_SERVER_ERROR: Self = Self::exact::<500>();
    pub const NOT_IMPLEMENTED: Self = Self::exact::<501>();
    pub const BAD_GATEWAY: Self = Self::exact::<502>();
    pub const SERVICE_UNAVAILABLE: Self = Self::exact::<503>();
    pub const GATEWAY_TIMEOUT: Self = Self::exact::<504>();
    pub const HTTP_VERSION_NOT_SUPPORTED: Self = Self::exact::<505>();
    pub const VARIANT_ALSO_NEGOTIATES: Self = Self::exact::<506>();
    pub const INSUFFICIENT_STORAGE: Self = Self::exact::<507>();
    pub const LOOP_DETECTED: Self = Self::exact::<508>();
    pub const NOT_EXTENDED: Self = Self::exact::<510>();
    pub const NETWORK_AUTHENTICATION_REQUIRED: Self = Self::exact::<511>();

    /// Admits known and extension statuses within the five HTTP status classes.
    pub const fn from_u16(actual: u16) -> Result<Self, Error> {
        if actual < 100 || actual > 599 {
            return Err(Error { actual });
        }
        Ok(Self(NonZero::<u16>::MIN.saturating_add(actual - 1)))
    }

    pub const fn as_u16(&self) -> u16 {
        self.0.get()
    }

    pub const fn as_bytes(&self) -> [u8; 3] {
        let value = self.as_u16();
        [
            b'0' + (value / 100) as u8,
            b'0' + ((value / 10) % 10) as u8,
            b'0' + (value % 10) as u8,
        ]
    }

    pub const fn is_info(&self) -> bool {
        self.as_u16() / 100 == 1
    }

    pub const fn is_success(&self) -> bool {
        self.as_u16() / 100 == 2
    }

    pub const fn reason(&self) -> Option<&str> {
        match self.as_u16() {
            100 => Some("Continue"),
            101 => Some("Switching Protocols"),
            102 => Some("Processing"),
            103 => Some("Early Hints"),
            200 => Some("OK"),
            201 => Some("Created"),
            202 => Some("Accepted"),
            203 => Some("Non Authoritative Information"),
            204 => Some("No Content"),
            205 => Some("Reset Content"),
            206 => Some("Partial Content"),
            207 => Some("Multi-Status"),
            208 => Some("Already Reported"),
            226 => Some("IM Used"),
            300 => Some("Multiple Choices"),
            301 => Some("Moved Permanently"),
            302 => Some("Found"),
            303 => Some("See Other"),
            304 => Some("Not Modified"),
            305 => Some("Use Proxy"),
            307 => Some("Temporary Redirect"),
            308 => Some("Permanent Redirect"),
            400 => Some("Bad Request"),
            401 => Some("Unauthorized"),
            402 => Some("Payment Required"),
            403 => Some("Forbidden"),
            404 => Some("Not Found"),
            405 => Some("Method Not Allowed"),
            406 => Some("Not Acceptable"),
            407 => Some("Proxy Authentication Required"),
            408 => Some("Request Timeout"),
            409 => Some("Conflict"),
            410 => Some("Gone"),
            411 => Some("Length Required"),
            412 => Some("Precondition Failed"),
            413 => Some("Payload Too Large"),
            414 => Some("URI Too Long"),
            415 => Some("Unsupported Media Type"),
            416 => Some("Range Not Satisfiable"),
            417 => Some("Expectation Failed"),
            418 => Some("I'm a teapot"),
            421 => Some("Misdirected Request"),
            422 => Some("Unprocessable Entity"),
            423 => Some("Locked"),
            424 => Some("Failed Dependency"),
            425 => Some("Too Early"),
            426 => Some("Upgrade Required"),
            428 => Some("Precondition Required"),
            429 => Some("Too Many Requests"),
            431 => Some("Request Header Fields Too Large"),
            451 => Some("Unavailable For Legal Reasons"),
            500 => Some("Internal Server Error"),
            501 => Some("Not Implemented"),
            502 => Some("Bad Gateway"),
            503 => Some("Service Unavailable"),
            504 => Some("Gateway Timeout"),
            505 => Some("HTTP Version Not Supported"),
            506 => Some("Variant Also Negotiates"),
            507 => Some("Insufficient Storage"),
            508 => Some("Loop Detected"),
            510 => Some("Not Extended"),
            511 => Some("Network Authentication Required"),
            _ => None,
        }
    }

    const fn exact<const VALUE: u16>() -> Self {
        const { assert!(VALUE >= 100 && VALUE <= 599) };
        Self(NonZero::<u16>::MIN.saturating_add(VALUE - 1))
    }
}

impl Default for Code {
    fn default() -> Self {
        Self::OK
    }
}

impl From<Code> for u16 {
    fn from(code: Code) -> Self {
        code.as_u16()
    }
}

impl Error {
    pub const fn actual(self) -> u16 {
        self.actual
    }
}

const _: () = {
    assert!(size_of::<Code>() == size_of::<u16>());
    assert!(size_of::<Option<Code>>() == size_of::<Code>());
    assert!(size_of::<Error>() == size_of::<u16>());
    assert!(!std::mem::needs_drop::<Code>());
    assert!(!std::mem::needs_drop::<Error>());
};
