const METHOD: u8 = 1 << 0;
const SCHEME: u8 = 1 << 1;
const PATH: u8 = 1 << 2;
const STATUS: u8 = 1 << 3;
const REQUEST_REQUIRED: u8 = METHOD | SCHEME | PATH;

pub(super) trait HeaderKind {
    const REQUEST: bool;
}

pub(super) struct RequestHeaders;

impl HeaderKind for RequestHeaders {
    const REQUEST: bool = true;
}

pub(super) struct ResponseHeaders;

impl HeaderKind for ResponseHeaders {
    const REQUEST: bool = false;
}

pub(super) struct Validate {
    seen: u8,
    empty: u8,
    request: bool,
    saw_regular: bool,
    trailing: bool,
    invalid: bool,
}

impl Validate {
    pub(super) const fn new<K: HeaderKind>(trailing: bool) -> Self {
        Self {
            seen: 0,
            empty: 0,
            request: K::REQUEST,
            saw_regular: false,
            trailing,
            invalid: false,
        }
    }

    pub(super) fn field(&mut self, name: &[u8], value: &[u8]) -> bool {
        if self.invalid {
            return false;
        }
        if name.is_empty() {
            return self.reject();
        }
        if name[0] == b':' {
            if self.saw_regular || self.trailing {
                return self.reject();
            }
            return if self.request {
                match name {
                    b":method" => self.mark(METHOD, value.is_empty()),
                    b":scheme" => self.mark(SCHEME, value.is_empty()),
                    b":path" => self.mark(PATH, value.is_empty()),
                    b":authority" | b":protocol" => true,
                    _ => self.reject(),
                }
            } else {
                match name {
                    b":status" => self.mark(STATUS, false),
                    _ => self.reject(),
                }
            };
        }
        if name.iter().any(u8::is_ascii_uppercase) {
            return self.reject();
        }
        self.saw_regular = true;
        match name {
            b"connection" | b"keep-alive" | b"proxy-connection" | b"transfer-encoding"
            | b"upgrade" => self.reject(),
            b"te" if value != b"trailers" => self.reject(),
            _ => true,
        }
    }

    pub(super) fn finish(self) -> bool {
        if self.invalid {
            return false;
        }
        if self.trailing {
            return true;
        }
        if self.request {
            self.seen & REQUEST_REQUIRED == REQUEST_REQUIRED && self.empty & REQUEST_REQUIRED == 0
        } else {
            self.seen & STATUS != 0
        }
    }

    fn mark(&mut self, bit: u8, empty: bool) -> bool {
        if self.seen & bit != 0 {
            return self.reject();
        }
        self.seen |= bit;
        if empty {
            self.empty |= bit;
        }
        true
    }

    fn reject(&mut self) -> bool {
        self.invalid = true;
        false
    }
}
