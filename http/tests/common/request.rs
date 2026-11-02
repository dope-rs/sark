use sark_http::{field::metadata::map::record::Entry, request::Form};

pub struct Layout {
    pub fields: &'static [Entry<'static>],
    pub form: Form,
    pub target: &'static [u8],
    pub authority: &'static [u8],
    pub route: &'static [u8],
    pub query: Option<&'static [u8]>,
}

pub const LAYOUTS: &[Layout] = &[
    Layout {
        fields: &[
            Entry::new(b":method", b"GET"),
            Entry::new(b":scheme", b"https"),
            Entry::new(b":authority", b"EXAMPLE.com:443"),
            Entry::new(b":path", b"/items?q=rust"),
            Entry::new(b"host", b"example.com"),
        ],
        form: Form::Path,
        target: b"/items?q=rust",
        authority: b"EXAMPLE.com:443",
        route: b"/items",
        query: Some(b"q=rust"),
    },
    Layout {
        fields: &[
            Entry::new(b":method", b"GET"),
            Entry::new(b":scheme", b"https"),
            Entry::new(b":path", b"/x"),
            Entry::new(b"host", b"host.example"),
        ],
        form: Form::Path,
        target: b"/x",
        authority: b"host.example",
        route: b"/x",
        query: None,
    },
    Layout {
        fields: &[
            Entry::new(b":authority", b"proxy.example:8443"),
            Entry::new(b":method", b"CONNECT"),
        ],
        form: Form::Authority,
        target: b"proxy.example:8443",
        authority: b"proxy.example:8443",
        route: b"*",
        query: None,
    },
    Layout {
        fields: &[
            Entry::new(b":path", b"/chat"),
            Entry::new(b":authority", b"example.com"),
            Entry::new(b":protocol", b"websocket"),
            Entry::new(b":scheme", b"https"),
            Entry::new(b":method", b"CONNECT"),
        ],
        form: Form::Extended,
        target: b"/chat",
        authority: b"example.com",
        route: b"/chat",
        query: None,
    },
];
