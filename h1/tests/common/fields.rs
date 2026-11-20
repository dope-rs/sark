use std::cell::Cell;

use o3::alloc;
use sark_h1::{
    route,
    wire::{output, response::Context},
};
use sark_http::{
    field,
    h1::head::{
        line::Version,
        request::Coding,
        response::{Date, Server},
    },
    payload::response::{Method, headers},
};

pub const DATE: &[u8; 29] = b"Mon, 01 Jan 2026 00:00:00 GMT";
pub const RESPONSE: Context = Context::new(Method::Other, Version::Eleven);
pub const GZIP_RESPONSE: Context = RESPONSE.with_coding(Coding::Gzip);
pub const GZIP_POLICY: route::Policy =
    route::Policy::new(route::Mode::Gzip, Date::Include, Server::Include);
pub const ENCODED_BODY: [u8; 256] = [b'x'; 256];

pub const ENTRIES: [headers::Entry<'static>; 2] = [
    headers::Entry::new(
        field::literal!(headers::Name::try_bytes(b"x-first")),
        field::literal!(field::Value::try_bytes(b"one")),
    ),
    headers::Entry::new(
        field::literal!(headers::Name::try_bytes(b"x-second")),
        field::literal!(field::Value::try_bytes(b" two")),
    ),
];

pub const GROUP: headers::Group<'static> = field::literal!(headers::Group::try_new(
    &ENTRIES,
    b"x-first: one\r\nx-second:  two\r\n",
));

pub const IDENTITY: headers::Entry<'static> = headers::Entry::new(
    field::literal!(headers::Name::try_bytes(b"content-encoding")),
    field::literal!(field::Value::try_bytes(b"identity")),
);

pub const GROUP_ENCODING: headers::Group<'static> = field::literal!(headers::Group::try_new(
    &[IDENTITY],
    b"content-encoding: identity\r\n",
));

pub struct Counted<'a> {
    pub emissions: &'a Cell<usize>,
}

impl field::Source<headers::Application> for Counted<'_> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field, headers::Application>) {
        self.emissions.set(self.emissions.get() + 1);
        sink.entry(headers::Entry::new(
            field::literal!(headers::Name::try_bytes(b"x-actual")),
            field::literal!(field::Value::try_bytes(b"present")),
        ));
    }
}

pub struct Generated<'a> {
    pub name: headers::Name<&'a [u8]>,
    pub value: field::Value<&'a [u8]>,
}

pub const GENERATED_IDENTITY: Generated<'static> = Generated {
    name: field::literal!(headers::Name::try_bytes(b"content-encoding")),
    value: field::literal!(field::Value::try_bytes(b"identity")),
};

pub fn try_encode<'d>(
    source: impl field::Source<headers::Application>,
    input: &'d [u8],
) -> Result<output::Outcome<'d>, alloc::Error> {
    use sark_http::{
        owner::handoff,
        payload::{
            compress::{Compression as _, Gzip},
            fixed::Body,
            response::{Fixed, status::Code},
        },
    };
    let response = Fixed::new(Code::OK, source, Body::new(input));
    let mut bytes = [0; 512];
    let mut gzip = Gzip::try_new()?;
    Ok(output::Egress::new(
        o3::buffer::write::Slice::new(&mut bytes).reborrow(),
        DATE,
        GZIP_RESPONSE,
    )
    .route(response, handoff::Slot::ready(None), &mut gzip, GZIP_POLICY))
}

impl field::Source<headers::Application> for Generated<'_> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field, headers::Application>) {
        sink.generated(self.name, self.value);
    }
}

pub struct Drifting<'a> {
    pub emissions: &'a Cell<usize>,
    pub first: headers::Entry<'a>,
    pub second: headers::Entry<'a>,
}

impl<'a> Drifting<'a> {
    pub fn values(emissions: &'a Cell<usize>, first: &'a [u8], second: &'a [u8]) -> Self {
        let name = field::literal!(headers::Name::try_bytes(b"x-drift"));
        Self {
            emissions,
            first: headers::Entry::new(name, field::Value::try_bytes(first).expect("first")),
            second: headers::Entry::new(name, field::Value::try_bytes(second).expect("second")),
        }
    }
}

impl field::Source<headers::Application> for Drifting<'_> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field, headers::Application>) {
        let index = self.emissions.replace(self.emissions.get() + 1);
        sink.entry(if index == 0 { self.first } else { self.second });
    }
}
