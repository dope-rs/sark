use std::cell::Cell;

use o3::alloc;
use sark_h1::wire::{output, response::Context};
use sark_http::{
    field,
    h1::head::line::Version,
    payload::response::{Method, headers},
};

pub const DATE: &[u8; 29] = b"Mon, 01 Jan 2026 00:00:00 GMT";
pub const RESPONSE: Context = Context::new(Method::Other, Version::Eleven);
pub const ENCODED_BODY: [u8; 256] = [b'x'; 256];
pub const DRIFT_VALUES: &[(&[u8], &[u8])] = &[
    (b"short", b"substantially-longer"),
    (b"substantially-longer", b"short"),
];

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

const GROUP_ENTRIES: [field::Entry<'static>; 2] = [ENTRIES[0].as_field(), ENTRIES[1].as_field()];

pub const GROUP: headers::Group<'static> =
    field::literal!(headers::Group::try_new(field::literal!(
        field::Group::try_new(&GROUP_ENTRIES, b"x-first: one\r\nx-second:  two\r\n",)
    )));

pub const IDENTITY: headers::Entry<'static> = headers::Entry::new(
    field::literal!(headers::Name::try_bytes(b"content-encoding")),
    field::literal!(field::Value::try_bytes(b"identity")),
);

const ENCODING_ENTRIES: [field::Entry<'static>; 1] = [IDENTITY.as_field()];

pub const GROUP_ENCODING: headers::Group<'static> =
    field::literal!(headers::Group::try_new(field::literal!(
        field::Group::try_new(&ENCODING_ENTRIES, b"content-encoding: identity\r\n",)
    )));

pub struct Counted<'a> {
    pub emissions: &'a Cell<usize>,
}

impl headers::Source for Counted<'_> {
    fn emit<'field>(&'field self, sink: &mut impl headers::Sink<'field>) {
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
    source: impl headers::Source,
    input: &'d [u8],
) -> Result<output::Outcome<'d>, alloc::Error> {
    use sark_h1::{route, wire::cache};
    use sark_http::{
        h1::head::{
            request::Coding,
            response::{Date, Server},
        },
        owner::handoff,
        payload::{
            compress::{Compression as _, Gzip},
            fixed::Body,
            response::{fixed, status::Code},
        },
    };
    let response = fixed::Direct::new(Code::OK, source, Body::new(input));
    let mut bytes = [0; 512];
    let mut gzip = Gzip::try_new()?;
    Ok(output::Egress::new(
        o3::buffer::write::Slice::new(&mut bytes).reborrow(),
        DATE,
        RESPONSE,
    )
    .route(
        response,
        handoff::Slot::ready(None),
        cache::Cache::empty(),
        &mut gzip,
        Coding::Gzip,
        route::Policy::new(
            route::Cache::Bypass,
            Coding::Gzip,
            Date::Include,
            Server::Include,
        ),
    ))
}

impl headers::Source for Generated<'_> {
    fn emit<'field>(&'field self, sink: &mut impl headers::Sink<'field>) {
        sink.generated(self.name, self.value);
    }
}

pub struct Drifting<'a> {
    pub emissions: &'a Cell<usize>,
    pub first: field::Value<&'a [u8]>,
    pub second: field::Value<&'a [u8]>,
}

impl headers::Source for Drifting<'_> {
    fn emit<'field>(&'field self, sink: &mut impl headers::Sink<'field>) {
        let emission = self.emissions.get();
        self.emissions.set(self.emissions.get() + 1);
        let value = if emission == 0 {
            self.first
        } else {
            self.second
        };
        let name = field::literal!(headers::Name::try_bytes(b"x-drift"));
        sink.field(name, value);
    }
}
