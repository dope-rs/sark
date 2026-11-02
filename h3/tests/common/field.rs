use std::cell::Cell;

use sark_http::field::{self, Lower, Pseudo, Value};

#[derive(Debug)]
pub struct Switching<S> {
    pub calls: Cell<usize>,
    pub first: S,
    pub next: S,
}

impl<S: field::Source> field::Source for Switching<S> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field>) {
        let calls = self.calls.get();
        self.calls.set(calls.saturating_add(1));
        if calls == 0 {
            self.first.emit(sink);
        } else {
            self.next.emit(sink);
        }
    }
}

/// Admits fixture bytes once; literal callers evaluate this at compile time.
pub const fn entry<'a>(name: &'a [u8], value: &'a [u8]) -> Result<field::Entry<'a>, field::Error> {
    let value = match Value::try_bytes(value) {
        Ok(value) => value,
        Err(error) => return Err(error),
    };
    let pseudo = match name {
        b":method" => Pseudo::Method,
        b":scheme" => Pseudo::Scheme,
        b":authority" => Pseudo::Authority,
        b":path" => Pseudo::Path,
        b":protocol" => Pseudo::Protocol,
        b":status" => Pseudo::Status,
        _ => {
            let name = match Lower::try_bytes(name) {
                Ok(name) => name,
                Err(error) => return Err(error),
            };
            return Ok(field::Entry::new(name, value));
        }
    };
    Ok(field::Entry::pseudo(pseudo, value))
}
