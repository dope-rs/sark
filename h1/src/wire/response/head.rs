use std::{mem, ops::Range};

use o3::buffer::write::{self, Sink as _};
use sark_http::{
    field,
    h1::{
        codec::numbers::Wire as _,
        head::{
            line::Version,
            response::{Date, Server},
        },
    },
    payload::response::status::Code,
};

use crate::wire::response::{Context, Error, consts, framing};

pub(in crate::wire) struct Head<'a, H, F> {
    pub(in crate::wire) response: Context,
    pub(in crate::wire) status: Code,
    pub(in crate::wire) headers: &'a H,
    pub(in crate::wire) framing: F,
}

pub(in crate::wire) struct Plan {
    len: usize,
    encodings: usize,
}

pub(in crate::wire) trait Count: Copy + Default {
    fn add(&mut self) -> Result<(), Error>;
}

impl Count for () {
    fn add(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl Count for usize {
    fn add(&mut self) -> Result<(), Error> {
        let current = *self;
        *self = current
            .checked_add(1)
            .ok_or(Error::EncodingOverflow { current })?;
        Ok(())
    }
}

fn count<C: Count>(total: &mut C, name: &[u8]) -> Result<(), Error> {
    let Ok(name) = <&[u8; 16]>::try_from(name) else {
        return Ok(());
    };
    if name == b"content-encoding" {
        total.add()?;
    }
    Ok(())
}

fn count_group<C: Count>(total: &mut C, group: &field::Group<'_>) -> Result<(), Error> {
    for entry in group.entries() {
        count(total, entry.name())?;
    }
    Ok(())
}

impl<H, F> Head<'_, H, F>
where
    H: field::Source,
    F: framing::Framing,
{
    pub(in crate::wire) fn plan(&self, server: Server, dated: Date) -> Result<Plan, Error> {
        let mut measure = Measure::default();
        self.headers.emit(&mut measure);
        measure.add(Writer::status_len(self.status));
        measure.add(self.framing.wire_len());
        measure.add(self.response.connection_field().len());
        match server {
            Server::Omit => {}
            Server::Include => measure.add(consts::SERVER.len()),
        }
        match dated {
            Date::Omit => {}
            Date::Include => {
                measure.add(consts::DATE.len());
                measure.add(consts::DATE_LEN);
                measure.add(consts::CRLF.len());
            }
        }
        measure.add(consts::CRLF.len());
        measure.finish()
    }

    pub(in crate::wire) fn emit<C: Count>(
        &self,
        out: &mut Writer<'_>,
        date: &[u8; 29],
        server: Server,
        dated: Date,
    ) -> Result<(Option<usize>, C), Error> {
        out.put_status(self.response.version(), self.status)?;
        let count = out.fields(self.headers)?;
        self.framing.write(out)?;
        let off = out.end_head(self.response, date, server, dated)?;
        Ok((off, count))
    }
}

impl Writer<'_> {
    pub(in crate::wire) fn fields<C: Count>(
        &mut self,
        source: &impl field::Source,
    ) -> Result<C, Error> {
        let mut fields = Fields {
            out: self,
            state: Ok(C::default()),
        };
        source.emit(&mut fields);
        fields.state
    }
}

struct Fields<'a, 'out, C> {
    out: &'a mut Writer<'out>,
    state: Result<C, Error>,
}

#[derive(Default)]
struct Measure {
    len: usize,
    encodings: usize,
    error: Option<Error>,
}

impl<'field, C: Count> field::Sink<'field> for Fields<'_, '_, C> {
    /// Bulk admission commits bytes and count together; failures replay scalar writes.
    fn group(&mut self, group: &field::Group<'field>) {
        let Ok(mut total) = self.state else {
            return;
        };
        if count_group(&mut total, group).is_ok() && self.out.put(group.wire()).is_ok() {
            self.state = Ok(total);
            return;
        }
        for entry in group.entries() {
            self.entry(*entry);
        }
    }

    fn entry(&mut self, entry: field::Entry<'field>) {
        let Ok(total) = &mut self.state else {
            return;
        };
        let entry = entry.as_record();
        let (name, value) = (entry.name, entry.value);
        let result = (|| {
            self.out.put(name)?;
            self.out.put(b": ")?;
            self.out.put(value)?;
            self.out.put(consts::CRLF)?;
            count(total, name)
        })();
        if let Err(error) = result {
            self.state = Err(error);
        }
    }

    fn encoded_with<E: field::Encode>(
        &mut self,
        name: field::Lower<&'field [u8]>,
        encoded: E,
        _: field::Indexing,
    ) {
        let Ok(total) = &mut self.state else {
            return;
        };
        let result = (|| {
            let name = name.as_bytes();
            let len = encoded.encoded_len();
            self.out.put(name)?;
            self.out.put(b": ")?;
            let mut value = write::Slice::new(self.out.take(len)?);
            encoded
                .try_write(|bytes| value.write_slice(bytes))
                .map_err(Error::ValueCapacity)?;
            self.out.put(consts::CRLF)?;
            count(total, name)
        })();
        if let Err(error) = result {
            self.state = Err(error);
        }
    }
}

impl Measure {
    fn add(&mut self, added: usize) {
        if self.error.is_some() {
            return;
        }
        let current = self.len;
        match current.checked_add(added) {
            Some(len) => self.len = len,
            None => {
                self.error = Some(Error::PlanOverflow { current, added });
            }
        }
    }

    fn finish_field(&mut self, name: &[u8]) {
        self.add(2 * consts::CRLF.len());
        if self.error.is_some() {
            return;
        }
        self.error = count(&mut self.encodings, name).err();
    }

    fn finish(self) -> Result<Plan, Error> {
        if let Some(error) = self.error {
            return Err(error);
        }
        Ok(Plan {
            len: self.len,
            encodings: self.encodings,
        })
    }
}

impl<'field> field::Sink<'field> for Measure {
    fn entry(&mut self, entry: field::Entry<'field>) {
        let entry = entry.as_record();
        let (name, value) = (entry.name, entry.value);
        self.add(name.len());
        self.add(value.len());
        self.finish_field(name);
    }

    fn encoded_with<E: field::Encode>(
        &mut self,
        name: field::Lower<&'field [u8]>,
        value: E,
        _: field::Indexing,
    ) {
        let name = name.as_bytes();
        self.add(name.len());
        self.add(value.encoded_len());
        self.finish_field(name);
    }
}

impl Plan {
    pub(in crate::wire) const fn len(&self) -> usize {
        self.len
    }

    pub(in crate::wire) const fn encodings(&self) -> usize {
        self.encodings
    }
}

const _: () = assert!(mem::size_of::<Option<Error>>() <= 3 * mem::size_of::<usize>());

const OK: &[u8] = b"HTTP/1.1 200 OK\r\n";

pub(in crate::wire) struct Writer<'a> {
    out: &'a mut [u8],
    offset: usize,
}

fn get_mut(out: &mut [u8], start: usize, len: usize) -> Result<(&mut [u8], usize), Error> {
    let end = start.checked_add(len).ok_or(Error::WriteOverflow {
        current: start,
        added: len,
    })?;
    let actual = out.len();
    let out = out.get_mut(start..end).ok_or(Error::WriteCapacity {
        required: end,
        actual,
    })?;
    Ok((out, end))
}

impl<'a> Writer<'a> {
    pub(in crate::wire) fn new(out: &'a mut [u8], len: usize) -> Result<Self, Error> {
        let actual = out.len();
        Ok(Self {
            out: out.get_mut(..len).ok_or(Error::WriteCapacity {
                required: len,
                actual,
            })?,
            offset: 0,
        })
    }

    pub(in crate::wire) fn exact(out: &'a mut [u8]) -> Self {
        Self { out, offset: 0 }
    }

    pub(in crate::wire) fn reborrow(&mut self) -> Writer<'_> {
        Writer {
            out: self.out,
            offset: self.offset,
        }
    }

    pub(in crate::wire) fn get_mut(&mut self, range: Range<usize>) -> Option<&mut [u8]> {
        self.out.get_mut(range)
    }

    pub(in crate::wire) fn into_inner(self) -> &'a mut [u8] {
        self.out
    }

    pub(in crate::wire) fn status_len(status: Code) -> usize {
        if status == Code::OK {
            OK.len()
        } else {
            consts::STATUS.len() + 3 + 1 + status.reason().map_or(0, str::len) + consts::CRLF.len()
        }
    }

    pub(in crate::wire) fn len(&self) -> usize {
        self.offset
    }

    pub(in crate::wire) fn finish(self) -> usize {
        self.offset
    }

    pub(in crate::wire) fn put(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let (out, end) = get_mut(self.out, self.offset, bytes.len())?;
        out.copy_from_slice(bytes);
        self.offset = end;
        Ok(())
    }

    pub(in crate::wire) fn put_decimal(&mut self, value: usize) -> Result<(), Error> {
        self.offset = value.try_write(self.out, self.offset)?.get();
        Ok(())
    }

    pub(in crate::wire) fn take(&mut self, len: usize) -> Result<&mut [u8], Error> {
        let (out, end) = get_mut(self.out, self.offset, len)?;
        self.offset = end;
        Ok(out)
    }

    pub(in crate::wire) fn put_status(
        &mut self,
        version: Version,
        status: Code,
    ) -> Result<(), Error> {
        if status == Code::OK {
            return self.put(match version {
                Version::Ten => b"HTTP/1.0 200 OK\r\n",
                Version::Eleven => OK,
            });
        }
        self.put(version.as_bytes())?;
        self.put(b" ")?;
        self.put(&status.as_bytes())?;
        self.put(b" ")?;
        if let Some(reason) = status.reason() {
            self.put(reason.as_bytes())?;
        }
        self.put(consts::CRLF)
    }

    pub(in crate::wire) fn end_head(
        &mut self,
        response: Context,
        date: &[u8; 29],
        server: Server,
        dated: Date,
    ) -> Result<Option<usize>, Error> {
        let connection = response.connection_field();
        if !connection.is_empty() {
            self.put(connection)?;
        }
        match server {
            Server::Omit => {}
            Server::Include => self.put(consts::SERVER)?,
        }
        let off = match dated {
            Date::Omit => None,
            Date::Include => {
                self.put(consts::DATE)?;
                let off = self.len();
                self.put(date)?;
                self.put(consts::CRLF)?;
                Some(off)
            }
        };
        self.put(consts::CRLF)?;
        Ok(off)
    }
}
