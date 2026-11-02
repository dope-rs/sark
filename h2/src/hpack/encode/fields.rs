use dynamic::table::delta;
use sark_http::{
    self, field,
    field::{
        Entry, Lower,
        metadata::{block::Connect, integer},
    },
    payload::{compress::dynamic, response},
};

use crate::hpack::encode;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Dynamic(delta::Fault),
    ConnectDisabled,
    List {
        used: usize,
        name: usize,
        value: usize,
        limit: usize,
    },
    ListOverflow {
        used: usize,
        name: usize,
        value: usize,
    },
}

/// Bounded transaction over an encoder and its reusable output block.
pub(super) struct Sink<'a, W> {
    draft: delta::Draft<encode::Encoder>,
    out: &'a mut W,
    used: usize,
    limit: usize,
    error: Option<Error>,
    mode: encode::Mode,
    connect: Connect,
    method: Option<&'a mut response::Method>,
}

impl<W> Sink<'_, W>
where
    W: Extend<u8> + field::Writer,
{
    pub(crate) fn new<'a>(
        encoder: encode::Encoder,
        out: &'a mut W,
        limit: usize,
        connect: Connect,
        method: Option<&'a mut response::Method>,
    ) -> Sink<'a, W> {
        let mode = encoder.mode;
        encoder.prefix(out);
        let draft = delta::Draft::new(encoder);
        Sink {
            draft,
            out,
            used: 0,
            limit,
            error: None,
            mode,
            connect,
            method,
        }
    }

    fn admit(&mut self, nlen: usize, vlen: usize) {
        if self.error.is_some() {
            return;
        }
        let Some(next) = self
            .used
            .checked_add(nlen)
            .and_then(|used| used.checked_add(vlen))
            .and_then(|used| used.checked_add(dynamic::ENTRY_OVERHEAD))
        else {
            self.error = Some(Error::ListOverflow {
                used: self.used,
                name: nlen,
                value: vlen,
            });
            return;
        };
        if next > self.limit {
            self.error = Some(Error::List {
                used: self.used,
                name: nlen,
                value: vlen,
                limit: self.limit,
            });
            return;
        }
        self.used = next;
    }

    pub(super) fn finish(self) -> (Result<(), Error>, encode::Commit) {
        let result = match self.error {
            Some(error) => Err(error),
            None => Ok(()),
        };
        (result, encode::Commit::new(self.draft.finish()))
    }
}

impl<'field, 'f, W> field::Sink<'field> for Sink<'f, W>
where
    W: Extend<u8> + field::Writer,
{
    fn entry(&mut self, header: Entry<'field>) {
        self.admit(header.name().len(), header.value().as_bytes().len());
        if self.error.is_some() {
            return;
        }
        if header.name() == b":protocol" && self.connect == Connect::Disabled {
            self.error = Some(Error::ConnectDisabled);
            return;
        }
        if let Err(cause) =
            encode::Encoder::encode_field(&mut self.draft, header.as_record(), self.out, self.mode)
        {
            self.error = Some(Error::Dynamic(cause));
            return;
        }
        // Pseudo-names enter through Entry; generated names prove lowercase tokens.
        if let Some(method) = self.method.as_deref_mut()
            && header.name() == b":method"
        {
            *method = response::Method::from_bytes(header.value().as_bytes());
        }
    }

    fn encoded_with<E: field::Encode>(
        &mut self,
        name: Lower<&'field [u8]>,
        encoded: E,
        indexing: field::Indexing,
    ) {
        let name = name.as_ref();
        let vlen = encoded.encoded_len();
        self.admit(name.len(), vlen);
        if self.error.is_some() {
            return;
        }
        encode::Encoder::encode_name(&self.draft, name, indexing, self.out, self.mode);
        integer::PrefixedInt::<7>::new(vlen as u64).encode(0, self.out);
        encoded.encode(self.out);
    }
}
