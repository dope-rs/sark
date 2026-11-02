use o3::buffer::write::Sink as _;
use sark_http::field::metadata::{
    block::{self, buffer},
    huffman::source,
    integer,
    map::record,
    strings,
};

use crate::hpack::decode::{self, limit, sink};

pub(super) const MAX_LITERAL_LEN: usize = 1 << 24;

pub(crate) struct Decoded {
    pub(super) len: usize,
    pub(super) retention: Retention,
}

#[repr(u8)]
#[derive(Clone, Copy)]
pub(crate) enum Retention {
    Retained,
    Counted,
}

#[derive(Clone, Copy)]
pub(crate) struct Encoded<'a> {
    pub(super) payload: &'a [u8],
    pub(super) encoding: strings::Encoding,
}

impl<'a> Encoded<'a> {
    pub(super) fn take(input: &mut &'a [u8]) -> Result<Self, decode::Error> {
        let Some(&first) = input.first() else {
            return Err(decode::Error::NeedMore);
        };
        let encoding = match first & 0x80 {
            0 => strings::Encoding::Plain,
            _ => strings::Encoding::Huffman,
        };
        let mut tail = *input;
        let len = integer::PrefixedInt::<7>::take(&mut tail)?.get();
        if len > MAX_LITERAL_LEN as u64 {
            return Err(decode::Error::BadString);
        }
        let (payload, tail) = tail
            .split_at_checked(len as usize)
            .ok_or(decode::Error::NeedMore)?;
        *input = tail;
        Ok(Self { payload, encoding })
    }

    pub(super) fn decode_named<'b>(
        self,
        out: &'b mut buffer::Names,
        cap: Option<usize>,
    ) -> Result<(usize, Option<block::Named<'b>>), decode::Error>
    where
        'a: 'b,
    {
        if matches!(self.encoding, strings::Encoding::Plain) {
            let name = match limit::Bound::new(cap).admit(self.payload.len()) {
                limit::Admission::Admitted => Some(block::Named::from_span(
                    record::Span::from_slice(self.payload),
                )),
                limit::Admission::Exceeded => None,
            };
            return Ok((self.payload.len(), name));
        }
        let decoded = self.decode_into(out, cap)?;
        let name = match decoded.retention {
            Retention::Retained => Some(out.named()),
            Retention::Counted => None,
        };
        Ok((decoded.len, name))
    }

    pub(super) fn decode_into<S: buffer::Scanner>(
        self,
        out: &mut buffer::Buffer<S>,
        cap: Option<usize>,
    ) -> Result<Decoded, decode::Error> {
        out.clear();
        let writer = out.writer(cap);
        let (writer, len) = match self.encoding {
            strings::Encoding::Huffman => match source::Encoded::new(self.payload).write_to(writer)
            {
                Ok(value) => value,
                Err(failure) => {
                    failure.sink.rollback();
                    return Err(decode::Error::retention(failure.cause, failure.progress));
                }
            },
            strings::Encoding::Plain => {
                let mut writer = writer;
                if let Err(error) = writer.write_slice(self.payload) {
                    writer.rollback();
                    return Err(decode::Error::Allocation(error));
                }
                (writer, self.payload.len())
            }
        };
        Ok(Decoded {
            len,
            retention: if writer.is_retained() {
                Retention::Retained
            } else {
                Retention::Counted
            },
        })
    }

    /// Complete plain literals retain the caller's input borrow through emission.
    pub(super) fn decode_span<'b>(
        self,
        out: &'b mut buffer::Values,
        cap: Option<usize>,
    ) -> Result<(usize, Option<block::Valued<'b>>), decode::Error>
    where
        'a: 'b,
    {
        if matches!(self.encoding, strings::Encoding::Plain) {
            let span = match limit::Bound::new(cap).admit(self.payload.len()) {
                limit::Admission::Admitted => Some(block::Valued::from_span(
                    record::Span::from_slice(self.payload),
                )),
                limit::Admission::Exceeded => None,
            };
            return Ok((self.payload.len(), span));
        }
        let decoded = self.decode_into(out, cap)?;
        let span = match decoded.retention {
            Retention::Retained => Some(out.valued()),
            Retention::Counted => None,
        };
        Ok((decoded.len, span))
    }

    pub(super) fn decoded_len(self) -> Result<Decoded, decode::Error> {
        let len = match self.encoding {
            strings::Encoding::Huffman => source::Encoded::new(self.payload)
                .decoded_len()
                .map_err(decode::Error::from)?,
            strings::Encoding::Plain => self.payload.len(),
        };
        Ok(Decoded {
            len,
            retention: Retention::Counted,
        })
    }

    pub(super) fn validate(self) -> Result<(Decoded, block::Validation), decode::Error> {
        let mut cursor = strings::Cursor::new(self.payload.len(), self.encoding);
        let (.., value) = cursor
            .feed_into(self.payload, block::Value::new())
            .map_err(|failure| decode::Error::encoding(failure.cause, failure.progress))?;
        let len = cursor.finish().map_err(decode::Error::from)?;
        Ok((
            Decoded {
                len,
                retention: Retention::Counted,
            },
            value.finish(),
        ))
    }

    pub(super) fn decode_to<S: sink::Direct>(self, sink: &mut S) -> Result<Decoded, S::Error> {
        let mut string = strings::Cursor::new(self.payload.len(), self.encoding);
        sink.string(&mut string, self.payload)?;
        let len = string.finish().map_err(decode::Error::from)?;
        Ok(Decoded {
            len,
            retention: Retention::Counted,
        })
    }
}

const _: () = {
    assert!(core::mem::size_of::<Retention>() == core::mem::size_of::<bool>());
};
