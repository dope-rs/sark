use core::convert::Infallible;
use std::borrow::Cow;

use o3::alloc;

use crate::{
    field::Lower,
    h1::{
        codec::decode::{chunks, fields},
        framing,
        head::{Limit, ParseError, response},
    },
    payload::response::{Method, Response, headers::List, status},
};

mod head;

pub use head::Head;

pub type Collected<'field, B = Vec<u8>> =
    Response<'field, B, List<'field>, status::Code, Lower<Cow<'field, str>>>;

type Parsed<T, E> = Result<Option<T>, Error<E>>;

#[derive(Debug)]
pub enum Error<E> {
    Head(ParseError<E>),
    Framing(framing::Error),
    Chunk(chunks::Error),
    Allocation(alloc::Exhausted),
    IncompleteHead,
    IncompleteBody,
    ForbiddenContent { actual: usize },
}

pub enum Progress<'field> {
    More,
    Complete(Collected<'field>),
}

const _: () =
    assert!(core::mem::size_of::<Progress<'_>>() == core::mem::size_of::<Option<Collected<'_>>>());

pub struct Decoder {
    request: Method,
}

impl Decoder {
    pub const fn new(request: Method) -> Self {
        Self { request }
    }

    pub fn scan(&self, buf: &[u8], max: usize) -> Parsed<Head<()>, Infallible> {
        let Some(plan) =
            response::Parts::scan(buf, Limit::RESPONSE.with_bytes(max)).map_err(Error::Head)?
        else {
            return Ok(None);
        };
        let head = plan.admit(self.request).map_err(Error::Framing)?;
        let head = Head::from(head, ());
        Ok(Some(head))
    }

    pub fn parse<'input, F: fields::Sink<'input>>(
        &self,
        buf: &'input [u8],
        max: usize,
        mut fields: F,
    ) -> Parsed<Head<F>, F::Error> {
        let Some(plan) = response::Parts::parse(buf, Limit::RESPONSE.with_bytes(max), |field| {
            fields.entry(field)
        })
        .map_err(Error::Head)?
        else {
            return Ok(None);
        };
        let head = plan.admit(self.request).map_err(Error::Framing)?;
        let head = Head::from(head, fields);
        Ok(Some(head))
    }

    pub fn collect<'field>(&self, buf: &[u8]) -> Parsed<Head<List<'field>>, fields::Error> {
        self.parse(buf, buf.len(), List::new())
    }

    fn collect_body<'input, 'field>(
        &self,
        buf: &'input [u8],
    ) -> Parsed<(Head<List<'field>>, &'input [u8]), fields::Error> {
        let Some(head) = self.collect(buf)? else {
            return Ok(None);
        };
        // SAFETY: collect admitted this head's complete wire prefix on this
        // unchanged buffer; header_len therefore bounds its exact body suffix.
        let body = unsafe { buf.get_unchecked(head.header_len.get() as usize..) };
        Ok(Some((head, body)))
    }

    pub fn decode<'field>(&self, buf: &[u8]) -> Result<Progress<'field>, Error<fields::Error>> {
        let (head, body) = match self.collect_body(buf)? {
            Some(parts) => parts,
            None => return Ok(Progress::More),
        };

        match head.payload {
            framing::ResponseBody::None | framing::ResponseBody::Empty(framing::Empty::Fixed) => {
                Ok(Progress::Complete(head.complete(Vec::new(), List::new())))
            }
            framing::ResponseBody::Chunked
            | framing::ResponseBody::Empty(framing::Empty::Chunked) => {
                let Some(result) = chunks::Output::drain(body, chunks::Decoder::DEFAULT_MAX_BODY)
                    .map_err(Error::Chunk)?
                else {
                    return Ok(Progress::More);
                };
                if matches!(head.payload, framing::ResponseBody::Empty(_))
                    && !result.body.is_empty()
                {
                    return Err(Error::ForbiddenContent {
                        actual: result.body.len(),
                    });
                }
                Ok(Progress::Complete(
                    head.complete(result.body, result.trailers),
                ))
            }
            framing::ResponseBody::Fixed(len) if body.len() < len => Ok(Progress::More),
            framing::ResponseBody::Fixed(len) => head
                .try_copy(&body[..len])
                .map(Progress::Complete)
                .map_err(Error::Allocation),
            framing::ResponseBody::UntilEof
            | framing::ResponseBody::Empty(framing::Empty::UntilEof) => Ok(Progress::More),
        }
    }

    pub fn finish<'field>(&self, buf: &[u8]) -> Result<Collected<'field>, Error<fields::Error>> {
        let (head, body) = self.collect_body(buf)?.ok_or(Error::IncompleteHead)?;

        match head.payload {
            framing::ResponseBody::Chunked
            | framing::ResponseBody::Empty(framing::Empty::Chunked) => {
                let result = chunks::Decoder::require(body, chunks::Decoder::DEFAULT_MAX_BODY)
                    .map_err(Error::Chunk)?;
                if matches!(head.payload, framing::ResponseBody::Empty(_))
                    && !result.body.is_empty()
                {
                    return Err(Error::ForbiddenContent {
                        actual: result.body.len(),
                    });
                }
                Ok(head.complete(result.body, result.trailers))
            }
            framing::ResponseBody::Fixed(len) if body.len() < len => Err(Error::IncompleteBody),
            framing::ResponseBody::Fixed(len) => {
                head.try_copy(&body[..len]).map_err(Error::Allocation)
            }
            framing::ResponseBody::None => Ok(head.complete(Vec::new(), List::new())),
            framing::ResponseBody::UntilEof => head.try_copy(body).map_err(Error::Allocation),
            framing::ResponseBody::Empty(framing::Empty::Fixed)
            | framing::ResponseBody::Empty(framing::Empty::UntilEof) => {
                if body.is_empty() {
                    Ok(head.complete(Vec::new(), List::new()))
                } else {
                    Err(Error::ForbiddenContent { actual: body.len() })
                }
            }
        }
    }
}
