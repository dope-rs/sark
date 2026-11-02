use mode::Direct as _;
use o3::buffer::write::Sink;
use sark_http::{
    self,
    field::metadata::{
        block::buffer,
        huffman::{decoder, source},
        strings::{Cursor, Encoding},
    },
    payload::compress::dynamic::insert::{self, stages, write},
};

use crate::hpack::decode::{self, decoder::Machine, limit, mode, strings};

pub(in crate::hpack::decode) struct Payload<'a, T> {
    stage: &'a mut T,
}

pub(in crate::hpack::decode) struct Scratch<'a> {
    decoder: &'a mut Machine<stages::Ready>,
}

#[repr(u8)]
pub(in crate::hpack::decode) enum Outcome {
    Stored,
    Overflow,
}

const _: () = assert!(std::mem::size_of::<Outcome>() == std::mem::size_of::<bool>());

impl Outcome {
    fn failed(
        failure: decoder::Failure<insert::Writer<'_>, write::Error>,
    ) -> Result<Self, decode::Error> {
        let progress = failure.progress;
        let cause = failure.cause;
        failure.sink.rollback();
        match cause {
            decoder::DecodeError::InvalidEncoding(cause) => Err(decode::Error::encoding(
                decoder::DecodeError::InvalidEncoding(cause),
                progress,
            )),
            decoder::DecodeError::Sink(
                write::Error::Capacity { .. } | write::Error::Length { .. },
            ) => Ok(Self::Overflow),
        }
    }
}

impl<'a, T> Payload<'a, T> {
    pub(in crate::hpack::decode) fn new(stage: &'a mut T) -> Self {
        Self { stage }
    }
}

impl<'a> Scratch<'a> {
    pub(in crate::hpack::decode) fn new(decoder: &'a mut Machine<stages::Ready>) -> Self {
        Self { decoder }
    }
}

impl<T: stages::Write> Payload<'_, T> {
    pub(in crate::hpack::decode) fn feed(
        &mut self,
        string: &mut limit::StringState,
        payload: &[u8],
    ) -> Result<Outcome, decode::Error> {
        match string.string.feed_into(payload, self.stage.writer()) {
            Ok(_) => Ok(Outcome::Stored),
            Err(failure) => Outcome::failed(failure),
        }
    }

    pub(in crate::hpack::decode) fn store(
        &mut self,
        encoded: strings::Encoded<'_>,
    ) -> Result<Outcome, decode::Error> {
        match encoded.encoding {
            Encoding::Huffman => {
                if let Err(failure) =
                    source::Encoded::new(encoded.payload).write_to(self.stage.writer())
                {
                    return Outcome::failed(failure);
                }
            }
            Encoding::Plain => {
                let mut writer = self.stage.writer();
                match writer.write_slice(encoded.payload) {
                    Ok(()) => {}
                    Err(write::Error::Capacity { .. } | write::Error::Length { .. }) => {
                        writer.rollback();
                        return Ok(Outcome::Overflow);
                    }
                }
            }
        }
        Ok(Outcome::Stored)
    }
}

impl Scratch<'_> {
    pub(in crate::hpack::decode) fn feed_name<M: mode::Kind>(
        &mut self,
        string: &mut limit::StringState,
        context: &limit::Literal<M>,
        payload: &[u8],
    ) -> Result<(), decode::Error> {
        if matches!(context.route(), mode::NameRoute::Count) {
            return match context.index() {
                mode::Index::Indexed => Self::discard(
                    string,
                    context.limit(),
                    &mut self.decoder.name_scratch,
                    payload,
                ),
                mode::Index::Plain | mode::Index::Never => Self::count(&mut string.string, payload),
            };
        }
        if matches!(string.retention, strings::Retention::Counted) {
            return Self::count(&mut string.string, payload);
        }
        Self::store(
            &mut string.string,
            &mut string.retention,
            self.decoder.name_scratch.writer(context.limit().option()),
            payload,
        )
    }

    pub(in crate::hpack::decode) fn feed_value<M, S: ?Sized>(
        &mut self,
        string: &mut limit::StringState,
        context: &limit::Named<M>,
        payload: &[u8],
        sink: &mut S,
    ) -> Result<(), M::Error>
    where
        M: mode::Flow<S>,
    {
        match context.route() {
            mode::Route::Retain => {
                if matches!(string.retention, strings::Retention::Counted) {
                    return Self::count(&mut string.string, payload).map_err(Into::into);
                }
                Self::store(
                    &mut string.string,
                    &mut string.retention,
                    self.decoder.value_scratch.writer(context.limit().option()),
                    payload,
                )?;
                Ok(())
            }
            mode::Route::Count => {
                match context.index() {
                    mode::Index::Indexed => Self::discard(
                        string,
                        context.limit(),
                        &mut self.decoder.value_scratch,
                        payload,
                    )?,
                    mode::Index::Plain | mode::Index::Never => {
                        Self::count(&mut string.string, payload)?;
                    }
                }
                Ok(())
            }
            mode::Route::Validate => {
                let (_, valid) = string
                    .string
                    .feed_into(payload, string.validation)
                    .map_err(|failure| decode::Error::encoding(failure.cause, failure.progress))?;
                string.validation = valid;
                Ok(())
            }
            mode::Route::Direct(proof) => proof.string(sink, &mut string.string, payload),
        }
    }

    /// Preserves fragment errors while discarding decoded bytes.
    fn discard(
        string: &mut limit::StringState,
        limit: limit::Bound,
        buf: &mut buffer::Buffer<impl buffer::Scanner>,
        payload: &[u8],
    ) -> Result<(), decode::Error> {
        match limit.admit(string.string.decoded_len()) {
            limit::Admission::Admitted => Self::store(
                &mut string.string,
                &mut string.retention,
                buf.writer(Some(0)),
                payload,
            ),
            limit::Admission::Exceeded => Self::count(&mut string.string, payload),
        }
    }

    fn count(string: &mut Cursor, payload: &[u8]) -> Result<(), decode::Error> {
        string.feed_count(payload).map_err(decode::Error::from)?;
        Ok(())
    }

    fn store(
        string: &mut Cursor,
        retain: &mut strings::Retention,
        writer: buffer::Writer<'_, impl buffer::Scanner>,
        payload: &[u8],
    ) -> Result<(), decode::Error> {
        match string.feed_into(payload, writer) {
            Ok((_, writer)) => {
                if !writer.is_retained() {
                    *retain = strings::Retention::Counted;
                }
                Ok(())
            }
            Err(failure) => {
                failure.sink.rollback();
                Err(decode::Error::retention(failure.cause, failure.progress))
            }
        }
    }
}
