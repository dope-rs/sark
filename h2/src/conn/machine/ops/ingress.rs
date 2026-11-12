use o3::buffer::{
    self,
    hold::{self, scope},
    resident, storage,
};
use sark_core::{
    life::session,
    manifold::{server::drain, stream::Boundary},
};
use sark_http::field::metadata::section::{rules, state::Selected};

use crate::{
    conn::{
        self, event,
        ingress::input,
        machine,
        machine::decoder::{self, data},
    },
    frame::{self, layout, numbers},
};

struct Complete;

impl From<Complete> for frame::Block {
    fn from(_: Complete) -> Self {
        Self::End
    }
}

#[repr(transparent)]
pub struct Ingress<
    'a,
    R: crate::Role,
    F: rules::Policy = rules::RetainRaw,
    C: session::Storage = storage::Shared,
    A: rules::Push = rules::RejectPush,
    E: event::Evidence = event::Retirement,
> {
    pub(in crate::conn::machine) conn: &'a mut machine::Connection<R, F, C, A, E>,
}

const _: () = {
    use core::mem::{align_of, size_of};

    assert!(size_of::<Ingress<'static, crate::ClientRole>>() == size_of::<usize>());
    assert!(align_of::<Ingress<'static, crate::ClientRole>>() == align_of::<usize>());
};

impl<'d, R, F, A, E> Ingress<'_, R, F, resident::Shared<'d>, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    A: rules::Push,
    E: event::Evidence,
{
    /// Binds the authority used to copy input and compact retained payload views.
    /// Rebinding the same domain succeeds; a different domain is rejected.
    /// Existing shared owners keep their original accounting.
    pub fn try_bind(&mut self, budget: resident::Budget<'d>) -> Result<(), conn::Error> {
        self.conn
            .ingress
            .input
            .retention
            .try_bind(budget)
            .map_err(conn::Error::Authority)
    }

    /// Decodes one exact receive scope, retaining complete DATA only at its
    /// final payload range. `consumed` is reset and records the committed prefix
    /// of this source, including commits made before a returned error.
    pub fn borrow_scope<S: buffer::Cursor + hold::raw::Retain<'d>, B>(
        &mut self,
        source: &scope::Input<'_, 'd, S, B>,
        consumed: &mut usize,
    ) -> Result<(), conn::Error> {
        *consumed = 0;
        self.try_bind(*source.as_parts().1)?;
        self.prepare()?;
        loop {
            if !self.conn.ingress.input.is_ready()
                || !self.conn.ingress.input.bytes.is_empty()
                || self.conn.ingress.headers.pending().is_some()
                || self.is_blocked()
            {
                self.resume()?;
            }
            let mut bytes = &source.as_slice()[*consumed..];
            let before = bytes.len();
            let result = self.borrow(&mut bytes);
            *consumed += before - bytes.len();
            result?;
            if bytes.is_empty() {
                return Ok(());
            }
            let max = self.conn.state.settings.local.max_frame_size;
            if self.conn.ingress.input.is_ready()
                && self.conn.ingress.input.bytes.is_empty()
                && self.conn.ingress.headers.pending().is_none()
                && let Some(wire) = bytes.first_chunk::<{ frame::HEADER_LEN }>()
            {
                if wire[3] == numbers::Type::Data as u8 {
                    let header = layout::Header::read(wire).map_err(|error| {
                        conn::Error::ParseError(numbers::ParseError::BadType(error.actual))
                    })?;
                    let total = frame::HEADER_LEN + header.length.into_usize();
                    if header.length.into_u32() <= max.into_u32() && bytes.len() >= total {
                        let result = data::Data::new(&mut *self.conn)
                            .apply_scoped(header, source, *consumed, consumed);
                        self.finish_drive(result)?;
                        continue;
                    }
                } else if wire[3] == numbers::Type::Settings as u8
                    && wire[4] & numbers::Flags::ACK != 0
                {
                    let header = layout::Header::read(wire).map_err(|error| {
                        conn::Error::ParseError(numbers::ParseError::BadType(error.actual))
                    })?;
                    if header.length == numbers::Length::ZERO && header.stream_id.is_zero() {
                        let result = decoder::Decoder {
                            conn: &mut *self.conn,
                        }
                        .acknowledge(header);
                        if result.is_ok() {
                            *consumed += frame::HEADER_LEN;
                        }
                        self.finish_drive(result)?;
                        continue;
                    }
                }
            }
            let count = self.conn.ingress.input.wire_prefix(bytes, max)?;
            let end = *consumed + count.get();
            let bytes = source
                .try_plan(*consumed..end)
                .map_err(hold::Error::from)?
                .bind(crate::INGRESS_RETENTION)
                .try_into()?;
            self.conn
                .ingress
                .input
                .push(bytes)
                .map_err(input::Full::reject)?;
            *consumed = end;
        }
    }
}

impl<R, F, C, A, E> Ingress<'_, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub fn poll_event(&mut self) -> Option<event::Event<Selected<F>, F::Block, C, E>> {
        self.ready_event().map(drain::Ready::take)
    }

    pub fn ready_event(&mut self) -> Option<event::Ready<'_, R, F, C, A, E>> {
        machine::ready::Incoming::new(self.conn)
    }

    pub fn has_event(&self) -> bool {
        !self.conn.ingress.events.is_empty()
    }

    pub fn is_blocked(&self) -> bool {
        self.conn.ingress.decoder_blocked
    }
}

impl<R, F, C, A, E> Ingress<'_, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    /// Consumes complete ordinary HEADERS while borrowing the receive owner.
    /// Unhandled bytes remain in `bytes` for the retained ingress path.
    pub fn borrow(&mut self, bytes: &mut &[u8]) -> Result<(), conn::Error> {
        loop {
            if !self.conn.ingress.input.is_ready()
                || !self.conn.ingress.input.bytes.is_empty()
                || self.conn.ingress.headers.pending().is_some()
            {
                return Ok(());
            }
            let Some(wire) = bytes.first_chunk::<{ frame::HEADER_LEN }>() else {
                return Ok(());
            };
            if wire[3] != numbers::Type::Headers as u8
                || wire[4] & (numbers::Flags::PADDED | numbers::Flags::PRIORITY) != 0
                || wire[4] & numbers::Flags::END_HEADERS == 0
            {
                return Ok(());
            }
            let header = layout::Header::read(wire).map_err(|error| {
                conn::Error::ParseError(numbers::ParseError::BadType(error.actual))
            })?;
            if header.length.into_u32() > self.conn.state.settings.local.max_frame_size.into_u32() {
                return Ok(());
            }
            let total = frame::HEADER_LEN + header.length.into_usize();
            let Some((_, tail)) = bytes.split_at_checked(total) else {
                return Ok(());
            };
            self.prepare()?;
            let stream = header.try_stream().map_err(conn::Error::ParseError)?;
            let boundary = if header.flags.contains(numbers::Flags::END_STREAM) {
                Boundary::End
            } else {
                Boundary::More
            };
            let mut committed = false;
            let result = decoder::Decoder {
                conn: &mut *self.conn,
            }
            .handle(
                stream,
                (boundary, Complete),
                frame::HEADER_LEN,
                header.length.into_usize(),
                total,
                Some((bytes, &mut committed)),
            );
            if committed {
                *bytes = tail;
            }
            self.finish_drive(result)?;
        }
    }

    pub fn ingest(&mut self, bytes: &[u8]) -> Result<(), conn::Error> {
        let bytes = if self.conn.ingress.headers.pending().is_some()
            && self.conn.ingress.input.bytes.is_empty()
        {
            if let Err(error) = self.prepare() {
                return match self.conn.ingress.input.append(bytes) {
                    Ok(()) => Err(error),
                    Err(input::Append::Full(full)) => Err(full.reject()),
                    Err(input::Append::Copy(error)) => Err(error.into()),
                };
            }
            let result = decoder::Decoder {
                conn: &mut *self.conn,
            }
            .try_chain(bytes);
            match result {
                Ok(decoder::Feed::Consumed) => {
                    self.conn.ingress.decoder_blocked = false;
                    return Ok(());
                }
                Err(error) => return self.finish_drive(Err(error)),
                Ok(decoder::Feed::Pending(bytes)) => bytes,
            }
        } else {
            bytes
        };
        match self.conn.ingress.input.append(bytes) {
            Ok(()) => {}
            Err(input::Append::Full(full)) => self.flush_with(full)?,
            Err(input::Append::Copy(error)) => {
                return Err(error.into());
            }
        }
        self.resume()
    }

    pub fn ingest_owned(&mut self, bytes: C) -> Result<(), conn::Error> {
        let bytes = if self.conn.ingress.headers.pending().is_some()
            && self.conn.ingress.input.bytes.is_empty()
        {
            if let Err(error) = self.prepare() {
                return match self.conn.ingress.input.push(bytes) {
                    Ok(()) => Err(error),
                    Err(full) => Err(full.reject()),
                };
            }
            let result = decoder::Decoder {
                conn: &mut *self.conn,
            }
            .try_chain(bytes);
            match result {
                Ok(decoder::Feed::Consumed) => {
                    self.conn.ingress.decoder_blocked = false;
                    return Ok(());
                }
                Err(error) => return self.finish_drive(Err(error)),
                Ok(decoder::Feed::Pending(bytes)) => bytes,
            }
        } else {
            bytes
        };
        if let Err(full) = self.conn.ingress.input.push(bytes) {
            self.flush_with(full)?;
        }
        self.resume()
    }

    /// Pulls owners only while decoding needs input.
    pub fn ingest_from<I: Iterator<Item = C>>(
        &mut self,
        source: &mut I,
    ) -> Result<(), conn::Error> {
        self.prepare()?;
        let result = decoder::Decoder {
            conn: &mut *self.conn,
        }
        .drive_run(source);
        self.finish_drive(result)
    }

    fn flush_with(&mut self, full: input::Full<C>) -> Result<(), conn::Error> {
        let (bytes, error) = full.into_parts();
        self.prepare()?;
        decoder::Decoder {
            conn: &mut *self.conn,
        }
        .flush_chain(error)?;
        match self.conn.ingress.input.push(bytes) {
            Ok(()) => Ok(()),
            Err(full) => Err(full.reject()),
        }
    }

    pub fn resume(&mut self) -> Result<(), conn::Error> {
        self.prepare()?;
        let result = decoder::Decoder {
            conn: &mut *self.conn,
        }
        .drive();
        self.finish_drive(result)
    }

    fn prepare(&mut self) -> Result<(), conn::Error> {
        if self.conn.state.active_streams.reset_len() != 0 {
            self.conn.state.flush_resets(&mut self.conn.egress.outbound);
        }
        self.conn.egress.outbound.check().map_err(|cause| {
            self.conn.ingress.decoder_blocked = true;
            conn::Error::from(cause)
        })?;
        Ok(())
    }

    fn finish_drive(&mut self, result: Result<(), conn::Error>) -> Result<(), conn::Error> {
        let result =
            result.and_then(|()| self.conn.egress.outbound.check().map_err(conn::Error::from));
        self.conn.ingress.decoder_blocked = result.as_ref().is_err_and(|error| error.is_pressure());
        result
    }
}
