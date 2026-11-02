use std::mem;

use o3::buffer::write::{Sink, SpareExt as _, Staged};

use crate::{
    inbound,
    server::{Acc, Error},
};

pub(super) enum Mask<T> {
    Commit(T),
    Restore(T),
}

pub(super) trait Frame {
    fn bytes(&self) -> &[u8];

    fn read<'state, T>(
        &mut self,
        input: &'state mut inbound::Inbound<inbound::ClientPeer>,
        limit: usize,
        apply: impl FnOnce(inbound::Admission<'state, inbound::ClientPeer>, &[u8]) -> Mask<T>,
    ) -> Result<Option<Mask<T>>, Error>
    where
        Self: Sized,
    {
        let frame = match input.decode(self.bytes(), limit).map_err(Error::Frame)? {
            inbound::Decode::NeedMore => return Ok(None),
            inbound::Decode::Ready(frame) => frame,
        };
        Self::unmask(
            Selection {
                source: self,
                frame,
            },
            apply,
        )
        .map(Some)
    }

    fn unmask<'state, T>(
        selected: Selection<'_, 'state, Self>,
        apply: impl FnOnce(inbound::Admission<'state, inbound::ClientPeer>, &[u8]) -> Mask<T>,
    ) -> Result<Mask<T>, Error>
    where
        Self: Sized;

    fn finish(self, discard: bool) -> Result<(), Error>;
}

pub(super) struct Selection<'source, 'state, S> {
    source: &'source mut S,
    frame: inbound::Admission<'state, inbound::ClientPeer>,
}

pub(super) struct Borrowed<'a> {
    pub(super) chunk: &'a [u8],
    pub(super) scratch: &'a mut Vec<u8>,
    pub(super) acc: &'a mut Acc,
}

impl Frame for &mut &mut [u8] {
    fn bytes(&self) -> &[u8] {
        self
    }

    fn unmask<'state, T>(
        selected: Selection<'_, 'state, Self>,
        apply: impl FnOnce(inbound::Admission<'state, inbound::ClientPeer>, &[u8]) -> Mask<T>,
    ) -> Result<Mask<T>, Error> {
        let Selection { source, frame } = selected;
        let end = frame.frame_len().get();
        let mask = crate::mask::Key::new(frame.mask());
        // SAFETY: Selection exclusively retains the same source whose complete
        // frame was admitted; decoding and projection cannot change its extent.
        let payload = unsafe { source.get_unchecked_mut(frame.range()) };
        let result = apply(frame, mask.apply(payload));
        match &result {
            Mask::Restore(_) => {
                mask.apply(payload);
            }
            Mask::Commit(_) => {
                // SAFETY: this exact frame's admitted end remains within the
                // original source; only its disjoint payload bytes were changed.
                **source = unsafe { mem::take(*source).get_unchecked_mut(end..) };
            }
        }
        Ok(result)
    }

    fn finish(self, discard: bool) -> Result<(), Error> {
        if discard {
            *self = &mut [];
        }
        Ok(())
    }
}

impl Frame for Borrowed<'_> {
    fn bytes(&self) -> &[u8] {
        self.chunk
    }

    fn unmask<'state, T>(
        selected: Selection<'_, 'state, Self>,
        apply: impl FnOnce(inbound::Admission<'state, inbound::ClientPeer>, &[u8]) -> Mask<T>,
    ) -> Result<Mask<T>, Error> {
        let Selection { source, frame } = selected;
        let end = frame.frame_len().get();
        // SAFETY: Selection binds this exact unchanged chunk to its admitted
        // complete frame; the exclusive source borrow prevents replacement.
        let payload = unsafe { source.chunk.get_unchecked(frame.range()) };
        source.scratch.clear();
        let mut stage =
            Staged::try_reserve(source.scratch, payload.len() as u32).map_err(Error::Reserve)?;
        {
            let mut spare = stage.spare();
            let mut writer = spare.bounded();
            writer.try_init(&crate::mask::Key::new(frame.mask()).initializer(payload))?;
            writer.commit();
            spare.commit();
        }
        stage.commit();
        let result = apply(frame, source.scratch);
        if matches!(&result, Mask::Commit(_)) {
            // SAFETY: the same source retained above is still unchanged, and
            // the admitted frame proves this suffix starts within its extent.
            source.chunk = unsafe { source.chunk.get_unchecked(end..) };
        }
        Ok(result)
    }

    fn finish(self, discard: bool) -> Result<(), Error> {
        self.acc.clear();
        if !discard {
            self.acc.write_slice(self.chunk).map_err(Error::Allocate)?;
        }
        Ok(())
    }
}
