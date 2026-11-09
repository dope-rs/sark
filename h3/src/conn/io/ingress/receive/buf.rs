use std::ops;

use o3::buffer::{
    resident, segments, storage,
    write::{consume, consume::Consume as _},
};

use crate::{Source as _, conn};

pub(in crate::conn) struct Buf<'d> {
    chunks: segments::Chain<resident::Shared<'d>>,
}

impl<'d> Buf<'d> {
    pub(in crate::conn) fn try_new(
        pool: &segments::Pool<resident::Shared<'d>>,
    ) -> Result<Self, conn::Error> {
        Ok(Self {
            chunks: pool.try_chain().map_err(conn::Error::Retain)?,
        })
    }

    pub(in crate::conn) fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    pub(in crate::conn) fn front(&self) -> Option<&[u8]> {
        self.chunks.segments().next().map(AsRef::as_ref)
    }

    pub(in crate::conn) fn copy(&self, output: &mut [u8]) -> Result<(), conn::Error> {
        let present = self.chunks.len();
        let written = self.cursor().copy_into(output);
        if written == output.len() && present != 0 {
            return Ok(());
        }
        Err(conn::Error::ReceiveCopy {
            requested: output.len(),
            written,
            available: present,
        })
    }

    pub(in crate::conn) fn append(&mut self, source: &[u8]) -> Result<(), conn::Error> {
        if source.is_empty() {
            return Ok(());
        }
        let source = match storage::Shared::try_copy(source) {
            Ok(source) => source,
            Err(error) => return Err(conn::Error::Build(error.into())),
        };
        self.push(resident::Shared::from(source))
    }

    pub(in crate::conn) fn append_owned(&mut self, mut source: Vec<u8>) -> Result<(), conn::Error> {
        if source.is_empty() {
            return Ok(());
        }
        let owner = match storage::Shared::try_from(&mut source) {
            Ok(owner) => owner,
            Err(error) => {
                return Err(conn::Error::Build(storage::BuildError::Exhausted(error)));
            }
        };
        self.push(resident::Shared::from(owner))
    }

    pub(in crate::conn) fn push(
        &mut self,
        source: resident::Shared<'d>,
    ) -> Result<(), conn::Error> {
        match self.chunks.try_push(source) {
            Ok(_) => Ok(()),
            Err(rejected) => Err(conn::Error::SegmentPush(rejected.discard())),
        }
    }

    pub(in crate::conn) fn try_advance(&mut self, n: usize) -> Result<(), consume::Error> {
        self.chunks.try_consume(n)
    }

    pub(in crate::conn) fn take_payload(
        &mut self,
        range: ops::Range<usize>,
        len: usize,
    ) -> Result<crate::Payload<'d>, conn::Error> {
        let stored = self.chunks.len();
        if range.start > range.end || range.end != len || len > stored {
            return Err(conn::Error::ReceiveRange {
                start: range.start,
                end: range.end,
                len,
                available: stored,
            });
        }
        let mut payload = self
            .chunks
            .try_take(len)
            .map_err(conn::Error::SegmentTake)?;
        payload
            .try_consume(range.start)
            .map_err(conn::Error::Consume)?;
        Ok(crate::Payload::from_chain(payload))
    }

    pub(in crate::conn) fn clear(&mut self) {
        self.chunks.clear();
    }
}

#[derive(Clone)]
pub(crate) struct Chunks<'a, 'd> {
    chunks: segments::Iter<'a, resident::Shared<'d>>,
}

impl<'a, 'd: 'a> Iterator for Chunks<'a, 'd> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.chunks.next().map(AsRef::as_ref)
    }
}

impl<'d> crate::Source for Buf<'d> {
    type Chunks<'a>
        = Chunks<'a, 'd>
    where
        Self: 'a;

    fn len(&self) -> usize {
        self.chunks.len()
    }

    fn chunks(&self) -> Self::Chunks<'_> {
        Chunks {
            chunks: self.chunks.segments(),
        }
    }
}
