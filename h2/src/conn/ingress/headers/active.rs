use decode::mode::Discard;
use sark_http::field::metadata::{
    lower,
    section::{rules, storage},
};

use crate::{
    conn::ingress::{Decode, header::sink, headers::Pending},
    hpack::decode::{self, limit::Budget, phase::Phase},
};

pub(super) struct Active<P: rules::Policy> {
    pub(super) pending: Pending,
    pub(super) source: Source<P>,
    pub(super) wire_len: usize,
}

pub(super) enum Source<P: rules::Policy> {
    Store {
        block: Phase<Decode>,
        budget: Budget,
        sink: sink::Header<P>,
    },
    Discard(Phase<Discard>, Budget, storage::Scratch),
}

impl<P: rules::Policy> Source<P> {
    fn decode<'a, A, I>(self, parts: I) -> Result<Self, decode::Failure<lower::Error>>
    where
        A: rules::Push,
        I: IntoIterator<Item = &'a [u8]>,
    {
        match self {
            Self::Store {
                block,
                mut budget,
                sink,
            } => match block.feed_all(&mut budget, parts, sink.hpack::<A>()) {
                Ok((block, sink)) => Ok(Self::Store {
                    block,
                    budget,
                    sink: sink.into_header(),
                }),
                Err((sink, cause)) => {
                    drop(sink.into_header().recycle());
                    Err(cause)
                }
            },
            Self::Discard(block, mut budget, scratch) => {
                match block.feed_all(&mut budget, parts, ()) {
                    Ok((block, ())) => Ok(Self::Discard(block, budget, scratch)),
                    Err(((), cause)) => {
                        drop(scratch);
                        Err(cause.into())
                    }
                }
            }
        }
    }
}

impl<P: rules::Policy> Active<P> {
    pub(super) fn decode<'a, A, I>(
        self,
        parts: I,
        wire_len: usize,
    ) -> Result<Self, decode::Failure<lower::Error>>
    where
        A: rules::Push,
        I: IntoIterator<Item = &'a [u8]>,
    {
        let pending = self.pending;
        let source = self.source.decode::<A, I>(parts)?;
        Ok(Self {
            pending,
            source,
            wire_len,
        })
    }
}
