use core::{mem, ops::ControlFlow};

use dynamic::{hash, mode, plan};
use o3::buffer::{pool, write::consume};
use sark_core::{life::session, manifold::stream::Boundary};
use sark_http::{
    self, field,
    payload::{compress::dynamic, response},
};

use crate::{
    conn::{
        self, build, config,
        outputs::{self, limits, owned, wire},
    },
    hpack::encode,
    stream,
};

pub(crate) struct Encoder {
    state: State,
    blocks: pool::Pool,
    capacity: limits::Block,
}

enum State {
    Local(Result<encode::Encoder, (usize, usize)>),
    Prepared {
        capacity: usize,
        limit: usize,
        encoded: Encoded<stream::Sending>,
    },
}

pub(crate) struct Idle<'a> {
    encoder: &'a mut encode::Encoder,
}

/// One validated HPACK block waiting for exact egress admission.
pub(crate) struct Encoded<T> {
    block: wire::Block,
    commit: encode::Commit,
    target: stream::Key,
    effect: T,
}

impl<T> Encoded<T> {
    pub(crate) fn stream(&self) -> stream::Key {
        self.target
    }
}

impl State {
    fn take(&mut self) -> Option<encode::Encoder> {
        let Self::Local(local) = self else {
            return None;
        };
        let Ok(encoder) = local else {
            return None;
        };
        let bounds = (encoder.capacity(), encoder.max_size());
        mem::replace(local, Err(bounds)).ok()
    }

    fn restore(&mut self, encoder: encode::Encoder) {
        *self = Self::Local(Ok(encoder));
    }

    fn capacity(&self) -> usize {
        match self {
            Self::Local(Ok(encoder)) => encoder.capacity(),
            Self::Local(Err((capacity, _))) | Self::Prepared { capacity, .. } => *capacity,
        }
    }

    fn limit(&self) -> usize {
        match self {
            Self::Local(Ok(encoder)) => encoder.max_size(),
            Self::Local(Err((_, limit))) | Self::Prepared { limit, .. } => *limit,
        }
    }
}

impl Encoder {
    pub(crate) fn try_new(
        table: plan::Plan<mode::Direct>,
        capacity: limits::Block,
        layout: pool::Layout,
        seed: hash::Seed,
    ) -> Result<Self, build::Headers> {
        Ok(Self {
            state: State::Local(Ok(encode::Encoder::try_with(
                table,
                config::Settings::DEFAULT.header_table_size as usize,
                capacity.list(),
                seed,
            )
            .map_err(build::Headers::Encoder)?)),
            blocks: pool::Pool::try_from(layout).map_err(build::Headers::Blocks)?,
            capacity,
        })
    }

    pub(crate) fn idle(&mut self) -> Option<Idle<'_>> {
        match &mut self.state {
            State::Local(Ok(encoder)) => Some(Idle { encoder }),
            State::Local(Err(_)) | State::Prepared { .. } => None,
        }
    }

    pub(crate) fn is_idle(&self) -> bool {
        matches!(&self.state, State::Local(Ok(_)))
    }

    pub(crate) fn capacity(&self) -> usize {
        self.state.capacity()
    }

    pub(crate) fn table_limit(&self) -> usize {
        self.state.limit()
    }

    pub(crate) fn retain(&mut self, prepared: Encoded<stream::Sending>) {
        self.state = State::Prepared {
            capacity: self.state.capacity(),
            limit: self.state.limit(),
            encoded: prepared,
        };
    }

    pub(crate) fn take(&mut self, key: stream::Key) -> Option<Encoded<stream::Sending>> {
        let State::Prepared {
            capacity,
            limit,
            encoded,
        } = &self.state
        else {
            return None;
        };
        if encoded.stream() != key {
            return None;
        }
        let local = State::Local(Err((*capacity, *limit)));
        match mem::replace(&mut self.state, local) {
            State::Prepared { encoded, .. } => Some(encoded),
            State::Local(_) => None,
        }
    }

    pub(crate) fn pending(&self) -> Option<stream::Key> {
        match &self.state {
            State::Local(_) => None,
            State::Prepared { encoded, .. } => Some(encoded.stream()),
        }
    }

    pub(crate) fn discard(&mut self, key: stream::Key) {
        if let Some(prepared) = self.take(key) {
            self.rollback(prepared);
        }
    }

    pub(crate) fn prepare<S: field::Source, T>(
        &mut self,
        source: S,
        peer: &config::Settings,
        target: stream::raw::Marked,
        effect: T,
        method: Option<&mut response::Method>,
    ) -> Result<Encoded<T>, conn::Error> {
        let cap = self.blocks.slots();
        let used = cap - self.blocks.available();
        let span = match self.blocks.acquire() {
            Some(span) => span,
            None => {
                return Err(conn::Error::HeaderEncodeFull { used, limit: cap });
            }
        };
        let mut block =
            wire::Wire::new(span, peer.max_frame_size, target).map_err(conn::Error::HeaderWire)?;
        let limit = peer
            .max_header_list_size
            .map_or(self.capacity.list(), |limit| {
                self.capacity.list().min(limit as usize)
            });
        let encoder = self.state.take().ok_or(conn::Error::HeaderBusy)?;
        let (result, commit) = encoder.prepare(
            source,
            &mut block,
            limit,
            peer.enable_connect_protocol,
            method,
        );
        let block = match block.finish() {
            Ok(block) => block,
            Err(error) => {
                self.state.restore(commit.discard());
                return Err(conn::Error::HeaderWire(error));
            }
        };
        if let Err(error) = result {
            self.state.restore(commit.discard());
            return Err(conn::Error::HeaderEncode(error));
        }
        Ok(Encoded {
            block,
            commit,
            target: target.key(),
            effect,
        })
    }

    pub(crate) fn rollback<T>(&mut self, encoded: Encoded<T>) {
        self.state.restore(encoded.commit.discard());
    }

    pub(crate) fn try_commit<C: session::Storage, T>(
        &mut self,
        outbound: &mut outputs::Outbound<C>,
        encoded: Encoded<T>,
    ) -> Result<ControlFlow<(Encoded<T>, outputs::Full), T>, conn::Error> {
        let stream = encoded.stream();
        let wire_len = encoded.block.as_ref().len();
        let (encoded, full) = if wire_len >= self.blocks.capacity().get().div_ceil(2) as usize {
            let Encoded {
                block,
                commit,
                target,
                effect,
            } = encoded;
            match owned::Owned::try_new(outbound, stream, block) {
                Ok(ControlFlow::Continue(output)) => {
                    output.commit();
                    self.state.restore(commit.apply());
                    return Ok(ControlFlow::Continue(effect));
                }
                Ok(ControlFlow::Break((block, full))) => (
                    Encoded {
                        block,
                        commit,
                        target,
                        effect,
                    },
                    full,
                ),
                Err((error, block)) => {
                    self.rollback(Encoded {
                        block,
                        commit,
                        target,
                        effect,
                    });
                    return Err(conn::Error::Egress(error));
                }
            }
        } else {
            match outbound.try_append([encoded.block.as_ref()]) {
                Ok(output) => {
                    output.commit();
                    self.state.restore(encoded.commit.apply());
                    return Ok(ControlFlow::Continue(encoded.effect));
                }
                Err(full) => (encoded, full),
            }
        };
        if let Err(error) = consume::Error::check(wire_len, outbound.limit()) {
            self.rollback(encoded);
            return Err(conn::Error::HeaderWire(error));
        }
        Ok(ControlFlow::Break((encoded, full)))
    }

    pub(crate) fn encode<S: field::Source, C: session::Storage>(
        &mut self,
        outbound: &mut outputs::Outbound<C>,
        stream: stream::Key,
        source: S,
        boundary: Boundary,
        peer: &config::Settings,
    ) -> Result<response::Method, conn::Error> {
        let mut method = response::Method::Other;
        let encoded = self.prepare(
            source,
            peer,
            stream::raw::Marked::new(stream, boundary),
            (),
            Some(&mut method),
        )?;
        match self.try_commit(outbound, encoded)? {
            ControlFlow::Continue(()) => Ok(method),
            ControlFlow::Break((encoded, full)) => {
                self.rollback(encoded);
                Err(full.into())
            }
        }
    }
}

impl Idle<'_> {
    pub(crate) fn apply(self, minimum: Option<u32>, max: u32) {
        if let Some(minimum) = minimum {
            if minimum != max {
                self.encoder.set_max_size(minimum as usize);
            }
            self.encoder.set_max_size(max as usize);
        }
    }
}

const _: () = assert!(usize::BITS >= u32::BITS);
