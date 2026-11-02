use std::{marker, num};

use o3::buffer::bytes::window::reserve;

use crate::{
    Endpoint,
    conn::{self, config},
    flow, frame,
};

#[derive(Clone, Copy)]
pub(crate) struct Capacity {
    storage: reserve::Plan<{ Capacity::TERMINAL_SLACK }>,
}

#[derive(Clone, Copy)]
pub(crate) struct Block {
    list: usize,
    wire: usize,
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct Wire(num::NonZero<usize>);

#[derive(Clone, Copy)]
pub(crate) struct Initial<R: crate::Role> {
    capacity: Capacity,
    settings: config::Wire,
    target: flow::Target,
    marker: marker::PhantomData<fn() -> R>,
}

impl Capacity {
    pub(crate) const RESET_SLACK: usize = frame::HEADER_LEN + 4;
    pub(crate) const TERMINAL_SLACK: usize = frame::HEADER_LEN + 8;
    const STORAGE_SLACK: usize = Self::RESET_SLACK + Self::TERMINAL_SLACK;
    pub(crate) const BLOCK_SLACK: usize = 12;
    const MIN_FRAME: usize = 16_384;

    pub(crate) fn new(logical: usize) -> Result<Self, config::Error> {
        let Some(logical) = num::NonZero::<usize>::new(logical) else {
            return Err(config::Error::ZeroCapacity(config::Field::Outbound));
        };
        let Some(reset) = logical.checked_add(Self::RESET_SLACK) else {
            return Err(config::Error::OutboundCapacityOverflow {
                logical,
                source: reserve::Error {
                    capacity: logical,
                    reserved: Self::RESET_SLACK,
                },
            });
        };
        let storage = reserve::Plan::new(reset)
            .map_err(|source| config::Error::OutboundCapacityOverflow { logical, source })?;
        Ok(Self { storage })
    }

    pub(super) const fn logical(self) -> usize {
        self.storage.capacity().get() - Self::STORAGE_SLACK
    }

    pub(super) const fn storage(self) -> reserve::Plan<{ Self::TERMINAL_SLACK }> {
        self.storage
    }

    pub(crate) fn wire(block: Block) -> Result<Wire, config::Error> {
        let wire = block.wire();
        let frames = wire.max(1).div_ceil(Self::MIN_FRAME);
        let Some(capacity) = frames
            .checked_mul(frame::HEADER_LEN)
            .and_then(|heads| wire.checked_add(heads))
            .and_then(num::NonZero::<usize>::new)
        else {
            return Err(config::Error::HpackWireOverflow { list: block.list });
        };
        Ok(Wire(capacity))
    }

    pub(crate) const fn slots(self, wire: Wire) -> usize {
        self.logical() / wire.owner() + 1
    }
}

impl Block {
    pub(crate) const fn new(list: usize) -> Result<Self, config::Error> {
        match list.checked_add(Capacity::BLOCK_SLACK) {
            Some(wire) => Ok(Self { list, wire }),
            None => Err(config::Error::HpackBlockOverflow { list }),
        }
    }

    pub(super) const fn list(self) -> usize {
        self.list
    }

    pub(super) const fn wire(self) -> usize {
        self.wire
    }
}

impl Wire {
    pub(crate) const fn bytes(self) -> usize {
        self.0.get()
    }

    const fn owner(self) -> usize {
        self.bytes() / 2 + self.bytes() % 2
    }
}

impl<R: crate::Role> Initial<R> {
    pub(crate) fn new(
        capacity: Capacity,
        settings: config::Wire,
        target: flow::Target,
    ) -> Result<Self, config::Error> {
        let required = Self::required(settings, target);
        if required > capacity.logical() {
            return Err(config::Error::OutboundTooSmall {
                required,
                actual: capacity.logical(),
            });
        }
        Ok(Self {
            capacity,
            settings,
            target,
            marker: marker::PhantomData,
        })
    }

    pub(super) fn into_parts(self) -> (Capacity, config::Wire, flow::Target) {
        (self.capacity, self.settings, self.target)
    }

    fn required(settings: config::Wire, target: flow::Target) -> usize {
        frame::HEADER_LEN
            + settings.len()
            + match R::ENDPOINT {
                Endpoint::Client => conn::CLIENT_PREFACE.len(),
                Endpoint::Server => 0,
            }
            + if target.increment().is_some() {
                frame::HEADER_LEN + 4
            } else {
                0
            }
    }
}

const _: () = assert!(core::mem::size_of::<Capacity>() == core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Block>() == core::mem::size_of::<[usize; 2]>());
const _: () = assert!(core::mem::size_of::<Wire>() == core::mem::size_of::<usize>());
