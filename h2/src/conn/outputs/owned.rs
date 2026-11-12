use std::ops::ControlFlow;

use o3::depot::queue::group;
use sark_core::life::session;

use crate::{
    conn::{
        outputs,
        outputs::{modes, payloads, wire},
    },
    stream,
};

/// The admitted header block and its actual queue destination.
pub(super) struct Owned<'a, C: session::Storage> {
    slot: payloads::Entry<'a, C>,
    mark: payloads::Mark,
    stream: stream::Key,
    block: wire::Block,
}

impl<'a, C: session::Storage> Owned<'a, C> {
    pub(super) fn try_new(
        outbound: &'a mut outputs::Outbound<C>,
        stream: stream::Key,
        block: wire::Block,
    ) -> Result<ControlFlow<(wire::Block, outputs::Full), Self>, (group::Error<1>, wire::Block)>
    {
        let len = block.as_ref().len();
        if !outbound
            .room::<modes::Retained>()
            .is_some_and(|room| len <= room)
        {
            return Ok(ControlFlow::Break((
                block,
                outbound.full::<modes::Retained>(len),
            )));
        }
        let mark = outbound.head.after(outbound.bytes.len());
        let slot = match outbound.payloads.try_reserve(u64::MAX) {
            Ok(slot) => slot,
            Err(error) => return Err((error, block)),
        };
        Ok(ControlFlow::Continue(Self {
            slot,
            mark,
            stream,
            block,
        }))
    }

    pub(super) fn commit(self) {
        let body = C::from(self.block.into_shared());
        let payload = payloads::Shared::wire(self.stream, self.mark, body);
        self.slot.insert(payload);
    }
}
