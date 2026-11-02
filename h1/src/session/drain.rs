use o3::buffer::storage::range;
use session::Ingress;

use crate::{
    session,
    wire::{Failure, InputError},
};

pub(super) enum Drain {
    Complete,
    Pending,
}

/// Advances the retained request window once, keeping its owner and suffix
/// available for the next frame without reconstructing a range from `offset`.
fn advance(offset: &mut usize, window: &mut crate::Input<'_>, used: usize) -> Result<(), Failure> {
    let len = window.as_slice().len();
    *window = window
        .try_get(used..len)
        .map_err(|_| Failure::Input(InputError::Range(range::Error::new(0..used, len))))?;
    *offset += used;
    Ok(())
}

impl Drain {
    pub(super) fn buffered<'d, Pending, Ready, Heads>(
        input: &mut Ingress<'d, Pending, Ready, Heads>,
        buffered: session::Buffered,
        window: &mut crate::Input<'_>,
        offset: &mut usize,
    ) -> Result<(), Failure> {
        advance(offset, window, buffered.used())?;
        input.finish_frame();
        Ok(())
    }

    pub(super) fn apply<'d, Pending, Ready, Heads>(
        input: &mut Ingress<'d, Pending, Ready, Heads>,
        frame: session::Consumption,
        window: &mut crate::Input<'_>,
        offset: &mut usize,
    ) -> Result<Self, Failure> {
        let state = match frame {
            session::Consumption::Buffered(used) => {
                Self::buffered(input, used, window, offset)?;
                return Ok(Self::Complete);
            }
            session::Consumption::Discard(frame) => {
                advance(offset, window, frame.head())?;
                let used = window.consume_max(frame.body());
                *offset += used;
                input.discard_remaining = frame.body() - used;
                if input.discard_remaining == 0 {
                    Self::Complete
                } else {
                    Self::Pending
                }
            }
        };
        input.finish_frame();
        Ok(state)
    }
}
