use std::{mem::MaybeUninit, ptr};

use o3::buffer::raw::Initializer;

use super::{Frame, Writer};

// SAFETY: private constructors bind an immutable payload and mask to the exact
// Plan. Header and payload initialize that stable wire length without overlap.
unsafe impl Initializer for Frame<'_> {
    fn len(&self) -> usize {
        self.plan.wire_len
    }

    unsafe fn initialize(&self, target: *mut MaybeUninit<u8>) {
        let dst = target.cast::<u8>();
        let head = usize::from(self.plan.head);
        let len = self.plan.payload_len;
        let first = if self.plan.fin {
            0x80 | self.plan.opcode
        } else {
            self.plan.opcode
        };
        let bit = if self.plan.masked { 0x80 } else { 0 };
        // SAFETY: the exclusive target holds the admitted 2/4/10-byte header,
        // optional mask and exact disjoint borrowed payload. Nothing reads output.
        unsafe {
            dst.write(first);
            if head == 2 {
                dst.add(1).write(bit | len as u8);
            } else if head == 4 {
                dst.add(1).write(bit | 126);
                dst.add(2)
                    .cast::<u16>()
                    .write_unaligned((len as u16).to_be());
            } else {
                dst.add(1).write(bit | 127);
                dst.add(2)
                    .cast::<u64>()
                    .write_unaligned((len as u64).to_be());
            }
            let payload = dst.add(head);
            match self.mask {
                Some(mask) => {
                    payload.cast::<[u8; 4]>().write_unaligned(mask);
                    crate::mask::Key::new(mask)
                        .initializer(self.payload)
                        .initialize(payload.add(4).cast());
                }
                None => ptr::copy_nonoverlapping(self.payload.as_ptr(), payload, len),
            }
        }
    }
}

impl Writer<'_, '_> {
    pub fn encode(self) -> usize {
        let len = self.frame.plan.wire_len;
        // SAFETY: bind retains this exact exclusive destination with room for
        // the immutable frame; initialize writes only its admitted prefix.
        unsafe { self.frame.initialize(self.dst.as_mut_ptr().cast()) };
        len
    }
}
