use dope_quic::wire::{Mark, Packed, stream::Id};

use crate::conn::io::Resume;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(in crate::conn) struct Credit(Packed);

impl Credit {
    pub(in crate::conn) const fn new(id: Id, resume: Resume) -> Self {
        let mark = match resume {
            Resume::Idle => Mark::Clear,
            Resume::Ready => Mark::Set,
        };
        Self(Packed::new(id, mark))
    }

    pub(in crate::conn) const fn id(self) -> Id {
        self.0.id()
    }

    pub(in crate::conn) const fn resume(self) -> Resume {
        match self.0.mark() {
            Mark::Clear => Resume::Idle,
            Mark::Set => Resume::Ready,
        }
    }
}

const _: () = assert!(core::mem::size_of::<Credit>() == core::mem::size_of::<u64>());
