#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::manifold::requests) enum Queue {
    Idle,
    Ready,
    Blocked,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::manifold::requests) enum Wake {
    Linked,
    Present,
}

impl Queue {
    pub(in crate::manifold::requests) fn wake(&mut self) -> Wake {
        match self {
            Self::Idle => {
                *self = Self::Ready;
                Wake::Linked
            }
            Self::Ready | Self::Blocked => Wake::Present,
        }
    }
}

const _: () = assert!(size_of::<Queue>() == 1);
