#[repr(u8)]
#[derive(Clone, Copy, Default, Eq, PartialEq)]
pub(in crate::server::runtime) enum Kind {
    #[default]
    Protocol,
    Semantic,
    Output,
    Completion,
    Task,
}

impl Kind {
    const fn next(self, tasks: bool) -> Self {
        match self {
            Self::Protocol => Self::Semantic,
            Self::Semantic => Self::Output,
            Self::Output if tasks => Self::Completion,
            Self::Completion if tasks => Self::Task,
            Self::Output | Self::Completion | Self::Task => Self::Protocol,
        }
    }
}

#[derive(Default)]
pub(in crate::server::runtime) struct Schedule {
    pub(in crate::server::runtime) waiting: usize,
    /// Remaining rows in the circular sweep admitted by the last capacity release.
    pub(in crate::server::runtime) retries: usize,
    /// Alternate ready producers and admitted blocked retries.
    pub(super) retry_next: bool,
    plane: Kind,
}

impl Schedule {
    pub(in crate::server::runtime) fn select(
        &mut self,
        tasks: bool,
        mut ready: impl FnMut(Kind) -> bool,
    ) -> Option<Kind> {
        let mut plane = self.plane;
        for _ in 0..if tasks { 5 } else { 3 } {
            if ready(plane) {
                self.plane = plane.next(tasks);
                return Some(plane);
            }
            plane = plane.next(tasks);
        }
        None
    }
}

const _: () = assert!(core::mem::size_of::<Kind>() == core::mem::size_of::<u8>());
