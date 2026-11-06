#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::manifold::requests) enum Queue {
    Idle,
    Ready,
    Blocked,
}

const _: () = assert!(size_of::<Queue>() == 1);
