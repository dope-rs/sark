#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Overflow {
    Arena { left: usize, right: usize },
    Index { left: usize, right: usize },
    Resident { left: usize, right: usize },
    Allocations { left: usize, right: usize },
    Tail { left: usize, right: usize },
}

const _: () = assert!(size_of::<Overflow>() == 3 * size_of::<usize>());
