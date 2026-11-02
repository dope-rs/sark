//! Trusted generated route dispatch.

use std::mem;

use crate::route::table;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PathCapture {
    start: usize,
    end: usize,
}

impl PathCapture {
    /// Creates a path capture proved by the generated route machine.
    ///
    /// # Safety
    /// The range must be the matched segment in its paired target.
    pub const unsafe fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    pub const fn start(self) -> usize {
        self.start
    }

    pub const fn end(self) -> usize {
        self.end
    }

    #[doc(hidden)]
    pub const fn is_schema(expected: &[&[u8]], actual: &[&[u8]]) -> bool {
        if expected.len() != actual.len() {
            return false;
        }
        let mut field = 0;
        while field < expected.len() {
            if expected[field].len() != actual[field].len() {
                return false;
            }
            let mut byte = 0;
            while byte < expected[field].len() {
                if expected[field][byte] != actual[field][byte] {
                    return false;
                }
                byte += 1;
            }
            field += 1;
        }
        true
    }
}

const _: () = {
    assert!(mem::size_of::<PathCapture>() == 2 * mem::size_of::<usize>());
    assert!(!mem::needs_drop::<PathCapture>());
};

/// Generated route dispatch trusted by typed storage.
///
/// # Safety
/// Visitors must receive only the owning slot; `each` requires `P::INDEX == index`.
pub unsafe trait Table<S> {
    type Set;

    const LEN: usize;
    const METHODS: u8;

    fn find<'a, V>(key: table::Key<'a>, visit: V) -> Option<V::Output>
    where
        V: table::Visit<S, Self::Set>;

    fn each<V>(index: u32, visit: V) -> Option<V::Output>
    where
        V: table::Each<S, Self::Set>;
}
