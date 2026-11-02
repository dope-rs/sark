pub(super) struct Scan<'a>(&'a [u8]);

impl<'a> Scan<'a> {
    pub(super) const fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    pub(super) fn literal(self) -> usize {
        match self
            .0
            .iter()
            .position(|byte| *byte == b'%' || !byte.is_ascii())
        {
            Some(index) => index,
            None => self.0.len(),
        }
    }

    pub(super) fn plain(self) -> (&'a [u8], &'a [u8]) {
        let index = match self
            .0
            .iter()
            .position(|byte| !matches!(*byte, b' '..=b'~') || *byte == b'%')
        {
            Some(index) => index,
            None => self.0.len(),
        };
        self.0.split_at(index)
    }
}
