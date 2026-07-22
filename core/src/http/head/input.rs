use super::error::ERR_INVALID_HEADER_NAME;

#[derive(Clone, Copy)]
pub struct HeaderLine<'a> {
    bytes: &'a [u8],
}

impl<'a> HeaderLine<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub fn is_whitespace(byte: u8) -> bool {
        byte == b' ' || byte == b'\t'
    }

    pub fn trimmed_range(&self, start: usize, end: usize) -> Option<(usize, usize)> {
        if start > end || end > self.bytes.len() {
            return None;
        }
        let mut value_start = start;
        let mut value_end = end;
        while value_start < value_end && Self::is_whitespace(self.bytes[value_start]) {
            value_start += 1;
        }
        while value_end > value_start && Self::is_whitespace(self.bytes[value_end - 1]) {
            value_end -= 1;
        }
        Some((value_start, value_end))
    }

    pub fn find_crlf_from(&self, start: usize) -> Option<usize> {
        if self.bytes.len() < 2 || start >= self.bytes.len().saturating_sub(1) {
            return None;
        }
        let mut seek = start;
        while let Some(relative) = memchr::memchr(b'\r', &self.bytes[seek..]) {
            let cr_idx = seek + relative;
            if cr_idx + 1 >= self.bytes.len() {
                return None;
            }
            if self.bytes[cr_idx + 1] == b'\n' {
                return Some(cr_idx);
            }
            seek = cr_idx + 1;
        }
        None
    }

    pub fn find_name_end_valid(
        &self,
        start: usize,
    ) -> Result<Option<(usize, u8)>, crate::error::Error> {
        match crate::simd::scan_header_name(self.bytes, start) {
            crate::simd::HeaderNameOutcome::Found { pos, byte } => Ok(Some((pos, byte))),
            crate::simd::HeaderNameOutcome::Invalid => Err(crate::error::Error::BadRequest(
                ERR_INVALID_HEADER_NAME.into(),
            )),
            crate::simd::HeaderNameOutcome::None => Ok(None),
        }
    }

    fn name_value(self) -> Option<(&'a [u8], &'a [u8])> {
        let colon = self.bytes.iter().position(|byte| *byte == b':')?;
        let (name_start, name_end) = self.trimmed_range(0, colon)?;
        let (value_start, value_end) = self.trimmed_range(colon + 1, self.bytes.len())?;
        Some((
            &self.bytes[name_start..name_end],
            &self.bytes[value_start..value_end],
        ))
    }
}

pub struct HeaderLines<'a> {
    remaining: &'a [u8],
    finished: bool,
}

impl<'a> HeaderLines<'a> {
    pub fn new(wire: &'a [u8]) -> Self {
        Self {
            remaining: wire,
            finished: false,
        }
    }
}

impl<'a> Iterator for HeaderLines<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        while !self.finished && !self.remaining.is_empty() {
            let (line, remaining) = match memchr::memchr(b'\n', self.remaining) {
                Some(newline) => (&self.remaining[..newline], &self.remaining[newline + 1..]),
                None => (self.remaining, &[][..]),
            };
            self.remaining = remaining;
            let line = match line.strip_suffix(b"\r") {
                Some(stripped) => stripped,
                None => line,
            };
            if line.is_empty() {
                self.finished = true;
                return None;
            }
            if let Some(parts) = HeaderLine::new(line).name_value() {
                return Some(parts);
            }
        }
        None
    }
}

pub struct HeaderLineScan {
    pub end: usize,
    pub colon: Option<usize>,
    pub value_start: usize,
    pub value_end: usize,
}

pub trait HeadInput {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn slice_range(&self, range: std::ops::Range<usize>) -> Option<&[u8]>;
    fn copy_range_frame(
        &self,
        range: std::ops::Range<usize>,
    ) -> Option<o3::buffer::Bytes<o3::buffer::Retained>>;
    fn copy_range_into(&self, range: std::ops::Range<usize>, out: &mut [u8]);
    fn for_each_slice<F>(&self, range: std::ops::Range<usize>, f: F)
    where
        F: FnMut(&[u8]);
}

impl HeadInput for [u8] {
    fn len(&self) -> usize {
        <[u8]>::len(self)
    }

    fn slice_range(&self, range: std::ops::Range<usize>) -> Option<&[u8]> {
        self.get(range)
    }

    fn copy_range_into(&self, range: std::ops::Range<usize>, out: &mut [u8]) {
        assert!(
            range.start <= range.end && range.end <= self.len(),
            "head input copy range invariant: range out of bounds",
        );
        let need = range.end - range.start;
        assert!(
            out.len() >= need,
            "head input copy range invariant: output too small",
        );
        out[..need].copy_from_slice(&self[range]);
    }

    fn copy_range_frame(
        &self,
        range: std::ops::Range<usize>,
    ) -> Option<o3::buffer::Bytes<o3::buffer::Retained>> {
        self.get(range)
            .map(o3::buffer::Shared::copy_from_slice)
            .map(o3::buffer::Bytes::<o3::buffer::Retained>::from)
    }

    fn for_each_slice<F>(&self, range: std::ops::Range<usize>, mut f: F)
    where
        F: FnMut(&[u8]),
    {
        if let Some(slice) = self.get(range) {
            f(slice);
        }
    }
}

impl HeaderLineScan {
    pub fn find(bytes: &[u8], start: usize) -> Option<Self> {
        if start >= bytes.len() {
            return None;
        }

        let mut search = start;
        let (colon_pos, value_segment_start) = loop {
            let rel = memchr::memchr2(b':', b'\r', &bytes[search..])?;
            let pos = search + rel;
            if bytes[pos] == b':' {
                break (pos, pos + 1);
            }
            if pos + 1 >= bytes.len() {
                return None;
            }
            if bytes[pos + 1] == b'\n' {
                return Some(HeaderLineScan {
                    end: pos,
                    colon: None,
                    value_start: 0,
                    value_end: 0,
                });
            }
            search = pos + 1;
        };

        let mut search = value_segment_start;
        let cr = loop {
            let rel = memchr::memchr(b'\r', &bytes[search..])?;
            let pos = search + rel;
            if pos + 1 >= bytes.len() {
                return None;
            }
            if bytes[pos + 1] == b'\n' {
                break pos;
            }
            search = pos + 1;
        };

        let segment = &bytes[value_segment_start..cr];
        let leading = segment
            .iter()
            .take_while(|&&b| HeaderLine::is_whitespace(b))
            .count();
        let trailing = segment[leading..]
            .iter()
            .rev()
            .take_while(|&&b| HeaderLine::is_whitespace(b))
            .count();
        Some(HeaderLineScan {
            end: cr,
            colon: Some(colon_pos),
            value_start: value_segment_start + leading,
            value_end: cr - trailing,
        })
    }
}
