use super::byte::is_ascii_ws;
use super::error::ERR_INVALID_HEADER_NAME;

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
    fn find_crlf_from(&self, start: usize) -> Option<usize>;
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

    fn find_crlf_from(&self, start: usize) -> Option<usize> {
        BytesScan::find_crlf_from(self, start)
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

pub struct BytesScan;

impl BytesScan {
    pub fn find_byte_from(bytes: &[u8], start: usize, needle: u8) -> Option<usize> {
        if start >= bytes.len() {
            return None;
        }
        memchr::memchr(needle, &bytes[start..]).map(|i| start + i)
    }

    pub fn find_crlf_from(bytes: &[u8], start: usize) -> Option<usize> {
        if bytes.len() < 2 || start >= bytes.len().saturating_sub(1) {
            return None;
        }
        let mut seek = start;
        while let Some(cr_idx) = Self::find_byte_from(bytes, seek, b'\r') {
            if cr_idx + 1 >= bytes.len() {
                return None;
            }
            if bytes[cr_idx + 1] == b'\n' {
                return Some(cr_idx);
            }
            seek = cr_idx + 1;
        }
        None
    }

    pub fn find_name_end_valid(
        bytes: &[u8],
        start: usize,
    ) -> Result<Option<(usize, u8)>, crate::error::Error> {
        match crate::simd::scan_header_name(bytes, start) {
            crate::simd::HeaderNameOutcome::Found { pos, byte } => Ok(Some((pos, byte))),
            crate::simd::HeaderNameOutcome::Invalid => Err(crate::error::Error::BadRequest(
                ERR_INVALID_HEADER_NAME.into(),
            )),
            crate::simd::HeaderNameOutcome::None => Ok(None),
        }
    }

    pub fn find_header_line_from(bytes: &[u8], start: usize) -> Option<HeaderLineScan> {
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
        let leading = segment.iter().take_while(|&&b| is_ascii_ws(b)).count();
        let trailing = segment[leading..]
            .iter()
            .rev()
            .take_while(|&&b| is_ascii_ws(b))
            .count();
        Some(HeaderLineScan {
            end: cr,
            colon: Some(colon_pos),
            value_start: value_segment_start + leading,
            value_end: cr - trailing,
        })
    }
}
