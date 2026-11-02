use std::mem::MaybeUninit;

use o3::buffer;

use crate::mask::Key;

pub(crate) struct Init<'a> {
    key: Key,
    source: &'a [u8],
}

impl<'a> Init<'a> {
    pub(super) fn new(key: Key, source: &'a [u8]) -> Self {
        Self { key, source }
    }
}

// SAFETY: the immutable source borrow retains its exact length and disjointness
// from the exclusively borrowed output. The shared kernel writes every byte once.
unsafe impl buffer::raw::Initializer for Init<'_> {
    fn len(&self) -> usize {
        self.source.len()
    }

    unsafe fn initialize(&self, target: *mut MaybeUninit<u8>) {
        // SAFETY: admission supplies the full writable destination; the source
        // remains borrowed and the copy kernel never reads destination bytes.
        unsafe {
            self.key
                .copy_to::<false>(target.cast(), self.source.as_ptr(), self.source.len())
        };
    }
}

const LANE: usize = 16;
const BLOCK: usize = 4 * LANE;
const ALIGN_MIN: usize = 384;

impl Key {
    pub fn apply(self, buf: &mut [u8]) -> &mut [u8] {
        // SAFETY: the exclusive slice retains every initialized byte in place.
        unsafe { self.copy_to::<true>(buf.as_mut_ptr(), buf.as_ptr(), buf.len()) };
        buf
    }

    /// Copies and unmasks the common prefix of both slices.
    pub fn copy<'dst>(self, dst: &'dst mut [u8], src: &[u8]) -> &'dst mut [u8] {
        let len = dst.len().min(src.len());
        // SAFETY: both borrowed slices retain this prefix, and the exclusive
        // destination cannot overlap the immutable source.
        unsafe { self.copy_to::<false>(dst.as_mut_ptr(), src.as_ptr(), len) };
        &mut dst[..len]
    }

    unsafe fn copy_to<const IN_PLACE: bool>(
        self,
        mut dst: *mut u8,
        src: *const u8,
        mut len: usize,
    ) {
        let mut src = if IN_PLACE { dst.cast_const() } else { src };
        let mut key = self.0;
        if len >= ALIGN_MIN {
            let prefix = dst.align_offset(LANE);
            if prefix != 0 {
                unsafe { tail::<IN_PLACE>(dst, src, 0, prefix, key) };
                dst = unsafe { dst.add(prefix) };
                src = unsafe { src.add(prefix) };
                len -= prefix;
                key = Key(key).advanced(prefix).0;
            }
        }

        let mut at = 0;
        while at + BLOCK <= len {
            let mut value = unsafe { src.add(at).cast::<[u32; 16]>().read_unaligned() };
            for word in &mut value {
                *word ^= key;
            }
            unsafe { dst.add(at).cast::<[u32; 16]>().write_unaligned(value) };
            at += BLOCK;
        }
        while at + LANE <= len {
            let mut value = unsafe { src.add(at).cast::<[u32; 4]>().read_unaligned() };
            for word in &mut value {
                *word ^= key;
            }
            unsafe { dst.add(at).cast::<[u32; 4]>().write_unaligned(value) };
            at += LANE;
        }
        unsafe { tail::<IN_PLACE>(dst, src, at, len, key) };
    }
}

unsafe fn tail<const IN_PLACE: bool>(
    dst: *mut u8,
    src: *const u8,
    mut at: usize,
    len: usize,
    key: u32,
) {
    let src = if IN_PLACE { dst.cast_const() } else { src };
    let word = u64::from(key) | (u64::from(key) << 32);
    while at + 8 <= len {
        let value = unsafe { src.add(at).cast::<u64>().read_unaligned() } ^ word;
        unsafe { dst.add(at).cast::<u64>().write_unaligned(value) };
        at += 8;
    }
    if at + 4 <= len {
        let value = unsafe { src.add(at).cast::<u32>().read_unaligned() } ^ key;
        unsafe { dst.add(at).cast::<u32>().write_unaligned(value) };
        at += 4;
    }
    let mask = key.to_ne_bytes();
    if at + 2 <= len {
        let word = u16::from_ne_bytes([mask[at & 3], mask[(at + 1) & 3]]);
        let value = unsafe { src.add(at).cast::<u16>().read_unaligned() } ^ word;
        unsafe { dst.add(at).cast::<u16>().write_unaligned(value) };
        at += 2;
    }
    if at < len {
        unsafe { *dst.add(at) = *src.add(at) ^ mask[at & 3] };
    }
}
