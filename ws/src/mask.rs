pub struct Mask;

impl Mask {
    /// XOR `buf` in place with the repeating 4-byte `mask`.
    #[inline]
    pub fn unmask_inline(buf: &mut [u8], mask: [u8; 4]) {
        let len = buf.len();
        let p = buf.as_mut_ptr();
        // SAFETY: dst == src == buf over the same `len`; each lane is read then
        // written at the same offset, so the self-overlap is sound.
        unsafe { unmask_raw(p, p, len, mask) }
    }

    /// XOR `src` into `dst` (disjoint) with the repeating 4-byte `mask`, keyed
    /// from `src[0]`. Used by the zero-copy recv path, where `src` is read-only.
    #[inline]
    pub fn unmask_copy(dst: &mut [u8], src: &[u8], mask: [u8; 4]) {
        let len = src.len();
        assert!(dst.len() >= len);
        // SAFETY: `len` bytes are read from `src` and written to `dst`, which is
        // at least `len` and (being a `&mut` distinct from `&src`) cannot alias.
        unsafe { unmask_raw(dst.as_mut_ptr(), src.as_ptr(), len, mask) }
    }
}

cfg_select! {
    target_arch = "aarch64" => {
        #[target_feature(enable = "neon")]
        unsafe fn unmask_raw(dst: *mut u8, src: *const u8, len: usize, mask: [u8; 4]) {
            use core::arch::aarch64::{vld1q_u8, vst1q_u8, veorq_u8};
            let key_arr: [u8; 16] = [
                mask[0], mask[1], mask[2], mask[3],
                mask[0], mask[1], mask[2], mask[3],
                mask[0], mask[1], mask[2], mask[3],
                mask[0], mask[1], mask[2], mask[3],
            ];
            let key = unsafe { vld1q_u8(key_arr.as_ptr()) };
            let mut i = 0;
            while i + 16 <= len {
                // SAFETY: i + 16 <= len bounds the 16-byte load/store windows.
                let v = unsafe { vld1q_u8(src.add(i)) };
                unsafe { vst1q_u8(dst.add(i), veorq_u8(v, key)) };
                i += 16;
            }
            unsafe { unmask_tail(dst, src, i, len, mask) };
        }
    }
    _ => {
        #[inline]
        unsafe fn unmask_raw(dst: *mut u8, src: *const u8, len: usize, mask: [u8; 4]) {
            let key = u32::from_ne_bytes(mask);
            let mut i = 0;
            while i + 4 <= len {
                // SAFETY: i + 4 <= len bounds the unaligned 4-byte load/store.
                let v = unsafe { (src.add(i) as *const u32).read_unaligned() } ^ key;
                unsafe { (dst.add(i) as *mut u32).write_unaligned(v) };
                i += 4;
            }
            unsafe { unmask_tail(dst, src, i, len, mask) };
        }
    }
}

#[inline]
unsafe fn unmask_tail(dst: *mut u8, src: *const u8, mut i: usize, len: usize, mask: [u8; 4]) {
    while i < len {
        // SAFETY: i < len bounds both accesses; caller guarantees `len` valid bytes.
        unsafe { *dst.add(i) = *src.add(i) ^ mask[i & 3] };
        i += 1;
    }
}
