pub struct Mask;

impl Mask {
    pub fn unmask_in_place(buf: &mut [u8], mask: [u8; 4]) {
        let len = buf.len();
        let p = buf.as_mut_ptr();
        unsafe { unmask_raw(p, p, len, mask) }
    }

    pub fn unmask_copy(dst: &mut [u8], src: &[u8], mask: [u8; 4]) {
        let len = src.len();
        assert!(dst.len() >= len);
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
                let v = unsafe { vld1q_u8(src.add(i)) };
                unsafe { vst1q_u8(dst.add(i), veorq_u8(v, key)) };
                i += 16;
            }
            unsafe { unmask_tail(dst, src, i, len, mask) };
        }
    }
    target_arch = "x86_64" => {
        unsafe fn unmask_raw(dst: *mut u8, src: *const u8, len: usize, mask: [u8; 4]) {
            if is_x86_feature_detected!("avx2") {
                unsafe { unmask_avx2(dst, src, len, mask) }
            } else {
                unsafe { unmask_sse2(dst, src, len, mask) }
            }
        }

        #[target_feature(enable = "avx2")]
        unsafe fn unmask_avx2(dst: *mut u8, src: *const u8, len: usize, mask: [u8; 4]) {
            use core::arch::x86_64::{
                __m128i, __m256i, _mm256_loadu_si256, _mm256_set1_epi32, _mm256_storeu_si256,
                _mm256_xor_si256, _mm_loadu_si128, _mm_set1_epi32, _mm_storeu_si128, _mm_xor_si128,
            };
            let key_bits = i32::from_ne_bytes(mask);
            let key32 = _mm256_set1_epi32(key_bits);
            let mut i = 0;
            while i + 32 <= len {
                let v = unsafe { _mm256_loadu_si256(src.add(i) as *const __m256i) };
                unsafe { _mm256_storeu_si256(dst.add(i) as *mut __m256i, _mm256_xor_si256(v, key32)) };
                i += 32;
            }
            if i + 16 <= len {
                let key16 = _mm_set1_epi32(key_bits);
                let v = unsafe { _mm_loadu_si128(src.add(i) as *const __m128i) };
                unsafe { _mm_storeu_si128(dst.add(i) as *mut __m128i, _mm_xor_si128(v, key16)) };
                i += 16;
            }
            unsafe { unmask_tail(dst, src, i, len, mask) };
        }

        #[target_feature(enable = "sse2")]
        unsafe fn unmask_sse2(dst: *mut u8, src: *const u8, len: usize, mask: [u8; 4]) {
            use core::arch::x86_64::{
                __m128i, _mm_loadu_si128, _mm_set1_epi32, _mm_storeu_si128, _mm_xor_si128,
            };
            let key = _mm_set1_epi32(i32::from_ne_bytes(mask));
            let mut i = 0;
            while i + 16 <= len {
                let v = unsafe { _mm_loadu_si128(src.add(i) as *const __m128i) };
                unsafe { _mm_storeu_si128(dst.add(i) as *mut __m128i, _mm_xor_si128(v, key)) };
                i += 16;
            }
            unsafe { unmask_tail(dst, src, i, len, mask) };
        }
    }
    _ => {
        unsafe fn unmask_raw(dst: *mut u8, src: *const u8, len: usize, mask: [u8; 4]) {
            let key = u32::from_ne_bytes(mask);
            let mut i = 0;
            while i + 4 <= len {
                let v = unsafe { (src.add(i) as *const u32).read_unaligned() } ^ key;
                unsafe { (dst.add(i) as *mut u32).write_unaligned(v) };
                i += 4;
            }
            unsafe { unmask_tail(dst, src, i, len, mask) };
        }
    }
}

unsafe fn unmask_tail(dst: *mut u8, src: *const u8, mut i: usize, len: usize, mask: [u8; 4]) {
    while i < len {
        unsafe { *dst.add(i) = *src.add(i) ^ mask[i & 3] };
        i += 1;
    }
}
