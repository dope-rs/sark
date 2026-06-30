#[derive(Debug, PartialEq, Eq)]
pub enum HeaderNameOutcome {
    Found { pos: usize, byte: u8 },
    Invalid,
    None,
}

fn is_header_name_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn scan_header_name_scalar(bytes: &[u8], start: usize) -> HeaderNameOutcome {
    let mut idx = start;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b == b':' || b == b'\r' {
            return HeaderNameOutcome::Found { pos: idx, byte: b };
        }
        if !is_header_name_byte(b) {
            return HeaderNameOutcome::Invalid;
        }
        idx += 1;
    }
    HeaderNameOutcome::None
}

cfg_select! {
    target_arch = "aarch64" => {
        pub fn scan_header_name(bytes: &[u8], start: usize) -> HeaderNameOutcome {
            if start >= bytes.len() {
                return HeaderNameOutcome::None;
            }
            // SAFETY: aarch64 baseline always includes the neon target feature.
            unsafe { scan_header_name_neon(bytes, start) }
        }

        use core::arch::aarch64::{
            uint8x16_t, vandq_u8, vceqq_u8, vcgeq_u8, vcleq_u8, vdupq_n_u8,
            vget_lane_u64, vld1q_u8, vminvq_u8, vmvnq_u8, vorrq_u8,
            vreinterpret_u64_u8, vreinterpretq_u16_u8, vshrn_n_u16,
        };

        #[target_feature(enable = "neon")]
        fn first_match(mask: uint8x16_t) -> Option<usize> {
            let narrow = vshrn_n_u16(vreinterpretq_u16_u8(mask), 4);
            let bits = vget_lane_u64(vreinterpret_u64_u8(narrow), 0);
            if bits == 0 {
                None
            } else {
                Some((bits.trailing_zeros() as usize) >> 2)
            }
        }

        #[target_feature(enable = "neon")]
        fn valid_mask(chunk: uint8x16_t) -> uint8x16_t {
            let upper = vandq_u8(
                    vcgeq_u8(chunk, vdupq_n_u8(b'A')),
                    vcleq_u8(chunk, vdupq_n_u8(b'Z')),
                );
            let lower = vandq_u8(
                    vcgeq_u8(chunk, vdupq_n_u8(b'a')),
                    vcleq_u8(chunk, vdupq_n_u8(b'z')),
                );
            let digit = vandq_u8(
                    vcgeq_u8(chunk, vdupq_n_u8(b'0')),
                    vcleq_u8(chunk, vdupq_n_u8(b'9')),
                );
            let alpha_num = vorrq_u8(vorrq_u8(upper, lower), digit);
            let punct = vorrq_u8(
                    vorrq_u8(
                        vorrq_u8(
                            vceqq_u8(chunk, vdupq_n_u8(b'!')),
                            vceqq_u8(chunk, vdupq_n_u8(b'#')),
                        ),
                        vorrq_u8(
                            vceqq_u8(chunk, vdupq_n_u8(b'$')),
                            vceqq_u8(chunk, vdupq_n_u8(b'%')),
                        ),
                    ),
                    vorrq_u8(
                        vorrq_u8(
                            vorrq_u8(
                                vceqq_u8(chunk, vdupq_n_u8(b'&')),
                                vceqq_u8(chunk, vdupq_n_u8(b'\'')),
                            ),
                            vorrq_u8(
                                vceqq_u8(chunk, vdupq_n_u8(b'*')),
                                vceqq_u8(chunk, vdupq_n_u8(b'+')),
                            ),
                        ),
                        vorrq_u8(
                            vorrq_u8(
                                vorrq_u8(
                                    vceqq_u8(chunk, vdupq_n_u8(b'-')),
                                    vceqq_u8(chunk, vdupq_n_u8(b'.')),
                                ),
                                vorrq_u8(
                                    vceqq_u8(chunk, vdupq_n_u8(b'^')),
                                    vceqq_u8(chunk, vdupq_n_u8(b'_')),
                                ),
                            ),
                            vorrq_u8(
                                vceqq_u8(chunk, vdupq_n_u8(b'`')),
                                vorrq_u8(
                                    vceqq_u8(chunk, vdupq_n_u8(b'|')),
                                    vceqq_u8(chunk, vdupq_n_u8(b'~')),
                                ),
                            ),
                        ),
                    ),
                );
            vorrq_u8(alpha_num, punct)
        }

        #[target_feature(enable = "neon")]
        fn scan_header_name_neon(bytes: &[u8], start: usize) -> HeaderNameOutcome {
            let mut idx = start;
            let len = bytes.len();
            let colon_v = vdupq_n_u8(b':');
            let cr_v = vdupq_n_u8(b'\r');
            while idx + 16 <= len {
                // SAFETY: idx + 16 <= len bounds the 16-byte load inside bytes.
                let chunk = unsafe { vld1q_u8(bytes.as_ptr().add(idx)) };
                let hit = vorrq_u8(vceqq_u8(chunk, colon_v), vceqq_u8(chunk, cr_v));
                let valid_or_hit = vorrq_u8(valid_mask(chunk), hit);
                if vminvq_u8(valid_or_hit) != 0xFF {
                    let invalid = vmvnq_u8(valid_or_hit);
                    let hit_pos = first_match(hit);
                    let invalid_pos = first_match(invalid);
                    return match (hit_pos, invalid_pos) {
                        (Some(hit_off), Some(invalid_off)) if invalid_off < hit_off => {
                            HeaderNameOutcome::Invalid
                        }
                        (Some(hit_off), _) => {
                            let pos = idx + hit_off;
                            HeaderNameOutcome::Found { pos, byte: bytes[pos] }
                        }
                        (None, Some(_)) => HeaderNameOutcome::Invalid,
                        (None, None) => unreachable!(),
                    };
                }
                if let Some(hit_off) = first_match(hit) {
                    let pos = idx + hit_off;
                    return HeaderNameOutcome::Found { pos, byte: bytes[pos] };
                }
                idx += 16;
            }
            scan_header_name_scalar(bytes, idx)
        }
    }
    target_arch = "x86_64" => {
        pub fn scan_header_name(bytes: &[u8], start: usize) -> HeaderNameOutcome {
            if start >= bytes.len() {
                return HeaderNameOutcome::None;
            }
            // SAFETY: sse2 is guaranteed on the x86_64 baseline.
            unsafe { scan_header_name_sse2(bytes, start) }
        }

        use core::arch::x86_64::{
            __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_max_epu8, _mm_min_epu8,
            _mm_movemask_epi8, _mm_or_si128, _mm_set1_epi8,
        };

        const LANE_MASK: u32 = 0xFFFF;

        unsafe fn lane_eq(chunk: __m128i, byte: u8) -> __m128i {
            unsafe { _mm_cmpeq_epi8(chunk, _mm_set1_epi8(byte as i8)) }
        }

        unsafe fn in_range(chunk: __m128i, lo: u8, hi: u8) -> __m128i {
            unsafe {
                let clamped =
                    _mm_min_epu8(_mm_max_epu8(chunk, _mm_set1_epi8(lo as i8)), _mm_set1_epi8(hi as i8));
                _mm_cmpeq_epi8(chunk, clamped)
            }
        }

        unsafe fn valid_mask(chunk: __m128i) -> __m128i {
            unsafe {
                let alpha_num = _mm_or_si128(
                    _mm_or_si128(in_range(chunk, b'A', b'Z'), in_range(chunk, b'a', b'z')),
                    in_range(chunk, b'0', b'9'),
                );
                let punct = _mm_or_si128(
                    _mm_or_si128(
                        _mm_or_si128(
                            _mm_or_si128(lane_eq(chunk, b'!'), lane_eq(chunk, b'#')),
                            _mm_or_si128(lane_eq(chunk, b'$'), lane_eq(chunk, b'%')),
                        ),
                        _mm_or_si128(
                            _mm_or_si128(lane_eq(chunk, b'&'), lane_eq(chunk, b'\'')),
                            _mm_or_si128(lane_eq(chunk, b'*'), lane_eq(chunk, b'+')),
                        ),
                    ),
                    _mm_or_si128(
                        _mm_or_si128(
                            _mm_or_si128(lane_eq(chunk, b'-'), lane_eq(chunk, b'.')),
                            _mm_or_si128(lane_eq(chunk, b'^'), lane_eq(chunk, b'_')),
                        ),
                        _mm_or_si128(
                            lane_eq(chunk, b'`'),
                            _mm_or_si128(lane_eq(chunk, b'|'), lane_eq(chunk, b'~')),
                        ),
                    ),
                );
                _mm_or_si128(alpha_num, punct)
            }
        }

        unsafe fn scan_header_name_sse2(bytes: &[u8], start: usize) -> HeaderNameOutcome {
            let mut idx = start;
            let len = bytes.len();
            while idx + 16 <= len {
                // SAFETY: idx + 16 <= len bounds the 16-byte load inside bytes.
                let chunk = unsafe { _mm_loadu_si128(bytes.as_ptr().add(idx) as *const __m128i) };
                let hit_v = unsafe { _mm_or_si128(lane_eq(chunk, b':'), lane_eq(chunk, b'\r')) };
                let valid_or_hit = unsafe { _mm_or_si128(valid_mask(chunk), hit_v) };
                let hit = unsafe { _mm_movemask_epi8(hit_v) } as u32;
                let invalid = unsafe { _mm_movemask_epi8(valid_or_hit) } as u32 ^ LANE_MASK;
                if invalid != 0 {
                    let inv_off = invalid.trailing_zeros();
                    if hit != 0 && hit.trailing_zeros() < inv_off {
                        let pos = idx + hit.trailing_zeros() as usize;
                        return HeaderNameOutcome::Found { pos, byte: bytes[pos] };
                    }
                    return HeaderNameOutcome::Invalid;
                }
                if hit != 0 {
                    let pos = idx + hit.trailing_zeros() as usize;
                    return HeaderNameOutcome::Found { pos, byte: bytes[pos] };
                }
                idx += 16;
            }
            scan_header_name_scalar(bytes, idx)
        }
    }
    _ => {
        pub fn scan_header_name(bytes: &[u8], start: usize) -> HeaderNameOutcome {
            if start >= bytes.len() {
                return HeaderNameOutcome::None;
            }
            scan_header_name_scalar(bytes, start)
        }
    }
}
