use core::arch::x86_64::{
    __m128i, _mm_andnot_si128, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_max_epu8, _mm_min_epu8,
    _mm_movemask_epi8, _mm_or_si128, _mm_set1_epi8,
};

use super::{HeaderNameOutcome, HeaderValueOutcome, scalar};

const LANE_MASK: u32 = 0xFFFF;

pub(super) fn scan_header_name(bytes: &[u8], start: usize) -> HeaderNameOutcome {
    // SAFETY: SSE2 is part of the x86_64 baseline, and the backend loads only
    // after proving that a complete 16-byte lane remains in `bytes`.
    unsafe { scan_header_name_sse2(bytes, start) }
}

pub(super) fn scan_header_value(bytes: &[u8], start: usize) -> HeaderValueOutcome {
    // SAFETY: same baseline and lane-bounds proof as `scan_header_name`.
    unsafe { scan_header_value_sse2(bytes, start) }
}

pub(super) fn request_target_is_valid(bytes: &[u8]) -> bool {
    // SAFETY: same baseline and lane-bounds proof as `scan_header_name`.
    unsafe { request_target_is_valid_sse2(bytes) }
}

#[target_feature(enable = "sse2")]
fn lane_eq(chunk: __m128i, byte: u8) -> __m128i {
    _mm_cmpeq_epi8(chunk, _mm_set1_epi8(byte as i8))
}

#[target_feature(enable = "sse2")]
fn in_range(chunk: __m128i, lo: u8, hi: u8) -> __m128i {
    let clamped = _mm_min_epu8(
        _mm_max_epu8(chunk, _mm_set1_epi8(lo as i8)),
        _mm_set1_epi8(hi as i8),
    );
    _mm_cmpeq_epi8(chunk, clamped)
}

#[target_feature(enable = "sse2")]
fn valid_mask(chunk: __m128i) -> __m128i {
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

#[target_feature(enable = "sse2")]
fn scan_header_name_sse2(bytes: &[u8], start: usize) -> HeaderNameOutcome {
    let mut idx = start;
    let len = bytes.len();
    while idx + 16 <= len {
        // SAFETY: the loop condition proves the full unaligned lane is in `bytes`.
        let chunk = unsafe { _mm_loadu_si128(bytes.as_ptr().add(idx).cast::<__m128i>()) };
        let hit_vector = _mm_or_si128(lane_eq(chunk, b':'), lane_eq(chunk, b'\r'));
        let valid_or_hit = _mm_or_si128(valid_mask(chunk), hit_vector);
        let hit = _mm_movemask_epi8(hit_vector) as u32;
        let invalid = _mm_movemask_epi8(valid_or_hit) as u32 ^ LANE_MASK;
        if invalid != 0 {
            let invalid_offset = invalid.trailing_zeros();
            if hit != 0 && hit.trailing_zeros() < invalid_offset {
                let pos = idx + hit.trailing_zeros() as usize;
                return HeaderNameOutcome::Found {
                    pos,
                    byte: bytes[pos],
                };
            }
            return HeaderNameOutcome::Invalid;
        }
        if hit != 0 {
            let pos = idx + hit.trailing_zeros() as usize;
            return HeaderNameOutcome::Found {
                pos,
                byte: bytes[pos],
            };
        }
        idx += 16;
    }
    scalar::scan_header_name(bytes, idx)
}

#[target_feature(enable = "sse2")]
fn scan_header_value_sse2(bytes: &[u8], start: usize) -> HeaderValueOutcome {
    let mut idx = start;
    let len = bytes.len();
    while idx + 16 <= len {
        // SAFETY: the loop condition proves the full unaligned lane is in `bytes`.
        let chunk = unsafe { _mm_loadu_si128(bytes.as_ptr().add(idx).cast::<__m128i>()) };
        let low_control = _mm_andnot_si128(
            lane_eq(chunk, b'\t'),
            _mm_cmpeq_epi8(_mm_min_epu8(chunk, _mm_set1_epi8(0x1f)), chunk),
        );
        let special = _mm_or_si128(low_control, lane_eq(chunk, 0x7f));
        let mask = _mm_movemask_epi8(special) as u32;
        if mask != 0 {
            return scalar::scan_header_value(bytes, idx + mask.trailing_zeros() as usize);
        }
        idx += 16;
    }
    scalar::scan_header_value(bytes, idx)
}

#[target_feature(enable = "sse2")]
fn request_target_is_valid_sse2(bytes: &[u8]) -> bool {
    let mut chunks = bytes.chunks_exact(16);
    for chunk in &mut chunks {
        // SAFETY: `chunks_exact(16)` yields a complete 16-byte lane.
        let vector = unsafe { _mm_loadu_si128(chunk.as_ptr().cast::<__m128i>()) };
        let low = _mm_cmpeq_epi8(_mm_min_epu8(vector, _mm_set1_epi8(0x20)), vector);
        let invalid = _mm_or_si128(low, lane_eq(vector, 0x7f));
        if _mm_movemask_epi8(invalid) != 0 {
            return false;
        }
    }
    scalar::request_target_is_valid(chunks.remainder())
}
