use core::arch::aarch64::{
    uint8x16_t, vandq_u8, vceqq_u8, vcgeq_u8, vcleq_u8, vdupq_n_u8, vget_lane_u64, vld1q_u8,
    vminvq_u8, vmvnq_u8, vorrq_u8, vreinterpret_u64_u8, vreinterpretq_u16_u8, vshrn_n_u16,
};

use super::{HeaderNameOutcome, HeaderValueOutcome, scalar};

pub(super) fn scan_header_name(bytes: &[u8], start: usize) -> HeaderNameOutcome {
    // SAFETY: this module is selected only when NEON is enabled, and it loads
    // only after proving that a complete 16-byte lane remains in `bytes`.
    unsafe { scan_header_name_neon(bytes, start) }
}

pub(super) fn scan_header_value(bytes: &[u8], start: usize) -> HeaderValueOutcome {
    // SAFETY: same feature and lane-bounds proof as `scan_header_name`.
    unsafe { scan_header_value_neon(bytes, start) }
}

pub(super) fn request_target_is_valid(bytes: &[u8]) -> bool {
    // SAFETY: same feature and lane-bounds proof as `scan_header_name`.
    unsafe { request_target_is_valid_neon(bytes) }
}

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
    let punctuation = vorrq_u8(
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
    vorrq_u8(alpha_num, punctuation)
}

#[target_feature(enable = "neon")]
fn scan_header_name_neon(bytes: &[u8], start: usize) -> HeaderNameOutcome {
    let mut idx = start;
    let len = bytes.len();
    let colon = vdupq_n_u8(b':');
    let carriage_return = vdupq_n_u8(b'\r');
    while idx + 16 <= len {
        // SAFETY: the loop condition proves the full lane is in `bytes`.
        let chunk = unsafe { vld1q_u8(bytes.as_ptr().add(idx)) };
        let hit = vorrq_u8(vceqq_u8(chunk, colon), vceqq_u8(chunk, carriage_return));
        let valid_or_hit = vorrq_u8(valid_mask(chunk), hit);
        if vminvq_u8(valid_or_hit) != 0xFF {
            let invalid = vmvnq_u8(valid_or_hit);
            let hit_pos = first_match(hit);
            let invalid_pos = first_match(invalid);
            return match (hit_pos, invalid_pos) {
                (Some(hit_offset), Some(invalid_offset)) if invalid_offset < hit_offset => {
                    HeaderNameOutcome::Invalid
                }
                (Some(hit_offset), _) => {
                    let pos = idx + hit_offset;
                    HeaderNameOutcome::Found {
                        pos,
                        byte: bytes[pos],
                    }
                }
                (None, Some(_)) => HeaderNameOutcome::Invalid,
                (None, None) => unreachable!(),
            };
        }
        if let Some(hit_offset) = first_match(hit) {
            let pos = idx + hit_offset;
            return HeaderNameOutcome::Found {
                pos,
                byte: bytes[pos],
            };
        }
        idx += 16;
    }
    scalar::scan_header_name(bytes, idx)
}

#[target_feature(enable = "neon")]
fn scan_header_value_neon(bytes: &[u8], start: usize) -> HeaderValueOutcome {
    let mut idx = start;
    let len = bytes.len();
    let control_max = vdupq_n_u8(0x1f);
    let tab = vdupq_n_u8(b'\t');
    let delete = vdupq_n_u8(0x7f);
    while idx + 16 <= len {
        // SAFETY: the loop condition proves the full lane is in `bytes`.
        let chunk = unsafe { vld1q_u8(bytes.as_ptr().add(idx)) };
        let low_control = vandq_u8(vcleq_u8(chunk, control_max), vmvnq_u8(vceqq_u8(chunk, tab)));
        let special = vorrq_u8(low_control, vceqq_u8(chunk, delete));
        if let Some(offset) = first_match(special) {
            return scalar::scan_header_value(bytes, idx + offset);
        }
        idx += 16;
    }
    scalar::scan_header_value(bytes, idx)
}

#[target_feature(enable = "neon")]
fn request_target_is_valid_neon(bytes: &[u8]) -> bool {
    let mut chunks = bytes.chunks_exact(16);
    let space = vdupq_n_u8(0x20);
    let delete = vdupq_n_u8(0x7f);
    for chunk in &mut chunks {
        // SAFETY: `chunks_exact(16)` yields a complete vector lane.
        let vector = unsafe { vld1q_u8(chunk.as_ptr()) };
        let invalid = vorrq_u8(vcleq_u8(vector, space), vceqq_u8(vector, delete));
        if first_match(invalid).is_some() {
            return false;
        }
    }
    scalar::request_target_is_valid(chunks.remainder())
}
