use std::arch::x86_64;

use dope_core::dope_sys::thread;

use crate::payload::crc32::{Backend, POLYNOMIAL, shift, table};

/// Reciprocal polynomial for reflected little-endian input without bit reversal.
const Q: u64 = ((POLYNOMIAL as u64) << 1) | 1;
const GROUP: [i64; 2] = [shift(1, 512) as i64, shift(1, 448) as i64];
const BLOCK: [i64; 2] = [shift(1, 128) as i64, shift(1, 64) as i64];
const WORD: i64 = shift(1, 32) as i64;
/// Q inverse modulo x^32, cancelling successive low product coefficients.
const INVERSE: u32 = {
    let mut inverse = 1_u32;
    let mut product = Q;
    let mut bit = 1;
    while bit < 32 {
        if product & (1 << bit) != 0 {
            inverse ^= 1 << bit;
            product ^= Q << bit;
        }
        bit += 1;
    }
    inverse
};

/// Constructed only after the CPU admits every instruction used below.
#[derive(Clone, Debug)]
pub(super) struct X86(());

impl Backend for X86 {
    fn admit() -> Option<Self> {
        match <dope_core::Backend as thread::Thread>::try_crc32() {
            Ok(true) => Some(Self(())),
            Ok(false) | Err(_) => None,
        }
    }

    fn update(&self, value: u32, bytes: &[u8]) -> u32 {
        let Some((first, tail)) = bytes.split_first_chunk::<128>() else {
            return table::update(value, bytes);
        };
        // SAFETY: this retained admission proves SSE2 and PCLMULQDQ support;
        // the exact initial array carries the admitted two-group extent.
        unsafe { update(value, first, tail) }
    }
}

#[target_feature(enable = "sse2,pclmulqdq")]
unsafe fn update(value: u32, first: &[u8; 128], bytes: &[u8]) -> u32 {
    let (blocks, tail) = bytes.as_chunks::<64>();
    let initial = first.as_chunks::<64>().0;
    // SAFETY: all vector loads receive exact array extents. This function's
    // admitted features are preserved by every helper call.
    unsafe {
        let [a, b, c, d] = load(&initial[0]);
        let [next_a, next_b, next_c, next_d] = load(&initial[1]);
        let step = x86_64::_mm_set_epi64x(GROUP[1], GROUP[0]);
        let mut a = fold(
            x86_64::_mm_xor_si128(a, x86_64::_mm_cvtsi32_si128(value as i32)),
            next_a,
            step,
        );
        let mut b = fold(b, next_b, step);
        let mut c = fold(c, next_c, step);
        let mut d = fold(d, next_d, step);
        for block in blocks {
            let [next_a, next_b, next_c, next_d] = load(block);
            a = fold(a, next_a, step);
            b = fold(b, next_b, step);
            c = fold(c, next_c, step);
            d = fold(d, next_d, step);
        }

        let step = x86_64::_mm_set_epi64x(BLOCK[1], BLOCK[0]);
        let mut state = fold(fold(fold(a, b, step), c, step), d, step);
        let (blocks, tail) = tail.as_chunks::<16>();
        for block in blocks {
            state = fold(state, x86_64::_mm_loadu_si128(block.as_ptr().cast()), step);
        }
        table::update(reduce(state), tail)
    }
}

#[target_feature(enable = "sse2")]
unsafe fn load(block: &[u8; 64]) -> [x86_64::__m128i; 4] {
    // SAFETY: each unaligned load reads one disjoint 16-byte range of the
    // retained 64-byte array; this function admits SSE2.
    unsafe {
        let start = block.as_ptr();
        [
            x86_64::_mm_loadu_si128(start.cast()),
            x86_64::_mm_loadu_si128(start.add(16).cast()),
            x86_64::_mm_loadu_si128(start.add(32).cast()),
            x86_64::_mm_loadu_si128(start.add(48).cast()),
        ]
    }
}

/// Two products represent the low and high halves multiplied by x^-n modulo Q.
#[target_feature(enable = "sse2,pclmulqdq")]
unsafe fn fold(
    state: x86_64::__m128i,
    input: x86_64::__m128i,
    powers: x86_64::__m128i,
) -> x86_64::__m128i {
    let low = x86_64::_mm_clmulepi64_si128::<0x00>(state, powers);
    let high = x86_64::_mm_clmulepi64_si128::<0x11>(state, powers);
    x86_64::_mm_xor_si128(input, x86_64::_mm_xor_si128(low, high))
}

/// Divide by x^64 and x^32, then cancel the low word with Q inverse.
/// The high word is the resulting division by x^32 modulo Q.
#[target_feature(enable = "sse2,pclmulqdq")]
unsafe fn reduce(state: x86_64::__m128i) -> u32 {
    let low32 = x86_64::_mm_set_epi64x(0, u32::MAX as i64);
    let state = x86_64::_mm_xor_si128(
        x86_64::_mm_clmulepi64_si128::<0x00>(state, x86_64::_mm_set_epi64x(0, BLOCK[1])),
        x86_64::_mm_srli_si128::<8>(state),
    );
    let state = x86_64::_mm_xor_si128(
        x86_64::_mm_clmulepi64_si128::<0x00>(
            x86_64::_mm_and_si128(state, low32),
            x86_64::_mm_set_epi64x(0, WORD),
        ),
        x86_64::_mm_srli_si128::<4>(state),
    );

    let quotient = x86_64::_mm_and_si128(
        x86_64::_mm_clmulepi64_si128::<0x00>(
            x86_64::_mm_and_si128(state, low32),
            x86_64::_mm_set_epi64x(0, INVERSE as i64),
        ),
        low32,
    );
    let result = x86_64::_mm_xor_si128(
        state,
        x86_64::_mm_clmulepi64_si128::<0x00>(quotient, x86_64::_mm_set_epi64x(0, Q as i64)),
    );
    x86_64::_mm_cvtsi128_si32(x86_64::_mm_srli_si128::<4>(result)) as u32
}
