pub mod error;
pub mod http;
pub mod simd;
pub mod test_util;
pub mod utils;

#[inline]
pub fn identity_mut<T>(x: &mut T) -> &mut T {
    x
}
