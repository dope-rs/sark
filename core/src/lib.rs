pub mod error;
pub mod http;
pub mod simd;
pub mod utils;

pub fn identity_mut<T>(x: &mut T) -> &mut T {
    x
}
