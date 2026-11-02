pub const INITIAL_SETTINGS: &[u8; 9] = &[0, 0, 0, 4, 0, 0, 0, 0, 0];

pub mod body;
pub mod bounded;

pub mod capacity;

pub mod headers;

pub mod hpack;

pub mod protocol;

pub mod recipe;

pub mod response;

pub mod retain;

pub mod session;
pub mod trailers;

mod repeater;
pub use repeater::Repeater;

pub mod tls;

pub fn recover(
    failure: dope::core::driver::life::recovery::Recovery<'_, '_>,
) -> std::convert::Infallible {
    failure.abort()
}
