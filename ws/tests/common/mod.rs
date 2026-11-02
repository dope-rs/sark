pub mod echo;
mod mask;
pub use mask::Mask;
pub mod peer;
pub mod protocol;

pub mod upgrade;

pub mod app;
pub mod client;
pub mod server;

pub fn recover(
    failure: dope::core::driver::life::recovery::Recovery<'_, '_>,
) -> std::convert::Infallible {
    failure.abort()
}
