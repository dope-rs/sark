mod request;

pub use request::Request;

pub mod body;
pub mod discard;
pub mod fields;

pub mod app;
pub mod tasks;

pub mod head;
pub mod session;

pub fn recover(
    failure: dope::core::driver::life::recovery::Recovery<'_, '_>,
) -> std::convert::Infallible {
    failure.abort()
}
