//! Preparation failures for generated server entry points.

use dope::core::driver::settings::errors;
use sark_h1::server::date;

/// Preparation failure before a dependent server enters its driver.
#[derive(Debug)]
pub enum PrepareError {
    Date(date::Error),
    Layout(errors::Claim),
}
