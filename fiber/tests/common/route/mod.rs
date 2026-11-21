use dope::core::driver::{life, life::recovery::Policy, settings};
use dope_test::scope::{Driver, Error};

pub mod body;
pub mod chunked;
pub mod glue;
pub mod keep;
pub mod metadata;
pub mod select;
pub mod skip;

pub fn with_scope(
    config: settings::Config,
    run: impl for<'a, 'd> FnOnce(&mut life::Scope<'a, 'd>),
) {
    let mut reports = Vec::new();
    Driver::new(config).with_scope(
        run,
        Policy::new(|event| reports.push(event), super::server::recover),
        |result| {
            result.as_ref().map_err(Error::cause).expect("route driver");
        },
    );
    assert!(reports.is_empty(), "driver retirement: {reports:?}");
}
