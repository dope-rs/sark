use std::time;

use o3::base::time::Positive;
use sark_core::life::session::{Config, ConfigError};

#[test]
fn rejects_zero_windows() {
    let one = time::Duration::from_secs(1);
    assert!(
        Config::try_new(time::Duration::ZERO, one)
            == Err(ConfigError::MaxAge {
                source: o3::base::time::Zero,
            })
    );
    assert!(
        Config::try_new(one, time::Duration::ZERO)
            == Err(ConfigError::DrainTimeout {
                source: o3::base::time::Zero,
            })
    );
}

#[test]
fn config_preserves_windows() {
    let secs_300 = (Positive::try_new(time::Duration::from_secs(300)))
        .expect("the fixture window is positive");
    let secs_10 =
        (Positive::try_new(time::Duration::from_secs(10))).expect("the fixture window is positive");
    let max_age = secs_300;
    let drain = secs_10;
    let config = Config::new(max_age, drain);

    assert!((config.age()) == (max_age));
    assert!((config.drain()) == (drain));
}
