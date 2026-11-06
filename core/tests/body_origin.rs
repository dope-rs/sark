pub mod common;

use common::{CREDIT, Retained as _, Seed as _};
use sark_core::body::{
    admit::Track,
    buffered::State,
    credit::{self, raw::Account},
};

#[test]
fn accounts_release_independently() {
    Account::new().scope(|mut first| {
        let mut left = State::<(), _>::start(Track::try_start(3).expect("body"), &first);
        let _ = left.seed(&mut first, credit::Disabled);
        let mut later = State::<(), _>::start(Track::try_start(3).expect("body"), &first);
        let _ = later.seed(&mut first, credit::Disabled);
        Account::new().scope(|mut second| {
            let mut right = State::<(), _>::start(Track::try_start(3).expect("body"), &second);
            let _ = right.seed(&mut second, credit::Disabled);
            assert_eq!(first.retained().bytes(), 6);
            assert!(matches!(
                later.discard(&mut first, &mut credit::Disabled),
                credit::Discard::Applied
            ));
            assert_eq!(first.retained(), CREDIT);
            assert_eq!(second.retained(), CREDIT);
            assert!(matches!(
                left.discard(&mut first, &mut credit::Disabled),
                credit::Discard::Applied
            ));
            assert!(first.retained().is_empty());
            assert_eq!(second.retained(), CREDIT);
            assert!(matches!(
                right.discard(&mut second, &mut credit::Disabled),
                credit::Discard::Applied
            ));
            assert!(second.retained().is_empty());
        });
    });
}
