//! Bodies and ready outputs retain account identity even when retained amounts match.
//! ```
//! # mod common;
//! # fn main() {
//! use common::{CREDIT, Retained as _, Seed as _};
//! use sark_core::body::{admit::Track, buffered::State, credit::{self, raw::Account}};
//! Account::new().scope(|mut first| {
//!     let mut left = State::<(), _>::start(Track::try_start(3).expect("body"), &first);
//!     left.seed(&mut first, credit::Disabled);
//!     let mut later = State::<(), _>::start(Track::try_start(3).expect("body"), &first);
//!     later.seed(&mut first, credit::Disabled);
//!     Account::new().scope(|mut second| {
//!         let mut right = State::<(), _>::start(Track::try_start(3).expect("body"), &second);
//!         right.seed(&mut second, credit::Disabled);
//!         assert_eq!(first.retained().bytes(), 6);
//!         assert!(matches!(later.discard(&mut first, &mut credit::Disabled), credit::Discard::Applied));
//!         assert_eq!(first.retained(), CREDIT);
//!         assert_eq!(second.retained(), CREDIT);
//!         assert!(matches!(left.discard(&mut first, &mut credit::Disabled), credit::Discard::Applied));
//!         assert!(first.retained().is_empty());
//!         assert_eq!(second.retained(), CREDIT);
//!         assert!(matches!(right.discard(&mut second, &mut credit::Disabled), credit::Discard::Applied));
//!         assert!(second.retained().is_empty());
//!     });
//! });
//! # }
//! ```
//!
//! ```compile_fail,E0521
//! # mod common;
//! # fn main() {
//! use common::{CREDIT, Retained as _, Seed as _};
//! use sark_core::body::{admit::Track, buffered::State, credit::{self, raw::Account}};
//! Account::new().scope(|mut first| {
//!     let mut left = State::<(), _>::start(Track::try_start(3).expect("body"), &first);
//!     left.seed(&mut first, credit::Disabled);
//!     Account::new().scope(|mut second| {
//!         let mut right = State::<(), _>::start(Track::try_start(3).expect("body"), &second);
//!         right.seed(&mut second, credit::Disabled);
//!         assert_eq!(first.retained(), CREDIT);
//!         assert_eq!(second.retained(), CREDIT);
//!         left.discard(&mut second, &mut credit::Disabled);
//!     });
//! });
//! # }
//! ```
//!
//! ```compile_fail,E0521
//! # mod common;
//! # fn main() {
//! use std::{cell::Cell, num::NonZero};
//! use common::{CREDIT, Retained as _, Seed as _, ready::{self, Ready}};
//! use o3::buffer::resident;
//! use sark_core::{
//!     body::{admit::Track, buffered::State, credit::{self, raw::Account}, input::Checked, stream::Drive},
//!     manifold::stream::Boundary,
//! };
//! use sark_http::payload::owned;
//! Account::new().scope(|mut first| {
//!     let valid = Cell::new(true);
//!     let open = Track::try_start(3).expect("body");
//!     let source = owned::Segment::from(resident::Shared::from(ready::BYTES));
//!     let (body, _) = open.retain(source, &mut first);
//!     let stream = body.try_stream(Ready::new(Ok(()), &valid), Boundary::End, Checked::new)
//!         .ok().expect("retained stream");
//!     let (step, evidence) = stream.try_drive(&mut first, credit::Disabled, NonZero::<usize>::MIN);
//!     let Drive::Ready(ready) = step else { return; };
//!     Account::new().scope(|mut second| {
//!         let mut right = State::<(), _>::start(Track::try_start(3).expect("body"), &second);
//!         right.seed(&mut second, credit::Disabled);
//!         assert_eq!(first.retained(), CREDIT);
//!         assert_eq!(second.retained(), CREDIT);
//!         ready.try_admit(&mut second, evidence);
//!     });
//! });
//! # }
//! ```
//!
//! A charged state cannot safely erase its identity into persistent storage.
//! ```compile_fail,E0308
//! # mod common;
//! # fn main() {
//! use common::{Retained as _, Seed as _};
//! use sark_core::body::{admit::Track, buffered::State, credit::{self, raw::Account}};
//! let mut slot = None::<State<()>>;
//! Account::new().scope(|mut ledger| {
//!     let mut body = State::<(), _>::start(Track::try_start(3).expect("body"), &ledger);
//!     body.seed(&mut ledger, credit::Disabled);
//!     slot = Some(body);
//! });
//! # }
//! ```
//!
//! A carrier must prove exact custody before storing or rebinding its state.
//! ```compile_fail,E0133
//! # mod common;
//! # fn main() {
//! use common::{Retained as _, Seed as _};
//! use sark_core::body::{admit::Track, buffered::State, credit::{self, raw::Account}};
//! Account::new().scope(|mut ledger| {
//!     let mut body = State::<(), _>::start(Track::try_start(3).expect("body"), &ledger);
//!     body.seed(&mut ledger, credit::Disabled);
//!     let _ = ledger.store(body);
//! });
//! # }
//! ```
//!
//! ```compile_fail,E0133
//! # mod common;
//! # fn main() {
//! use common::Retained as _;
//! use sark_core::body::{admit::Track, buffered::State, credit::raw::Account};
//! let stored = State::<()>::new(Track::try_start(3).expect("body"));
//! Account::new().scope(|ledger| {
//!     let _ = ledger.bind(stored);
//! });
//! # }
//! ```

use std::env;

use dope_test::checks::{Error, Suite};

#[test]
fn origin_contracts() -> Result<(), Error> {
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &["sark-core", "sark-http", "o3", "dope-core", "dope-test"],
    );
    let exe = env::current_exe()?;
    let output = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.output()?;
    let artifacts = suite.try_collect(&exe, output)?;
    assert_eq!(artifacts.try_errors(include_str!("body_origin.rs"))?, 5);
    let status = artifacts.try_rustdoc()?.status()?;
    assert!(status.success(), "body account identity: {status}");
    Ok(())
}
