pub mod common;

use common::field::sections::PATH_VALUES;
use sark_http::field::{
    head,
    metadata::{block, map::record::Span},
};

#[test]
fn compressed_paths() {
    for &(value, expected) in PATH_VALUES {
        for split in 0..=value.len() {
            let mut gate = block::Gate::new(
                block::Kind::Request,
                block::Connect::Disabled,
                block::Version::Two,
            );
            assert_eq!(
                gate.accept_value(
                    head::Name::Path,
                    Span::new(&value[..split], &value[split..])
                ),
                expected,
                "{value:?} split at {split}",
            );
        }
    }
}
