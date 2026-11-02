use sark_http::field::{self, base64::Encoder};

use crate::metadata::Metadata;

/// Emits borrowed metadata using its insertion-time field proofs.
pub(super) fn emit<'field>(metadata: &'field Metadata, sink: &mut impl field::Sink<'field>) {
    use sark_http::field::raw::Admit as _;

    for entry in metadata.entries() {
        // SAFETY: Metadata admitted this immutable owner's lowercase field name at insertion.
        let name = unsafe { field::Lower::from_raw(entry.name) };
        if entry.name.ends_with(b"-bin") {
            sink.encoded_with(name, Encoder::new(entry.value), entry.indexing);
        } else {
            // SAFETY: Nonbinary Metadata values were admitted as visible ASCII at insertion.
            let value = unsafe { field::Value::from_raw(entry.value) };
            sink.entry(field::Entry::new(name, value).with_index(entry.indexing));
        }
    }
}
