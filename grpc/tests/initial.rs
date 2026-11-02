pub mod common;

use common::headers;
use sark_grpc::{Code, Reason, details::Diagnostic, protocol::response};
use sark_http::field::metadata::map::Owned;

#[test]
fn rejects_control_values() {
    for name in headers::GRPC_VALUE_FIELDS {
        for value in headers::RAW_CONTROL_VALUES {
            let fields =
                headers::try_response([Owned::try_new(name, value).expect("raw field bytes")])
                    .expect("response fields");
            let error = response::Head::from_fields(&fields)
                .err()
                .expect("initial response validates raw HTTP values before gRPC fields");
            assert!(matches!(
                Diagnostic::from(&error),
                Diagnostic::Local {
                    code: Code::InvalidArgument,
                    reason: Reason::FieldSection,
                }
            ));
        }
    }
}
