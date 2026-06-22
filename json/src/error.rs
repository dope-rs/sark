pub(super) struct Fail;

impl Fail {
    pub(super) fn bad() -> sark_core::error::Error {
        sark_core::error::Error::BadRequest("Invalid JSON body".into())
    }

    pub(super) fn with(msg: impl Into<String>) -> sark_core::error::Error {
        sark_core::error::Error::BadRequest(msg.into().into())
    }
}
