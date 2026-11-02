use sark_h2::stream::Id;

/// Handler view carrying only the transport's observable wire identity.
pub struct Stream {
    stream_id: Id,
}

const _: () = {
    assert!(core::mem::size_of::<Stream>() == core::mem::size_of::<u32>());
    assert!(core::mem::size_of::<Option<Stream>>() == core::mem::size_of::<Stream>());
};

impl Stream {
    #[doc(hidden)]
    pub const fn new(id: Id) -> Self {
        Self { stream_id: id }
    }

    pub fn stream_id(&self) -> Id {
        self.stream_id
    }
}
