use crate::metadata;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(in crate::server::service) enum State {
    Ready,
    Failed,
}

const _: () = assert!(core::mem::size_of::<State>() == core::mem::size_of::<bool>());
const _: () = assert!(core::mem::align_of::<State>() == core::mem::align_of::<bool>());

pub(in crate::server::service) struct Parts<S, W> {
    pub(in crate::server::service) metadata: metadata::Metadata,
    pub(in crate::server::service) trailers: metadata::Metadata,
    pub(in crate::server::service) status: S,
    pub(in crate::server::service) writer: W,
    pub(in crate::server::service) state: State,
}

impl<S, W> Parts<S, W> {
    pub(in crate::server::service) fn new(status: S, writer: W) -> Self {
        Self {
            metadata: metadata::Metadata::new(),
            trailers: metadata::Metadata::new(),
            status,
            writer,
            state: State::Ready,
        }
    }

    pub(in crate::server::service) fn into_inner(
        self,
    ) -> (metadata::Metadata, metadata::Metadata, S, W) {
        (self.metadata, self.trailers, self.status, self.writer)
    }
}

impl<S, W: super::Write> Parts<S, W> {
    pub(in crate::server::service) fn fail(&mut self, status: S) {
        self.writer.clear();
        self.state = State::Failed;
        self.status = status;
    }
}
