use std::path::PathBuf;
use std::pin::Pin;
use std::task::Poll;

use dope::io::file::{O_CLOEXEC, O_RDONLY, OpenPath};
use dope::manifold::file::Files;
use dope_fiber::file;
use dope_fiber::{Context, Fiber};
use o3::buffer::Shared;
use o3::mem::{ByteBudgetHandle, ByteLease};
use sark_core::http::Response;

#[derive(Clone, Copy)]
pub(super) enum LoadError {
    NotFound,
    Overloaded,
}

impl LoadError {
    pub(super) fn response(self) -> Response {
        match self {
            Self::NotFound => Response::not_found(),
            Self::Overloaded => Response::new(sark_core::http::StatusCode::SERVICE_UNAVAILABLE),
        }
    }
}

pub(super) struct LoadedFile {
    pub(super) body: Shared,
    pub(super) metadata: file::Metadata,
}

pub(super) struct ReadFile<'f, 'b, 'd, const ID: u8, const N: usize> {
    files: &'f Files<'d, ID, N>,
    path: Option<PathBuf>,
    budget: ByteBudgetHandle<'b>,
    max_file_bytes: usize,
    lease: Option<ByteLease<'b>>,
    metadata: Option<file::Metadata>,
    source: Option<file::Source<'d>>,
    open: Option<file::Open<'f, 'd, ID, N>>,
    stat: Option<file::Stat<'f, 'd, ID, N>>,
    read: Option<file::Read<'f, 'd, ID, N>>,
    done: bool,
}

impl<'f, 'b, 'd, const ID: u8, const N: usize> ReadFile<'f, 'b, 'd, ID, N> {
    pub(super) fn new(
        files: &'f Files<'d, ID, N>,
        path: PathBuf,
        budget: ByteBudgetHandle<'b>,
        max_file_bytes: usize,
    ) -> Self {
        Self {
            files,
            path: Some(path),
            budget,
            max_file_bytes,
            lease: None,
            metadata: None,
            source: None,
            open: None,
            stat: None,
            read: None,
            done: false,
        }
    }

    fn release(&mut self) {
        self.lease = None;
    }

    fn fail(&mut self, error: LoadError) -> Poll<Result<LoadedFile, LoadError>> {
        self.release();
        self.done = true;
        Poll::Ready(Err(error))
    }
}

impl<const ID: u8, const N: usize> Drop for ReadFile<'_, '_, '_, ID, N> {
    fn drop(&mut self) {
        self.release();
    }
}

impl<'f, 'b, 'd, const ID: u8, const N: usize> Fiber<'d> for ReadFile<'f, 'b, 'd, ID, N> {
    type Output = Result<LoadedFile, LoadError>;

    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.done {
            panic!("file fiber polled after completion");
        }

        if this.open.is_none() && this.stat.is_none() && this.read.is_none() {
            let path = this.path.take().expect("file path missing");
            let Some(path) = path.to_str() else {
                return this.fail(LoadError::NotFound);
            };
            let Ok(path) = OpenPath::new(path) else {
                return this.fail(LoadError::NotFound);
            };
            this.open = Some(file::Open::direct(this.files, path, O_RDONLY | O_CLOEXEC));
        }

        if let Some(open) = this.open.as_mut() {
            let Poll::Ready(result) = Fiber::poll(Pin::new(open), cx.as_mut()) else {
                return Poll::Pending;
            };
            this.open = None;
            let Ok(source) = result else {
                return this.fail(LoadError::NotFound);
            };
            this.source = Some(source);
            this.stat = Some(file::Stat::source(
                this.files,
                this.source.as_ref().expect("file source missing"),
            ));
        }

        if let Some(stat) = this.stat.as_mut() {
            let Poll::Ready(result) = Fiber::poll(Pin::new(stat), cx.as_mut()) else {
                return Poll::Pending;
            };
            this.stat = None;
            let Ok(metadata) = result else {
                return this.fail(LoadError::NotFound);
            };
            if !metadata.is_file() {
                return this.fail(LoadError::NotFound);
            }
            let Ok(expected) = usize::try_from(metadata.len()) else {
                return this.fail(LoadError::NotFound);
            };
            if expected > this.max_file_bytes {
                return this.fail(LoadError::NotFound);
            }
            let Some(lease) = this.budget.try_acquire(expected) else {
                return this.fail(LoadError::Overloaded);
            };
            this.lease = Some(lease);
            this.metadata = Some(metadata);
            let source = this.source.take().expect("file source missing");
            this.read = Some(file::Read::new(this.files, &source, vec![0; expected], 0));
        }

        let read = this.read.as_mut().expect("read child missing");
        let Poll::Ready((buffer, result)) = Fiber::poll(Pin::new(read), cx.as_mut()) else {
            return Poll::Pending;
        };
        this.read = None;
        let Ok(count) = result else {
            return this.fail(LoadError::NotFound);
        };
        let expected = this
            .metadata
            .as_ref()
            .and_then(|metadata| usize::try_from(metadata.len()).ok())
            .expect("file metadata missing");
        if count != expected || buffer.len() != expected {
            return this.fail(LoadError::NotFound);
        }
        let body = Shared::from(buffer);
        let metadata = this.metadata.take().expect("file metadata missing");
        this.release();
        this.done = true;
        Poll::Ready(Ok(LoadedFile { body, metadata }))
    }
}
