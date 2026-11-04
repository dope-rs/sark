use std::ops::ControlFlow;

use o3::buffer::{
    resident,
    segments::{Count, Item},
    storage,
};
use sark_http::{h1::head::response::Coding, payload::response::status};

use crate::protocol::response;

pub mod assembly;
pub mod body;
pub mod decode;
pub mod detach;
mod plan;

use detach::{Error, Part, copies::Copies};
use plan::{Mode, Plan};

/// A filtered borrowed view over retained incoming response fields.
pub struct Headers<'a, S: AsRef<[u8]>> {
    fields: &'a response::Fields<S>,
    omitted: bool,
}

impl<'a, S: AsRef<[u8]>> Headers<'a, S> {
    pub fn get(&self, name: impl AsRef<str>) -> Option<&'a [u8]> {
        let name = name.as_ref();
        let bytes = name.as_bytes();
        if self.omitted
            && (bytes.eq_ignore_ascii_case(b"content-encoding")
                || bytes.eq_ignore_ascii_case(b"content-length"))
        {
            None
        } else {
            self.fields.get(name)
        }
    }
}

/// Incoming response retaining its metadata-pool borrow and driver-bound bytes.
///
pub struct Scoped<'a, 'd> {
    status: status::Code,
    coding: Coding,
    headers: response::Fields<resident::Shared<'d>>,
    trailers: Option<response::Fields<resident::Shared<'d>>>,
    body: body::Scoped<'a, 'd>,
    omitted: bool,
}

impl<'a, 'd> Scoped<'a, 'd> {
    pub const fn status(&self) -> status::Code {
        self.status
    }

    pub fn headers(&self) -> Headers<'_, resident::Shared<'d>> {
        Headers {
            fields: &self.headers,
            omitted: self.omitted,
        }
    }

    pub const fn trailers(&self) -> Option<&response::Fields<resident::Shared<'d>>> {
        self.trailers.as_ref()
    }

    pub const fn body(&self) -> &body::Scoped<'a, 'd> {
        &self.body
    }

    /// Transfers the body owner and releases the unused response fields.
    pub fn into_body(self) -> body::Scoped<'a, 'd> {
        self.body
    }

    fn omit_coding(&mut self) {
        self.coding = Coding::None;
        self.omitted = true;
    }

    /// Detaches every owner while preserving body segment boundaries.
    /// This allocates at most one metadata block and one shared byte block.
    /// Each original nonempty segment keeps its extent in the reserved pool.
    pub fn try_retain(mut self) -> Result<Retained, Error> {
        let plan = Plan::try_new(&mut self, Mode::Detach)?;
        let segments = plan.segments();
        let target =
            body::Vacant::try_new(segments).map_err(|cause| Error::Metadata { segments, cause })?;
        let copies = plan.allocate()?;
        self.detach(copies, |mut source, copies| {
            Ok(match target {
                body::Vacant::Empty => body::Retained::empty(),
                body::Vacant::One => {
                    let Some(owner) = source.pop() else {
                        return Err(Error::BodyCount {
                            expected: Count::ONE,
                            actual: Count::EMPTY,
                        });
                    };
                    let owner = copies.try_retain(owner, Part::Body)?;
                    let ControlFlow::Continue(owner) = Item::new(owner) else {
                        return Err(Error::BodyCount {
                            expected: Count::ONE,
                            actual: Count::EMPTY,
                        });
                    };
                    body::Retained::one(owner)
                }
                body::Vacant::Many(mut chain) => {
                    while let Some(owner) = source.pop() {
                        let owner = copies.try_retain(owner, Part::Body)?;
                        let segment = chain.count();
                        if let Err(rejected) = chain.try_push(owner) {
                            return Err(Error::MetadataCommit {
                                segment,
                                cause: rejected.discard(),
                            });
                        }
                    }
                    body::Retained::many(chain)
                }
            })
        })
    }

    /// Detaches the response and joins a fragmented body in one shared block.
    ///
    /// All required byte copies share at most one allocation.
    pub fn try_flatten(mut self) -> Result<Contiguous, Error> {
        let mode = if self.body.contiguous().is_some() {
            Mode::Detach
        } else {
            Mode::Copy
        };
        let copies = Plan::try_new(&mut self, mode)?.allocate()?;
        self.detach(copies, |mut source, copies| {
            if mode == Mode::Copy {
                copies.flatten(source)
            } else {
                match source.pop() {
                    Some(owner) => copies.try_retain(owner, Part::Body),
                    None => Ok(storage::Shared::new()),
                }
            }
        })
    }

    fn detach<B>(
        self,
        mut copies: Copies,
        build: impl FnOnce(body::Scoped<'a, 'd>, &mut Copies) -> Result<B, Error>,
    ) -> Result<Response<B>, Error> {
        let headers = self.headers.try_retain(&mut copies, Part::Headers)?;
        let body = build(self.body, &mut copies)?;
        let trailers = match self.trailers {
            Some(fields) => Some(fields.try_retain(&mut copies, Part::Trailers)?),
            None => None,
        };
        Ok(Response {
            status: self.status,
            headers,
            trailers,
            body,
            omitted: self.omitted,
        })
    }
}

pub type Retained = Response<body::Retained>;
pub type Contiguous = Response<storage::Shared>;

/// Detached response fields paired with their concrete body owner.
pub struct Response<B> {
    status: status::Code,
    headers: response::Fields<storage::Shared>,
    trailers: Option<response::Fields<storage::Shared>>,
    body: B,
    omitted: bool,
}

impl<B> Response<B> {
    pub const fn status(&self) -> status::Code {
        self.status
    }

    pub fn headers(&self) -> Headers<'_, storage::Shared> {
        Headers {
            fields: &self.headers,
            omitted: self.omitted,
        }
    }

    pub const fn trailers(&self) -> Option<&response::Fields<storage::Shared>> {
        self.trailers.as_ref()
    }

    pub const fn body(&self) -> &B {
        &self.body
    }
}
