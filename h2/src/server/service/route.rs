use std::{convert::Infallible, ops};

use o3::mem::Rejected;
use sark_core::{
    body,
    body::admit::Length,
    decode,
    decode::{dispatch, state::Uniforms},
    request::{BindError, Head},
    route,
};
use sark_http::{
    field::{
        metadata::{
            packed,
            section::{rules, state},
        },
        text::IntError,
    },
    owner::handoff,
    request::{self, body::Error},
};

use crate::server::service::{Handler, Prepared, Request, Selected};

/// A manual handler represented as the canonical protocol-neutral decoder.
pub struct Route<H> {
    handler: H,
}

#[derive(Default)]
struct Fields {
    path: Option<Path>,
    authority: Option<ops::Range<usize>>,
    host: Option<ops::Range<usize>>,
}

enum Path {
    Asterisk {
        range: ops::Range<usize>,
    },
    Origin {
        range: ops::Range<usize>,
        route_len: usize,
    },
}

const _: () = assert!(std::mem::size_of::<Path>() == 4 * std::mem::size_of::<usize>());

impl<H> Route<H> {
    pub const fn new(handler: H) -> Self {
        Self { handler }
    }

    pub fn into_inner(self) -> H {
        self.handler
    }
}

impl<H: Handler> decode::Decode for Route<H> {
    type Head = Selected;
    type Prepared = Prepared;
    type Routes = Uniforms<Prepared>;
    type Plan = rules::RetainRaw;
    type Cause = Infallible;
    type Error<'req> = Infallible;
    type Projected<'req, 'd>
        = decode::Call<Prepared, body::Scoped<'req, 'd, body::Segmented>>
    where
        'd: 'req;

    type Egress<'d> = H::Egress<'d>;

    fn reject<'req>(error: Self::Error<'req>) -> route::Reject {
        match error {}
    }

    fn try_full(
        &self,
        head: state::RetainedHead<packed::Decoded, state::Selected<Self::Plan>>,
    ) -> Result<Self::Head, decode::HeadError> {
        self.try_head(head)
    }

    fn try_head(
        &self,
        head: state::RetainedHead<packed::Decoded, state::Selected<Self::Plan>>,
    ) -> Result<Self::Head, decode::HeadError> {
        let fields = Fields::scan(head.fields());
        // SAFETY: fields was scanned from this exact immutable native head.
        unsafe { fields.bind(head) }
    }

    fn try_project<'d, B, T, F>(
        &self,
        prepared: Self::Prepared,
        body: body::Input<B>,
        project: F,
    ) -> Result<(T, B::Owner), Rejected<(Self::Prepared, body::Input<B>), Error>>
    where
        B: handoff::Project<'d>,
        F: for<'req> FnOnce(decode::Scoped<'req, 'd, Self, B::Proof>) -> T,
    {
        match <body::Segmented as body::Project>::try_input(body, prepared, |scope| {
            let (prepared, body, slot) = scope.into_source();
            project(decode::Scoped::new(decode::Call::new(prepared, body), slot))
        }) {
            Ok(output) => Ok(output),
            Err(failure) => {
                let ((input, prepared), error) = failure.into_parts();
                Err(Rejected::new((prepared, input), error))
            }
        }
    }

    fn try_dispatch<'req, 'd, E: decode::Encode<'req, 'd>>(
        &self,
        request: Self::Projected<'req, 'd>,
        encoder: E,
    ) -> Result<
        E::Prepared,
        (
            dispatch::Error<<E as decode::Encode<'req, 'd>>::Error, Infallible, Self::Error<'req>>,
            E::Target,
        ),
    > {
        use sark_http::payload::response::dispatch::Immediate;

        let (prepared, body) = request.into_parts();
        let head = prepared.into_head();
        head.scope(|head| {
            let encoder = decode::Encode::begin_head(encoder, head.owner().view());
            let request = Request::new(&head, body.into_inner());
            let prepared = Immediate::emit(self.handler.handle(request), encoder)
                .map_err(|(error, encoder)| (dispatch::Error::Encode(error), encoder))?;
            Ok(decode::Lineage::resolve_head(prepared, head))
        })
    }
}

impl Fields {
    /// # Safety
    /// These ranges must have been scanned from this exact immutable head.
    unsafe fn bind(
        self,
        head: state::RetainedHead<packed::Decoded, state::Selected<rules::RetainRaw>>,
    ) -> Result<Selected, decode::HeadError> {
        let length = head
            .framing()
            .and_then(|framing| framing.content_length())
            .map(usize::try_from)
            .transpose()
            .map_err(|_| decode::HeadError::ContentLength(IntError::Overflow))?;
        let body = Selected::plan(Length::from_option(length));
        let (path, path_len) = match self.path {
            Some(Path::Asterisk { range }) => (Some(range), None),
            Some(Path::Origin {
                range,
                route_len: len,
            }) => (Some(range), Some(len)),
            None => (None, None),
        };
        // SAFETY: scan obtained every range from this exact native RetainRaw
        // head's immutable field iteration. The original owner and control
        // remain together until this conversion consumes them.
        let bound = unsafe {
            request::raw::Bind::try_bind(head, path, path_len, self.authority, self.host)
        }
        .map_err(|error| decode::HeadError::Bind(BindError::Layout(error)))?;
        let head = Head::try_bound(bound).map_err(decode::HeadError::Bind)?;
        Ok(Selected::new(head, body))
    }

    fn scan(fields: &packed::Decoded) -> Self {
        let mut ranges = Self::default();
        for (field, range) in fields.ranges() {
            match field.name {
                b":path" => {
                    let path = if field.value == b"*" {
                        Path::Asterisk { range }
                    } else {
                        Path::Origin {
                            range,
                            route_len: field
                                .value
                                .iter()
                                .position(|byte| *byte == b'?')
                                .unwrap_or(field.value.len()),
                        }
                    };
                    ranges.path = Some(path);
                }
                b":authority" => ranges.authority = Some(range),
                b"host" => ranges.host = Some(range),
                _ => {}
            }
        }
        ranges
    }
}
