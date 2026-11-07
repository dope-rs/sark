use core::mem;

use sark_core::{
    body::{
        admit::{Length, Retention},
        credit,
    },
    route,
};
use sark_http::{
    h1::{framing::RequestBody, head::request::Coding},
    request::Layout,
};

use crate::{
    session,
    wire::{
        head, parsing,
        parsing::{framing, framing::Parsed},
        response,
    },
};

type Discarded<'req, R> = Result<
    (
        Parsed<&'req [u8], R>,
        usize,
        Length,
        session::Consumption,
        head::Connection,
        Coding,
        response::Context,
    ),
    parsing::Request<<R as route::Spec>::Request>,
>;

pub(crate) struct Discard<'a, 'req, R: route::Spec> {
    fields: framing::Fields<'a, 'req, R>,
    body_total: usize,
    content_length: Length,
    consumption: session::Consumption,
    connection: head::Connection,
    response: response::Context,
    coding: Coding,
}

impl<'a, 'req, R: route::Spec> Discard<'a, 'req, R> {
    pub(crate) fn into_parts(self) -> Discarded<'req, R> {
        let request = self.fields.into_head()?;
        Ok((
            request,
            self.body_total,
            self.content_length,
            self.consumption,
            self.connection,
            self.coding,
            self.response,
        ))
    }

    pub(crate) fn drive<'d, RouteState, Ready, Heads>(
        base: framing::Head<'a, 'req, R, Coding>,
        ingress: &mut session::Ingress<'d, RouteState, Ready, Heads>,
        proof: &mut impl credit::Evidence,
    ) -> Result<framing::Discarded<'a, 'req, R>, parsing::Request<R::Request>> {
        const {
            assert!(
                mem::size_of::<Self>()
                    <= mem::size_of::<(
                        parsing::raw::Bound<&'a parsing::Head<'req>, R>,
                        <R::Request as route::Schema>::RawHeaders,
                        usize,
                        usize,
                        Length,
                        session::Consumption,
                        head::Connection,
                        Coding,
                        response::Context,
                        Layout,
                    )>()
            );
            assert!(mem::size_of::<Option<Self>>() == mem::size_of::<Self>());
        }
        let ctx = base.fields.source.head;
        base.check_body()?;
        credit::Evidence::head(proof);
        let (total, length, used) = match base.body_framing {
            RequestBody::Empty => (
                0,
                Length::Known(0),
                session::Consumption::Discard(session::Discarded::new(
                    base.fields.len.get() as usize,
                    0,
                )),
            ),
            RequestBody::Fixed(length) => (
                length,
                Length::Known(length),
                session::Consumption::Discard(session::Discarded::new(
                    base.fields.len.get() as usize,
                    length,
                )),
            ),
            RequestBody::Chunked => {
                match ingress.decode(
                    ctx.input(),
                    base.fields.len.get() as usize,
                    R::MAX_BODY,
                    Retention::Discarded,
                ) {
                    Ok(None) => {
                        let consumed = ctx.input().as_slice().len();
                        return Ok(framing::Discarded::Pending(framing::Pending::new(
                            base,
                            consumed,
                            Length::Unknown,
                        )?));
                    }
                    Ok(Some(done)) => (
                        done.body.len(),
                        Length::Unknown,
                        session::Consumption::Buffered(session::Buffered::new(done.consumed)),
                    ),
                    Err(error) => return Err(parsing::Request::chunked(error)),
                }
            }
        };
        Ok(framing::Discarded::Complete(Self {
            fields: base.fields,
            body_total: total,
            content_length: length,
            consumption: used,
            connection: base.connection,
            response: base.response,
            coding: base.coding,
        }))
    }
}
