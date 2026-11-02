use sark_core::{
    life::session,
    manifold::stream::{Boundary, Section},
};
use sark_http::{
    field::metadata::{
        block,
        section::{
            self, rules,
            state::{Selected, decode::Final},
        },
    },
    payload::response::Method,
};

use crate::{
    conn::{
        event,
        ingress::{events, header, input},
    },
    frame::numbers,
    stream,
};

pub(crate) type Reset<'a, P, C, E> =
    events::Reset<'a, Selected<P>, <P as section::Policy>::Block, C, E>;

pub(crate) struct Prepared<'a, P: rules::Policy, C: session::Storage, E: event::Evidence> {
    frame: input::Frame<'a, C>,
    event: events::Vacant<'a, Selected<P>, P::Block, C, E>,
    head: Final<P>,
    validation: header::Validation,
}

impl<'a, P: rules::Policy, C: session::Storage, E: event::Evidence> Prepared<'a, P, C, E> {
    pub(in crate::conn::ingress) fn new(
        frame: input::Frame<'a, C>,
        event: events::Vacant<'a, Selected<P>, P::Block, C, E>,
        head: Final<P>,
        check: header::Validation,
    ) -> Self {
        Self {
            frame,
            event,
            head,
            validation: check,
        }
    }

    pub(crate) const fn validation(&self) -> header::Validation {
        self.validation
    }

    pub(crate) fn response(&self) -> Option<block::Response> {
        match &self.head {
            Final::Response(head) => Some(*head.control()),
            Final::Rejected(rejected) => rejected.response(),
            Final::Request(_) | Final::Other(_) => None,
        }
    }

    pub(crate) fn method(&self) -> Option<Method> {
        self.head.framing().and_then(block::Framing::method)
    }

    pub(crate) fn content(
        &self,
        method: Method,
    ) -> Result<(stream::Content, u64), numbers::ErrorCode> {
        let framing = self
            .head
            .framing()
            .ok_or(numbers::ErrorCode::ProtocolError)?;
        framing.content(method).map_err(numbers::ErrorCode::from)
    }

    pub(crate) fn commit(self, stream: stream::Key, boundary: Boundary, section: Section) {
        self.frame.commit();
        self.event.push_back(event::Event::Headers {
            stream_id: stream,
            head: self.head.into_parts().0,
            boundary,
            section,
        });
    }

    pub(crate) fn promise(self, stream: stream::Id, promised: stream::Key) {
        self.frame.commit();
        self.event.push_back(event::Event::PushPromise {
            stream_id: stream,
            promised_stream_id: promised,
            head: self.head.into_parts().0,
        });
    }

    pub(crate) fn reject(self) -> Reset<'a, P, C, E> {
        self.frame.commit();
        let check = match self.validation {
            header::Validation::Valid => None,
            header::Validation::Rejected {
                admit,
                sealed,
                finished,
            } => Some(event::Rejected {
                admit,
                sealed,
                finished,
            }),
        };
        events::Reset::new(self.event, check)
    }

    pub(crate) fn discard(self) {
        self.frame.commit();
    }
}
