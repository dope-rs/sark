use sark_core::http::body_kind::ResponseKind;
use sark_core::http::compress::Gzip;
use sark_core::http::{CacheTemplate, Egress, Preparation, Prepared, Shape};

use super::conn_state::Outcome;
use super::response_cache::{Cache, Cached};
use crate::CANNED_500;
use crate::service::RouteSpec;

pub(super) struct ResponseEgress<'a> {
    write: &'a mut [u8],
    date: &'a [u8; 29],
}

impl<'a> ResponseEgress<'a> {
    pub(super) fn new(write: &'a mut [u8], date: &'a [u8; 29]) -> Self {
        Self { write, date }
    }

    pub(super) fn cached<R: RouteSpec>(&mut self, cache: &Cache<'_>) -> Option<Outcome> {
        if !R::STATIC_RESPONSE {
            return None;
        }
        Some(match cache.write(self.write, self.date)? {
            Cached::Fixed { written } => Outcome::Send {
                written,
                close_after: false,
            },
            Cached::Static { hdr_written, body } => Outcome::SendStatic {
                hdr_written,
                body,
                close_after: false,
            },
        })
    }

    pub(super) fn route<'req, R: RouteSpec>(
        self,
        response: R::Response<'req>,
        cache: Cache<'_>,
        gzip: &mut Gzip,
        accept_gzip: bool,
    ) -> Outcome {
        let mode = if R::STATIC_RESPONSE {
            Preparation::Cache
        } else if accept_gzip && matches!(R::RESPONSE_BODY_KIND, ResponseKind::Inline) {
            Preparation::Compress
        } else {
            Preparation::Plain
        };
        match response.prepare(mode, Some(gzip), self.write, self.date) {
            Prepared::Egress(egress) => Self::outcome(egress, false),
            Prepared::Cache(template) => self.cache(template, cache, R::EMIT_DATE, R::EMIT_SERVER),
        }
    }

    pub(super) fn plain<'req, S: Shape<'req>>(self, response: S, close_after: bool) -> Outcome {
        match response.prepare(Preparation::Plain, None, self.write, self.date) {
            Prepared::Egress(egress) => Self::outcome(egress, close_after),
            Prepared::Cache(_) => Outcome::Close(CANNED_500),
        }
    }

    pub(super) fn stream<'req, S: Shape<'req>>(
        self,
        response: S,
    ) -> Option<(usize, S::StreamInner)> {
        match response.prepare(Preparation::Plain, None, self.write, self.date) {
            Prepared::Egress(Egress::Stream { head, stream }) => Some((head, stream)),
            Prepared::Egress(
                Egress::Inline { .. }
                | Egress::Static { .. }
                | Egress::Shared { .. }
                | Egress::Pooled { .. }
                | Egress::Failed,
            )
            | Prepared::Cache(_) => None,
        }
    }

    fn outcome<S>(egress: Egress<S>, close_after: bool) -> Outcome {
        match egress {
            Egress::Inline { written } => Outcome::Send {
                written,
                close_after,
            },
            Egress::Static { head, body } => Outcome::SendStatic {
                hdr_written: head,
                body,
                close_after,
            },
            Egress::Shared { head, body } => Outcome::SendSplit {
                hdr_written: head,
                body,
                close_after,
            },
            Egress::Pooled { head, body } => Outcome::SendPooled {
                hdr_written: head,
                body,
                close_after,
            },
            Egress::Stream { .. } | Egress::Failed => Outcome::Close(CANNED_500),
        }
    }

    fn cache(
        self,
        mut template: CacheTemplate,
        cache: Cache<'_>,
        emit_date: bool,
        emit_server: bool,
    ) -> Outcome {
        template.configure_head(emit_date, emit_server);
        match template {
            CacheTemplate::Inline { bytes, date_offset } => {
                let written = sark_core::http::FixedResponse::write_preserialized(
                    self.write,
                    &bytes,
                    date_offset,
                    self.date,
                );
                cache.insert_fixed(bytes, date_offset);
                match written {
                    Some(written) => Outcome::Send {
                        written,
                        close_after: false,
                    },
                    None => Outcome::Close(CANNED_500),
                }
            }
            CacheTemplate::Static {
                head,
                date_offset,
                body,
            } => {
                let written = sark_core::http::FixedResponse::write_preserialized(
                    self.write,
                    &head,
                    date_offset,
                    self.date,
                );
                cache.insert_static(head, date_offset, body);
                match written {
                    Some(hdr_written) => Outcome::SendStatic {
                        hdr_written,
                        body,
                        close_after: false,
                    },
                    None => Outcome::Close(CANNED_500),
                }
            }
        }
    }
}
