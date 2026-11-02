use o3::buffer::pool;
use sark_http::field::metadata::{
    block, lower,
    section::{rules, state, storage},
};
use state::decode::{Final, Session};

use crate::{
    conn,
    conn::ingress::{Decode, header},
    hpack::{
        decode,
        decode::{limit, phase::Phase},
    },
};

mod hpack;
pub(in crate::conn::ingress) mod output;

pub(in crate::conn::ingress) struct Header<P: rules::Policy> {
    session: Session<P>,
}

impl<P: rules::Policy> Header<P> {
    pub(in crate::conn::ingress) fn try_new(
        lease: pool::Span,
        kind: rules::Kind,
        connect: block::Connect,
        scratch: storage::Scratch,
    ) -> Result<Self, (storage::Scratch, lower::Error)> {
        Session::try_new(lease, kind, connect, scratch).map(|session| Self { session })
    }

    pub(in crate::conn::ingress) fn hpack<A: rules::Push>(self) -> hpack::Hpack<P, A> {
        hpack::Hpack::new(self.session)
    }

    pub(in crate::conn::ingress) fn finish_block(
        self,
        block: Phase<Decode>,
        budget: limit::Budget,
    ) -> Result<(decode::Decoder, storage::Scratch, output::Decoded<P>), conn::Error> {
        let decoder = match block.finish() {
            Ok(done) => done,
            Err(cause) => {
                drop(self.recycle());
                return Err(conn::Error::Hpack(cause));
            }
        };
        let (scratch, decoded) = self.finish(budget.admission)?;
        Ok((decoder, scratch, decoded))
    }

    pub(in crate::conn::ingress) fn finish(
        self,
        admit: limit::Admission,
    ) -> Result<(storage::Scratch, output::Decoded<P>), conn::Error> {
        let (scratch, result) = self.session.finish();
        let head = result.map_err(conn::Error::Header)?;
        let check = match &head {
            Final::Rejected(rejected) => {
                let (sealed, finished) = rejected.validation();
                header::Validation::new(admit, sealed, finished)
            }
            Final::Request(_) | Final::Response(_) | Final::Other(_) => {
                header::Validation::new(admit, block::Validation::Valid, block::Validation::Valid)
            }
        };
        Ok((
            scratch,
            output::Decoded {
                head,
                validation: check,
            },
        ))
    }

    pub(in crate::conn::ingress) fn recycle(self) -> storage::Scratch {
        self.session.recycle()
    }
}
