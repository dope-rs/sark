use dope_quic::wire::stream::Id;
use sark_http::{
    self,
    field::metadata::{
        block,
        section::{
            rules,
            state::{RetainedHead, Selected},
        },
    },
};

use crate::{
    conn,
    conn::io::{
        ingress::Parts,
        write::feedback::{Feedback, FeedbackMut},
    },
    qpack::{self, decoder, identity::Identity},
};

pub(super) enum Decoded<P: rules::Policy> {
    Ready {
        head: RetainedHead<P::Block, Selected<P>, Option<block::Plan>>,
        response: Option<block::Response>,
    },
    Blocked,
    FeedbackBlocked,
    RejectPush,
}

pub(super) fn decode<P: rules::Policy, A: rules::Push, S: crate::Source + ?Sized>(
    parts: &mut Parts<'_, '_, P, A>,
    id: Id,
    block: &S,
    kind: rules::Kind,
    identity: Option<&mut Identity>,
    required: &mut Option<qpack::Reference>,
) -> Result<Decoded<P>, conn::Error> {
    let prepared = parts
        .qpack
        .qpack_decoder
        .prepare(block, required)
        .map_err(conn::Error::Qpack)?;
    let limit = Feedback::new(&parts.qpack.feedback, parts.session, parts.queues).limit();
    let feedback = &mut parts.qpack.feedback;
    let budget = &mut parts.queues.write_budget;
    let section = move |required: qpack::Reference| {
        let (feedback, budget) = (feedback, budget);
        FeedbackMut::new(feedback, budget)
            .try_section(required, id.get(), limit)
            .map(Some)
    };
    let feedback = match &prepared {
        decoder::Prepared::ReadyContiguous(plan) => match plan.requirement() {
            qpack::Requirement::Static => Ok(None),
            qpack::Requirement::Dynamic(required) => section(required),
        },
        decoder::Prepared::ReadySegmented(plan) => match plan.requirement() {
            qpack::Requirement::Static => Ok(None),
            qpack::Requirement::Dynamic(required) => section(required),
        },
        decoder::Prepared::Blocked(_) => Ok(None),
    };
    let feedback = match feedback {
        Ok(feedback) => feedback,
        Err(conn::Error::Feedback(_)) => return Ok(Decoded::FeedbackBlocked),
        Err(error) => return Err(error),
    };
    let outcome = parts
        .qpack
        .qpack_decoder
        .decode_head::<P, A, S>(
            prepared,
            kind,
            parts.settings.local_settings.connect(),
            identity,
        )
        .map_err(conn::Error::Qpack)?;
    match outcome {
        decoder::Message::Ready {
            head,
            validation: valid,
            response,
        } => {
            if let Some(feedback) = feedback {
                feedback.commit();
            }
            match valid {
                block::Validation::Valid => Ok(Decoded::Ready { head, response }),
                block::Validation::Malformed => Err(conn::Error::Malformed {
                    stream_id: id.get(),
                    kind,
                }),
                block::Validation::RejectPush => Ok(Decoded::RejectPush),
            }
        }
        decoder::Message::Blocked(required) => {
            parts
                .qpack
                .blocked
                .block(id, required)
                .map_err(conn::Error::Qpack)?;
            Ok(Decoded::Blocked)
        }
    }
}
