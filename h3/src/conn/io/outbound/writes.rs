use o3::{buffer::hold, mem::Rejected};

use crate::{
    conn,
    conn::io::{
        outbound::{Pending, batch, section},
        write::{Entry, Permit, Single},
    },
    qpack::encoder,
    stream,
};

pub(super) fn commit<'q, 'd, S, B>(
    batch: batch::Batch<'q, 'd, S, B>,
    qpack: Option<(stream::Target, Entry<'d>)>,
) -> Result<hold::Charge, conn::Error> {
    let batch::Batch {
        pending:
            Pending {
                fields,
                queues,
                outgoing,
                ..
            },
        trailing,
    } = batch;
    let section::Section {
        target,
        entry: fields,
        qpack: commit,
    } = fields;
    let retained = match (qpack, trailing) {
        (None, None) => match Single::new(queues, target, fields) {
            Ok(permit) => {
                commit.clear();
                permit.commit::<hold::evidence::On>()
            }
            Err(rejected) => {
                let error = rejected.discard();
                commit.discard();
                return Err(error);
            }
        },
        (Some(qpack), None) => publish(Permit::new(queues, [qpack, (target, fields)]), commit)?,
        (None, Some(trailing)) => publish(
            Permit::new(queues, [(target, fields), (target, trailing)]),
            commit,
        )?,
        (Some(qpack), Some(trailing)) => publish(
            Permit::new(queues, [qpack, (target, fields), (target, trailing)]),
            commit,
        )?,
    };
    if let Some(outgoing) = outgoing {
        outgoing.commit();
    }
    Ok(retained)
}

fn publish<'a, 'd, const N: usize>(
    permit: Result<Permit<'a, 'd, N>, Rejected<[(stream::Target, Entry<'d>); N], conn::Error>>,
    qpack: encoder::Commit<'a>,
) -> Result<hold::Charge, conn::Error> {
    let result = match permit {
        Ok(permit) => permit.commit::<hold::evidence::On, _>(qpack, encoder::Commit::clear),
        Err(rejected) => Err((rejected, qpack)),
    };
    result.map_err(|(rejected, qpack)| {
        let error = rejected.discard();
        qpack.discard();
        error
    })
}
