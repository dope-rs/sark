use o3::{buffer::hold, mem::Rejected};

use crate::{
    conn,
    conn::io::{
        outbound::{batch, section},
        write::{Entry, Permit, Queues, Single},
    },
    qpack::encoder,
};

enum Writes<'a, 'd> {
    Fields(Single<'a, 'd, Entry<'d>>),
    Pair(Permit<'a, 'd, Entry<'d>, 2>),
    All(Permit<'a, 'd, Entry<'d>, 3>),
}

impl<'a, 'd> Writes<'a, 'd> {
    fn new<S, B>(
        qpack: Option<Entry<'d>>,
        fields: Entry<'d>,
        trailing: Option<Entry<'d>>,
        queues: &'a mut Queues<'d, S, B>,
    ) -> Result<Self, conn::Error> {
        match (qpack, trailing) {
            (None, None) => Single::new(queues, fields)
                .map(Self::Fields)
                .map_err(Rejected::discard),
            (Some(qpack), None) => Permit::new(queues, [qpack, fields])
                .map(Self::Pair)
                .map_err(Rejected::discard),
            (None, Some(trailing)) => Permit::new(queues, [fields, trailing])
                .map(Self::Pair)
                .map_err(Rejected::discard),
            (Some(qpack), Some(trailing)) => Permit::new(queues, [qpack, fields, trailing])
                .map(Self::All)
                .map_err(Rejected::discard),
        }
    }

    fn commit(
        self,
        qpack: encoder::Commit<'a>,
        retained: hold::Charge,
    ) -> Result<hold::Charge, conn::Error> {
        match self {
            Self::Fields(permit) => {
                qpack.clear();
                permit.commit();
            }
            Self::Pair(permit) => Self::publish(permit, qpack)?,
            Self::All(permit) => Self::publish(permit, qpack)?,
        }
        Ok(retained)
    }

    fn publish<const N: usize>(
        permit: Permit<'a, 'd, Entry<'d>, N>,
        qpack: encoder::Commit<'a>,
    ) -> Result<(), conn::Error> {
        permit
            .commit(qpack, encoder::Commit::clear)
            .map_err(|(rejected, qpack)| {
                let error = rejected.discard();
                qpack.discard();
                error
            })
    }
}

pub(super) fn commit<'q, 'd, S, B>(
    batch: batch::Batch<'q, 'd, S, B>,
    qpack: Option<Entry<'d>>,
    retained: hold::Charge,
) -> Result<hold::Charge, conn::Error> {
    let batch::Batch {
        fields,
        trailing,
        queues,
        outgoing,
        ..
    } = batch;
    let section::Section {
        entry: fields,
        qpack: commit,
    } = fields;
    let writes = match Writes::new(qpack, fields, trailing, queues) {
        Ok(writes) => writes,
        Err(error) => {
            commit.discard();
            return Err(error);
        }
    };
    let retained = writes.commit(commit, retained)?;
    if let Some(outgoing) = outgoing {
        outgoing.commit();
    }
    Ok(retained)
}
