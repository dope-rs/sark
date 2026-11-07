use dope::manifold::network::listener::conn::write;
use o3::base::num::bounded;
use sark_core::life::session;

use crate::conn::{event, machine::ops::state, outputs::transfer};

#[must_use = "dropping a bound write keeps the H2 egress state unchanged"]
pub(crate) struct Bound<'a, R: crate::Role, C: session::Storage + 'a, const PREFIX: usize> {
    pub(in crate::conn::machine) state: &'a mut state::State<R>,
    pub(in crate::conn::machine) write: transfer::Prepared<'a, C, PREFIX>,
}

impl<'a, R, C, const PREFIX: usize> Bound<'a, R, C, PREFIX>
where
    R: crate::Role,
    C: session::Storage + 'a,
{
    pub(crate) fn is_drained(&self) -> bool {
        self.write.is_drained()
    }

    pub(crate) fn commit_with<T>(
        self,
        prefix: &mut write::Prefix<'_, '_>,
        evidence: impl FnOnce(crate::stream::Key, usize) -> T,
    ) -> Result<event::Write<T, C>, bounded::Error> {
        let Self { state, write: head } = self;
        head.commit_to(prefix, |write, tail| {
            let output = state.finish_write(write, |stream, bytes, _| evidence(stream, bytes));
            state.flush_resets(tail);
            output
        })
    }
}
