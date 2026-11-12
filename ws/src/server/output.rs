use dope::net::link::egress;
use o3::buffer::{
    storage::{self, Shared},
    write::Slice,
};

use crate::server::{ConnState, Error, Phase, Response, app::Sink};

pub(super) struct Output<'a> {
    pub(super) response: Response<'a>,
    pub(super) body: Option<storage::Shared>,
}

impl<'a> Output<'a> {
    pub(super) fn new(buffer: Slice<'a, &'a mut usize>) -> Self {
        Self {
            response: Response::new(buffer),
            body: None,
        }
    }

    pub(super) fn try_message(
        &mut self,
        required: usize,
        commit: impl FnOnce(&mut Response<'_>) -> Result<Phase, Error>,
    ) -> Result<Option<Phase>, Error> {
        if required <= self.response.user {
            let room = self.response.user;
            let before = self.response.written;
            self.response.user = required;
            let result = commit(&mut self.response);
            self.response.user = room - (self.response.written - before);
            return result.map(Some);
        }
        if self.response.written != 0 {
            return Ok(None);
        }
        let mut owner = storage::Owned::try_with(required).map_err(Error::Allocate)?;
        let mut response = Response::owned(&mut owner);
        let result = commit(&mut response);
        if !owner.is_empty() {
            self.body = Some(owner.freeze());
        }
        result.map(Some)
    }

    pub(super) fn finish<H: Sink>(self, state: &mut ConnState, user: &H) -> bool {
        match self.try_finish(state) {
            Ok(()) => true,
            Err(error) => {
                user.failed(error);
                state.phase = Phase::Closed;
                false
            }
        }
    }

    /// Completes the EOF transition before separating the output owners.
    pub(super) fn into_parts<P>(self, state: &mut ConnState<P>) -> Option<Shared> {
        if state.eof
            && !matches!(state.phase, Phase::Rejected(_))
            && (self.body.is_none() || (state.phase == Phase::Active && state.acc.is_empty()))
            && !state
                .inbound
                .required()
                .is_some_and(|need| need <= state.acc.len())
        {
            state.phase = Phase::Closed;
        }
        self.body
    }

    fn try_finish(self, state: &mut ConnState) -> Result<(), Error> {
        let body = self.into_parts(state);
        if let Some(owner) = body {
            state.pending = Some(egress::NonEmpty::try_new(owner).map_err(Error::Egress)?);
        }
        Ok(())
    }
}
