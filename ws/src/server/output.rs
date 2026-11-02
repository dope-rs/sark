use std::convert::Infallible;

use dope::{manifold::network::listener::conn::write, net::link::egress};
use o3::buffer::storage;

use crate::{
    frame,
    server::{ConnState, Error, Phase, Response, app::Sink},
};

pub(super) struct Output<'a> {
    pub(super) response: Response<'a>,
    pub(super) body: Option<storage::Shared>,
    capacity: usize,
}

impl<'a> Output<'a> {
    pub(super) fn new(buffer: &'a mut [u8]) -> Self {
        Self {
            capacity: buffer.len().saturating_sub(frame::MAX_SERVER_CONTROL_WIRE),
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
        if required <= self.capacity || self.response.written != 0 {
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

    pub(super) fn finish<H: Sink>(
        self,
        state: &mut ConnState,
        user: &H,
    ) -> write::Draft<Infallible, ()> {
        match self.try_finish(state) {
            Ok(draft) => draft,
            Err(error) => {
                user.failed(error);
                state.phase = Phase::Closed;
                write::Draft::new(0, None, ()).close_after()
            }
        }
    }

    fn try_finish(self, state: &mut ConnState) -> Result<write::Draft<Infallible, ()>, Error> {
        if state.eof
            && !matches!(state.phase, Phase::Rejected(_))
            && self.body.is_none()
            && !state
                .inbound
                .required()
                .is_some_and(|need| need <= state.acc.len())
        {
            state.phase = Phase::Closed;
        }
        let draft = write::Draft::new(self.response.written, None, ());
        if let Some(owner) = self.body {
            if state.pending.is_some() {
                return Err(Error::Pending);
            }
            state.pending = Some(egress::NonEmpty::try_new(owner).map_err(Error::Egress)?);
            Ok(draft)
        } else if state.phase == Phase::Closed {
            Ok(draft.close_after())
        } else {
            Ok(draft)
        }
    }
}
