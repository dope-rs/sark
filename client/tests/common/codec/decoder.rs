use std::num::NonZero;

use dope::manifold::client::session::{
    codec::{
        self, Stateful as _,
        staging::{Limit, Policy},
        storage::Slot,
    },
    response::Direct,
};
use o3::buffer::{self, hold, resident, resident::staging};
use sark_client::protocol::{
    self,
    codec::{Codec, State, Storage, output},
};
use sark_http::payload::response::Method;

use crate::common::codec::Source;

#[derive(Debug)]
pub enum Error {
    Protocol(protocol::Error),
    MissingConsumption,
    MissingCallback,
}

pub struct Decoder<'a> {
    codec: &'a Codec,
    limit: Limit,
}

impl<'a> Decoder<'a> {
    pub fn try_new(codec: &'a Codec) -> Result<Self, staging::Error> {
        let spec = codec::Codec::staging(codec);
        let plan = <<Codec as codec::Codec>::Staging as Policy>::plan(spec)?;
        let limit = <<Codec as codec::Codec>::Staging as Policy>::limit(plan);
        Ok(Self { codec, limit })
    }

    pub const fn limit(&self) -> Limit {
        self.limit
    }

    pub fn storage<'r, R>(&self, bytes: &'r R, state: &State<'_>) -> Storage<Source<'r, R>> {
        self.codec.storage(Source::new(bytes), state)
    }

    pub fn try_parse<'d, R: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Source>(
        &self,
        state: &mut State<'d>,
        bytes: &R,
        budget: &resident::Budget<'d>,
    ) -> Result<Option<(output::Output<'d>, NonZero<usize>)>, Error> {
        self.try_item(state, bytes, budget, || Some(Method::Other))
    }

    pub fn try_item<'d, R: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Source>(
        &self,
        state: &mut State<'d>,
        bytes: &R,
        budget: &resident::Budget<'d>,
        method: impl FnOnce() -> Option<Method>,
    ) -> Result<Option<(output::Output<'d>, NonZero<usize>)>, Error> {
        let mut storage = self.storage(bytes, state);
        self.try_decode(state, &mut storage, budget, method)
    }

    pub fn try_stored<'d, R: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume>(
        &self,
        state: &mut State<'d>,
        storage: &mut Storage<R>,
        budget: &resident::Budget<'d>,
    ) -> Result<Option<(output::Output<'d>, NonZero<usize>)>, Error> {
        self.try_decode(state, storage, budget, || Some(Method::Other))
    }

    pub fn try_decode<'d, R: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume>(
        &self,
        state: &mut State<'d>,
        storage: &mut Storage<R>,
        budget: &resident::Budget<'d>,
        method: impl FnOnce() -> Option<Method>,
    ) -> Result<Option<(output::Output<'d>, NonZero<usize>)>, Error> {
        let before = storage.source().as_ref().len();
        let mut output = None;
        let target = &mut output;
        let step: codec::Step<Direct> =
            self.codec.decode(storage, state, budget, self.limit, |_| {
                codec::Admission::Ready(
                    move |parsed: Result<output::Parsed<'d>, protocol::Error>,
                          state: &mut State<'d>| {
                        *target = Some(
                            parsed
                                .map(|head| self.codec.admit(state, head, method))
                                .map_err(Error::Protocol),
                        );
                    },
                )
            });
        output
            .map(|output| {
                let output = output?;
                let used = before
                    .checked_sub(storage.source().as_ref().len())
                    .and_then(NonZero::new)
                    .filter(|_| step.has_consumed())
                    .ok_or(Error::MissingConsumption)?;
                Ok((output, used))
            })
            .transpose()
    }

    pub fn try_finish<'d, R: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume>(
        &self,
        state: State<'d>,
        storage: Storage<R>,
        budget: &resident::Budget<'d>,
    ) -> (
        (Storage<R>, State<'d>),
        Result<codec::Finish<output::Output<'d>>, Error>,
    ) {
        let mut output = None;
        let target = &mut output;
        let (owners, finished): (_, Result<codec::Finish<Direct>, Direct>) =
            Slot::new((storage, state)).run(|input| {
                self.codec.finish(input, budget, self.limit, |_| {
                    codec::Admission::Ready(
                        move |parsed: Result<output::Parsed<'d>, protocol::Error>,
                              state: &mut State<'d>| {
                            *target = Some(
                                parsed
                                    .map(|head| self.codec.admit(state, head, || None))
                                    .map_err(Error::Protocol),
                            );
                        },
                    )
                })
            });
        let result = match output {
            Some(output) => output.map(codec::Finish::Item),
            None => match finished {
                Ok(codec::Finish::Empty) => Ok(codec::Finish::Empty),
                Ok(codec::Finish::CapacityExhausted) => Ok(codec::Finish::CapacityExhausted),
                Ok(codec::Finish::Item(_)) | Err(_) => Err(Error::MissingCallback),
            },
        };
        (owners.into_parts(), result)
    }
}
