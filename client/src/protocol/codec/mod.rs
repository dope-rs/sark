use core::{mem, num};

use codec::{
    staging::{Buffered, Limit},
    storage::{Edit, Owner},
};
use dope::{
    core::io::transfer,
    manifold::client::session::{codec, response::Outcome},
};
use o3::buffer::{
    self, hold,
    resident::{self, staging},
    write::consume::scope,
};
use sark_http::{
    h1::{
        framing,
        head::response::{self, Coding},
        switch,
    },
    payload::response::Method,
};

use crate::protocol::{
    Error,
    response::{Data, Event, Fields, Head},
};

mod chunks;
mod limits;
pub mod output;
mod pending;
mod prepared;
mod retain;
mod state;
mod storage;

pub use limits::Limits;
use output::{Completion, Output, Parsed, Pending, Preview};
pub use state::State;
use state::{Content, Framing};
pub use storage::Storage;
use storage::{Phase, Rejection};

#[derive(Clone, Copy)]
pub struct Codec {
    limits: Limits,
}

impl Default for Codec {
    fn default() -> Self {
        Self::new(Limits::DEFAULT)
    }
}

impl Codec {
    pub const fn new(limits: Limits) -> Self {
        Self { limits }
    }

    pub const fn limits(&self) -> Limits {
        self.limits
    }

    pub const fn output_bound(&self) -> output::Bound {
        output::Bound::new(self.limits.body_bytes())
    }

    pub fn reject<'d>(state: &mut State<'d>, error: Error) -> Output<'d> {
        state.framing = Framing::Failed;
        Output::error(error)
    }

    /// Binds a new response to its original request before inspecting any header.
    /// Pending heads retain that admission across input growth and retries.
    pub fn bind<S: buffer::raw::Source>(
        &self,
        storage: &mut Storage<S>,
        state: &State<'_>,
        request: impl FnOnce() -> Option<Method>,
    ) {
        if matches!(storage.phase, Phase::Body) && matches!(state.framing, Framing::Head) {
            storage.phase = Phase::Unbound;
        }
        if matches!(storage.phase, Phase::Unbound)
            && !storage.source.as_ref().is_empty()
            && let Some(request) = request()
        {
            storage.phase = Phase::Head(response::raw::State::new(request, self.limits.head()));
        }
    }

    /// Framing admits the representation; metadata-only coding never starts a decoder.
    pub fn admit<'d>(&self, state: &mut State<'d>, parsed: Parsed<'d>) -> Output<'d> {
        use sark_http::payload::response::status;

        let pending = match parsed {
            Parsed::Ready(output) => return output,
            Parsed::Pending(pending) => pending,
        };
        let Pending { parts, fields } = pending;
        let status = parts.status();
        if status == status::Code::SWITCHING_PROTOCOLS {
            return Self::reject(state, Error::Transition(switch::Error::UnexpectedSwitch));
        }
        let admitted = match parts.admit() {
            Ok(admitted) => admitted,
            Err(error) => return Self::reject(state, Error::Framing(error)),
        };
        if status.is_info() {
            if state.informational == output::INFORMATIONAL_LIMIT {
                return Self::reject(state, Error::InformationalLimit);
            }
            state.informational += 1;
            return Output::event(
                Event::Informational(Head::new(status, admitted.coding(), fields)),
                Completion::Pending,
            );
        }
        state.informational = 0;
        let coding = match admitted.body() {
            framing::ResponseBody::None | framing::ResponseBody::Empty(_) => Coding::None,
            _ => admitted.coding(),
        };
        let end = match admitted.body() {
            framing::ResponseBody::None | framing::ResponseBody::Fixed(0) => Completion::Complete,
            framing::ResponseBody::Fixed(len) if len > self.limits.body_bytes() => {
                return Self::reject(state, Error::BodyLimit);
            }
            framing::ResponseBody::Fixed(len) => {
                state.framing = Framing::Sized {
                    remaining: num::NonZero::<usize>::MIN.saturating_add(len - 1),
                };
                Completion::Pending
            }
            framing::ResponseBody::Chunked => {
                state.framing = Framing::Chunked(chunks::Chunks::new(self.limits.body_bytes()));
                Completion::Pending
            }
            framing::ResponseBody::UntilEof => {
                state.framing = Framing::UntilEof {
                    received: 0,
                    content: Content::Allowed,
                };
                Completion::Pending
            }
            framing::ResponseBody::Empty(framing::Empty::Fixed) => Completion::Complete,
            framing::ResponseBody::Empty(framing::Empty::Chunked) => {
                state.framing = Framing::Chunked(chunks::Chunks::new(0));
                Completion::Pending
            }
            framing::ResponseBody::Empty(framing::Empty::UntilEof) => {
                state.framing = Framing::UntilEof {
                    received: 0,
                    content: Content::Forbidden,
                };
                Completion::Pending
            }
        };
        Output::response(
            Event::Head(Head::new(status, coding, fields)),
            end,
            admitted.control(),
        )
    }

    fn more<'id, R: buffer::Cursor, T>(
        input: &codec::Input<'id, '_, '_, R>,
        consumed: Option<scope::End<'id, { transfer::MAX_BYTES as u32 }>>,
    ) -> codec::Parse<'id, T> {
        match input.need_more(consumed) {
            Some(need) => codec::Parse::More(need),
            None => codec::Parse::CapacityExhausted,
        }
    }

    fn data<'id, 'd, R, T: Outcome, F>(
        state: &mut State<'d>,
        input: &codec::Input<'id, '_, 'd, R>,
        take: num::NonZero<usize>,
        commit: impl FnOnce(&mut Framing<'d>),
        end: Completion,
        admit: impl FnOnce(Result<Preview<'_>, &Error>) -> codec::Admission<F>,
    ) -> Result<codec::Parse<'id, T>, T>
    where
        R: buffer::Cursor + hold::raw::Retain<'d>,
        F: FnOnce(Result<Parsed<'d>, Error>, &mut State<'d>) -> T::Accepted,
    {
        let (data, consumed) = match input.try_prefix(0, take) {
            Ok(plan) => plan,
            Err(error) => return Err(output::rejected(Error::Input(error), state, admit)),
        };
        let emit = match admit(Ok(Preview::body(data.as_slice(), end))) {
            codec::Admission::RetryInput => return Err(T::retry_input()),
            codec::Admission::Ready(emit) => emit,
        };
        let data = match data.bind(retain::RESPONSE).try_into() {
            Ok(data) => data,
            Err(error) => return Err(T::accept(emit(Err(Error::Retention(error)), state))),
        };
        commit(&mut state.framing);
        let parsed = Output::event(Event::Data(Data::from_owner(data)), end).into();
        Ok(codec::Parse::Item {
            head: T::accept(emit(Ok(parsed), state)),
            consumed,
        })
    }

    fn parse_sized<'id, 'd, R, T: Outcome, F>(
        &self,
        state: &mut State<'d>,
        input: &codec::Input<'id, '_, 'd, R>,
        left: num::NonZero<usize>,
        admit: impl FnOnce(Result<Preview<'_>, &Error>) -> codec::Admission<F>,
    ) -> Result<codec::Parse<'id, T>, T>
    where
        R: buffer::Cursor + hold::raw::Retain<'d>,
        F: FnOnce(Result<Parsed<'d>, Error>, &mut State<'d>) -> T::Accepted,
    {
        state.framing = Framing::Sized { remaining: left };
        let Some(len) = num::NonZero::new(input.len()) else {
            return Ok(Self::more(input, None));
        };
        if len.get() < output::DATA_LIMIT && len < left {
            return Ok(Self::more(input, None));
        }
        let take = len
            .min(left)
            .min(num::NonZero::<usize>::MIN.saturating_add(output::DATA_LIMIT - 1));
        let rest = num::NonZero::new(left.get() - take.get());
        let end = if rest.is_none() {
            Completion::Complete
        } else {
            Completion::Pending
        };
        Self::data(
            state,
            input,
            take,
            move |framing| {
                *framing = match rest {
                    None => Framing::Head,
                    Some(rest) => Framing::Sized { remaining: rest },
                };
            },
            end,
            admit,
        )
    }

    fn parse_eof<'id, 'd, R, T: Outcome, F>(
        &self,
        state: &mut State<'d>,
        input: &codec::Input<'id, '_, 'd, R>,
        received: usize,
        content: Content,
        admit: impl FnOnce(Result<Preview<'_>, &Error>) -> codec::Admission<F>,
    ) -> Result<codec::Parse<'id, T>, T>
    where
        R: buffer::Cursor + hold::raw::Retain<'d>,
        F: FnOnce(Result<Parsed<'d>, Error>, &mut State<'d>) -> T::Accepted,
    {
        state.framing = Framing::UntilEof { received, content };
        let len = input.len();
        if let Content::Forbidden = content
            && len != 0
        {
            return Err(output::rejected(Error::ForbiddenContent, state, admit));
        }
        if received
            .checked_add(len)
            .is_none_or(|total| total > self.limits.body_bytes())
        {
            return Err(output::rejected(Error::BodyLimit, state, admit));
        }
        if len < output::DATA_LIMIT {
            return Ok(Self::more(input, None));
        }
        let take = num::NonZero::<usize>::MIN.saturating_add(output::DATA_LIMIT - 1);
        let received = received + take.get();
        Self::data(
            state,
            input,
            take,
            move |framing| {
                *framing = Framing::UntilEof { received, content };
            },
            Completion::Pending,
            admit,
        )
    }

    fn progress<'id, 'input, 'd, R, T: Outcome, F>(
        &self,
        state: &mut State<'d>,
        input: &codec::Input<'id, 'input, 'd, R>,
        admit: impl FnOnce(Result<Preview<'_>, &Error>) -> codec::Admission<F>,
    ) -> Result<codec::Parse<'id, T>, T>
    where
        R: buffer::Cursor + hold::raw::Retain<'d>,
        F: FnOnce(Result<Parsed<'d>, Error>, &mut State<'d>) -> T::Accepted,
    {
        match mem::replace(&mut state.framing, Framing::Head) {
            Framing::Chunked(chunks) => prepared::Stage::parse(chunks, state, input, admit),
            Framing::Head => Ok(Self::more(input, None)),
            Framing::Sized { remaining: left } => self.parse_sized(state, input, left, admit),
            Framing::UntilEof { received, content } => {
                self.parse_eof(state, input, received, content, admit)
            }
            Framing::Failed => unreachable!("framing"),
        }
    }
}

impl codec::Codec for Codec {
    type Head<'input, 'd, R: buffer::Cursor>
        = Parsed<'d>
    where
        R: 'input,
        'd: 'input;
    type Error = Error;
    type Staging = Buffered;

    fn staging(&self) -> Result<staging::Plan, staging::Error> {
        let limit = self.limits.head_bytes().max(output::DATA_LIMIT);
        staging::Plan::new(output::DATA_LIMIT, limit)
    }
}

impl codec::Stateful for Codec {
    type State<'d> = State<'d>;
    type Storage<S> = Storage<S>;
    type Outcome<'d> = Parsed<'d>;
    type Preview<'a> = Preview<'a>;

    fn state<'d>(&self, budget: resident::Budget<'d>) -> State<'d> {
        State::new(budget)
    }

    fn storage<'d, S>(&self, source: S, state: &State<'d>) -> Storage<S> {
        Storage {
            source,
            phase: Phase::new(state),
        }
    }

    fn restart<'a, 'd, S>(&self, storage: &'a mut Storage<S>, state: &State<'d>) -> Edit<'a, S> {
        storage.phase = Phase::new(state);
        Self::edit(storage)
    }

    fn source<S: buffer::raw::Source>(storage: &Storage<S>) -> &S {
        storage.source()
    }

    fn edit<S>(storage: &mut Storage<S>) -> Edit<'_, S> {
        Edit::new(&mut storage.source)
    }

    fn decode<'d, S, T: Outcome, F>(
        &self,
        storage: &mut Storage<S>,
        state: &mut State<'d>,
        budget: &resident::Budget<'d>,
        limit: Limit,
        admit: impl FnOnce(Result<Preview<'_>, &Error>) -> codec::Admission<F>,
    ) -> codec::Step<T>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        F: FnOnce(Result<Parsed<'d>, Error>, &mut State<'d>) -> T::Accepted,
    {
        if matches!(state.framing, Framing::Failed) {
            return codec::Input::wait(&storage.source, budget, limit, None);
        }
        let parts = match mem::replace(&mut storage.phase, Phase::Body) {
            Phase::Unbound => {
                storage.phase = Phase::Unbound;
                if storage.source.as_ref().is_empty() {
                    return codec::Input::wait(&storage.source, budget, limit, None);
                }
                return codec::Step::rejected(storage.rejected(
                    Rejection::Unsolicited,
                    state,
                    admit,
                ));
            }
            Phase::Head(head) => {
                // SAFETY: this codec exclusively owns the stable source. Head
                // progress permits only append, growth, or exact-prefix transfer;
                // consumption replaces the phase before another scan.
                match unsafe { head.scan_from(storage.source.as_ref()) } {
                    Ok((_, Some(parts))) => parts,
                    Ok((head, None)) => {
                        storage.phase = Phase::Head(head);
                        return codec::Input::wait(&storage.source, budget, limit, None);
                    }
                    Err(error) => {
                        return codec::Step::rejected(storage.rejected(
                            Rejection::Head(error),
                            state,
                            admit,
                        ));
                    }
                }
            }
            Phase::Ready(parts) => parts,
            Phase::Rejected(error) => {
                return codec::Step::rejected(storage.rejected(error, state, admit));
            }
            Phase::Body => {
                let step = codec::Input::commit(&mut storage.source, budget, limit, |input| {
                    self.progress(state, &input, admit)
                });
                storage.phase = Phase::new(state);
                return step;
            }
        };
        let step = codec::Input::commit(&mut storage.source, budget, limit, |input| {
            let plan = (|| {
                let count =
                    num::NonZero::<usize>::MIN.saturating_add(parts.wire_len().get() as usize - 1);
                let consumed = input.try_consumed(count).map_err(Error::Input)?;
                let span = parts.fields();
                let fields = input
                    .try_plan(span.start()..span.end())
                    .map_err(Error::Range)?;
                Ok((fields, consumed))
            })();
            let (fields, consumed) = match plan {
                Ok(plan) => plan,
                Err(error) => return Err(output::rejected(error, state, admit)),
            };
            let emit = match admit(Ok(Preview::Head {
                parts,
                fields: fields.as_slice(),
            })) {
                codec::Admission::RetryInput => return Err(T::retry_input()),
                codec::Admission::Ready(emit) => emit,
            };
            let owner = match fields.bind(retain::RESPONSE).try_into() {
                Ok(owner) => owner,
                Err(error) => return Err(T::accept(emit(Err(Error::Retention(error)), state))),
            };
            let parsed = Parsed::pending(parts, Fields::admit(owner));
            Ok(codec::Parse::Item {
                head: T::accept(emit(Ok(parsed), state)),
                consumed,
            })
        });
        storage.phase = if step.has_consumed() {
            Phase::new(state)
        } else {
            Phase::Ready(parts)
        };
        step
    }

    fn finish<'slot, 'id, 'd, S, T: Outcome, F>(
        &self,
        mut owner: Owner<'slot, 'id, Storage<S>, State<'d>>,
        budget: &resident::Budget<'d>,
        limit: Limit,
        admit: impl FnOnce(Result<Preview<'_>, &Error>) -> codec::Admission<F>,
    ) -> (
        Owner<'slot, 'id, Storage<S>, State<'d>>,
        Result<codec::Finish<T>, T>,
    )
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        F: FnOnce(Result<Parsed<'d>, Error>, &mut State<'d>) -> T::Accepted,
    {
        let (storage, state) = owner.parts();
        let result = if let Phase::Rejected(error) = &storage.phase {
            let error = *error;
            Err(storage.rejected(error, state, admit))
        } else {
            codec::Input::with(&storage.source, budget, limit, |input| {
                state.finish(input, self.limits.body_bytes(), admit)
            })
        };
        (owner, result)
    }
}
