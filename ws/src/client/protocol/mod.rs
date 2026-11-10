use std::{mem, num};

use codec::{
    staging::{Buffered, Limit},
    storage,
};
use dope::manifold::client::{
    close,
    session::{codec, response::Outcome},
};
use o3::buffer::{
    self, hold,
    resident::{self, staging},
};
use sark_http::{
    h1::{
        codec::decode::{fields::Sink, response::Error},
        head::{ParseError, response},
    },
    payload::response::{Method, status::Code},
};

use crate::{
    client::{CloseReason, Config, Fault, headers},
    frame::Plan,
    inbound,
    upgrade::{self, response::Admission},
};

pub(in crate::client) mod raw;

mod head;
pub use head::Head;
pub(super) use head::Kind;

pub(super) enum Phase {
    Connecting,
    Active(inbound::Inbound<inbound::ServerPeer>),
    Closed,
}

pub struct State {
    pub(super) phase: Phase,
}

fn more<'id, 'input, 'd, R: buffer::Cursor, T>(
    input: &codec::Input<'id, 'input, 'd, R>,
) -> codec::Parse<'id, T> {
    match input.need_more(None) {
        Some(need) => codec::Parse::More(need),
        None => codec::Parse::CapacityExhausted,
    }
}

impl Default for State {
    fn default() -> Self {
        Self {
            phase: Phase::Connecting,
        }
    }
}

#[derive(Default)]
enum Status {
    #[default]
    Idle,
    Connecting([u8; 28]),
    Open,
    Closing(CloseReason),
}

#[derive(Default)]
#[repr(transparent)]
pub struct Connection(Status);

pub(super) enum Handshake {
    Opened,
    Rejected,
}

impl Connection {
    pub(super) fn begin(&mut self, expected: [u8; 28]) {
        self.0 = Status::Connecting(expected);
    }

    pub(super) fn open(&mut self, accept: [u8; 28]) -> Handshake {
        match &mut self.0 {
            Status::Connecting(expected) if *expected == accept => {
                self.0 = Status::Open;
                Handshake::Opened
            }
            Status::Idle | Status::Connecting(_) | Status::Open | Status::Closing(_) => {
                Handshake::Rejected
            }
        }
    }

    pub(super) fn close(&mut self, reason: CloseReason) -> close::Directive {
        let cause = reason.lifecycle();
        match &mut self.0 {
            Status::Closing(_) => {}
            state @ (Status::Idle | Status::Connecting(_) | Status::Open) => {
                *state = Status::Closing(reason);
            }
        }
        close::Directive::reconnect(cause)
    }

    pub(super) fn take_reason(&mut self, cause: close::Reason) -> CloseReason {
        match mem::replace(&mut self.0, Status::Idle) {
            Status::Closing(reason) => reason,
            Status::Idle | Status::Connecting(_) | Status::Open => match cause {
                close::Reason::Local(origin) => CloseReason::Local(origin),
                close::Reason::Capacity(cause) => CloseReason::Capacity(cause),
                close::Reason::EndpointRetired => CloseReason::EndpointRetired,
                close::Reason::Transport => CloseReason::Transport,
                close::Reason::Timeout(kind) => CloseReason::Timeout(kind),
                close::Reason::Protocol | close::Reason::Remote => CloseReason::Protocol,
                close::Reason::Receive(error) => CloseReason::Receive(error),
                close::Reason::Ingress(cause) => CloseReason::Ingress(cause),
                close::Reason::Egress(cause) => CloseReason::Reserve(cause),
            },
        }
    }
}

const _: () = assert!(mem::size_of::<Connection>() == mem::size_of::<Status>());
const _: () = assert!(
    mem::size_of::<Connection>() <= mem::size_of::<CloseReason>() + mem::size_of::<usize>()
);

pub struct Codec {
    pub(super) config: Config,
    pub(super) staging: staging::Plan,
}

impl Codec {
    pub(super) fn try_new(config: Config) -> Result<Self, staging::Error> {
        let limit = config.max_frame_payload;
        let Ok(payload) = num::NonZero::<u32>::try_from(limit) else {
            return Err(staging::Error::LimitTooLarge { limit });
        };
        let limit = Plan::capacity(payload).get().max(headers::MAX_BYTES);
        let staging = staging::Plan::new(headers::MAX_BYTES, limit)?;
        Ok(Self { config, staging })
    }
}

impl codec::Codec for Codec {
    type Head<'input, 'd, R: buffer::Cursor>
        = Head<'d>
    where
        R: 'input,
        'd: 'input;
    type Error = Fault;
    type Staging = Buffered;

    fn staging(&self) -> Result<staging::Plan, staging::Error> {
        Ok(self.staging)
    }
}

/// The exact append-only source and its retained opening progress.
pub struct Storage<S> {
    source: S,
    progress: Progress,
}

enum Progress {
    Connecting(Open),
    Ready {
        wire: num::NonZero<usize>,
        accept: [u8; 28],
    },
    Active,
    Failed(Fault),
}

impl Progress {
    fn new(state: &State) -> Self {
        match state.phase {
            Phase::Connecting => Self::Connecting(Open::new()),
            Phase::Active(_) | Phase::Closed => Self::Active,
        }
    }
}

struct Open {
    head: response::raw::State,
    fields: Result<Admission, upgrade::Error>,
    offset: usize,
}

impl Open {
    fn new() -> Self {
        use sark_http::h1::head::Limit;
        Self {
            head: response::raw::State::new(
                Method::Other,
                Limit::RESPONSE.with_bytes(headers::MAX_BYTES),
            ),
            fields: Ok(Admission::default()),
            offset: 0,
        }
    }
}

fn reject<'d, T, F>(
    progress: &mut Progress,
    error: Fault,
    state: &mut State,
    admit: impl FnOnce(Result<Option<inbound::Reply<'_>>, &Fault>) -> codec::Admission<F>,
) -> T
where
    T: Outcome,
    F: FnOnce(Result<Head<'d>, Fault>, &mut State) -> T::Accepted,
{
    match admit(Err(&error)) {
        codec::Admission::RetryInput => {
            *progress = Progress::Failed(error);
            T::retry_input()
        }
        codec::Admission::Ready(emit) => {
            *progress = Progress::Active;
            T::accept(emit(Err(error), state))
        }
    }
}

impl Codec {
    /// Commits fragmentation and retention only after input admission succeeds.
    fn frame<'id, 'input, 'd, S, T, F>(
        &self,
        input: codec::Input<'id, 'input, 'd, S>,
        state: &mut State,
        progress: &mut Progress,
        admit: impl FnOnce(Result<Option<inbound::Reply<'_>>, &Fault>) -> codec::Admission<F>,
    ) -> Result<codec::Parse<'id, T>, T>
    where
        S: buffer::Cursor + hold::raw::Retain<'d>,
        T: Outcome,
        F: FnOnce(Result<Head<'d>, Fault>, &mut State) -> T::Accepted,
    {
        let inbound = match &mut state.phase {
            Phase::Active(inbound) => inbound,
            Phase::Connecting | Phase::Closed => return Ok(more(&input)),
        };
        let bytes = input.as_slice();
        let frame = match inbound.decode(bytes, self.config.max_frame_payload.get()) {
            Ok(inbound::Decode::Ready(frame)) => frame,
            Ok(inbound::Decode::NeedMore) => {
                return Ok(match inbound.required() {
                    Some(target) => match input.need_to(target) {
                        Some(need) => codec::Parse::More(need),
                        None => codec::Parse::CapacityExhausted,
                    },
                    None => more(&input),
                });
            }
            Err(error) => return Err(reject(progress, Fault::Frame(error), state, admit)),
        };
        let consumed = match input.try_consumed(frame.frame_len()) {
            Ok(consumed) => consumed,
            Err(error) => return Err(reject(progress, Fault::Input(error), state, admit)),
        };
        let range = frame.range();
        // SAFETY: decode admitted this complete frame within the same unchanged
        // bytes borrow, so its private payload range remains in bounds.
        let payload = unsafe { bytes.get_unchecked(range.clone()) };
        let prepared = match frame.prepare(payload) {
            Ok(prepared) => prepared,
            Err(error) => return Err(reject(progress, Fault::Frame(error), state, admit)),
        };
        let reply = match &prepared {
            inbound::Prepared::Ready(_) => None,
            inbound::Prepared::Required(need) => Some(need.reply()),
        };
        let emit = match admit(Ok(reply)) {
            codec::Admission::RetryInput => {
                return Ok(codec::Parse::Item {
                    head: T::retry_input(),
                    consumed,
                });
            }
            codec::Admission::Ready(emit) => emit,
        };
        match Head::frame(&input, prepared, range) {
            Ok(head) => Ok(codec::Parse::Item {
                head: T::accept(emit(Ok(head), state)),
                consumed,
            }),
            Err(error) => Err(T::accept(emit(Err(error), state))),
        }
    }
}

impl codec::Stateful for Codec {
    type State<'d> = State;
    type Storage<S> = Storage<S>;
    type Outcome<'d> = Head<'d>;
    type Preview<'a> = Option<inbound::Reply<'a>>;

    fn state<'d>(&self, _: resident::Budget<'d>) -> Self::State<'d> {
        State::default()
    }

    fn storage<'d, S>(&self, source: S, state: &State) -> Storage<S> {
        Storage {
            source,
            progress: Progress::new(state),
        }
    }

    fn source<S: buffer::raw::Source>(storage: &Storage<S>) -> &S {
        &storage.source
    }

    fn edit<S>(storage: &mut Storage<S>) -> storage::Edit<'_, S> {
        storage::Edit::new(&mut storage.source)
    }

    fn restart<'a, 'd, S>(
        &self,
        storage: &'a mut Storage<S>,
        state: &State,
    ) -> storage::Edit<'a, S> {
        storage.progress = Progress::new(state);
        Self::edit(storage)
    }

    fn decode<'d, S, T, F>(
        &self,
        storage: &mut Storage<S>,
        state: &mut State,
        budget: &resident::Budget<'d>,
        limit: Limit,
        admit: impl FnOnce(Result<Self::Preview<'_>, &Fault>) -> codec::Admission<F>,
    ) -> codec::Step<T>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        T: Outcome,
        F: FnOnce(Result<Head<'d>, Fault>, &mut State) -> T::Accepted,
    {
        let progress = match storage.progress {
            Progress::Active => None,
            _ => Some(mem::replace(&mut storage.progress, Progress::Active)),
        };
        let ready = match progress {
            Some(Progress::Connecting(mut open)) => {
                use sark_http::h1::head::Limit;
                let source = storage.source.as_ref();
                loop {
                    // SAFETY: Each complete informational head admits offset within the
                    // retained prefix. Storage appends unchanged bytes; destructive edits
                    // restart Open, so this suffix and the scanner's prefix remain valid.
                    let (progress, head) = match unsafe {
                        open.head
                            .advance_from(source.get_unchecked(open.offset..), |field| {
                                if let Ok(fields) = &mut open.fields
                                    && let Err(error) = fields.entry(field)
                                {
                                    open.fields = Err(error);
                                }
                                Ok(())
                            })
                    } {
                        Ok(progress) => progress,
                        Err(error) => break Err(Fault::Open(Error::Head(error))),
                    };
                    open.head = progress;
                    let Some(head) = head else {
                        storage.progress = Progress::Connecting(open);
                        return codec::Input::wait(&storage.source, budget, limit, None);
                    };
                    let head = match head.admit() {
                        Ok(head) => head,
                        Err(error) => break Err(Fault::Open(Error::Framing(error))),
                    };
                    let wire = num::NonZero::<usize>::MIN
                        .saturating_add(open.offset + head.wire_len().get() as usize - 1);
                    let fields = mem::replace(&mut open.fields, Ok(Admission::default()));
                    if head.status().is_info() && head.status() != Code::SWITCHING_PROTOCOLS {
                        open.offset = wire.get();
                        open.head = response::raw::State::new(
                            Method::Other,
                            Limit::RESPONSE.with_bytes(headers::MAX_BYTES - open.offset),
                        );
                        continue;
                    }
                    break fields
                        .map_err(ParseError::Sink)
                        .map_err(Error::Head)
                        .map_err(Fault::Open)
                        .and_then(|fields| {
                            fields
                                .finish(head.version(), head.status(), head.control().connection())
                                .map_err(Fault::Handshake)
                        })
                        .map(|accept| (wire, accept));
                }
            }
            Some(Progress::Ready { wire, accept }) => Ok((wire, accept)),
            Some(Progress::Failed(error)) => Err(error),
            None | Some(Progress::Active) => {
                return codec::Input::commit(&mut storage.source, budget, limit, |input| {
                    self.frame(input, state, &mut storage.progress, admit)
                });
            }
        };
        let (wire, accept) = match ready {
            Ok(ready) => ready,
            Err(error) => {
                return codec::Step::rejected(reject(&mut storage.progress, error, state, admit));
            }
        };
        storage.progress = Progress::Ready { wire, accept };
        let step = codec::Input::commit(&mut storage.source, budget, limit, |input| {
            let consumed = match input.try_consumed(wire) {
                Ok(consumed) => consumed,
                Err(error) => {
                    return Err(reject(
                        &mut storage.progress,
                        Fault::Input(error),
                        state,
                        admit,
                    ));
                }
            };
            let head = match admit(Ok(None)) {
                codec::Admission::RetryInput => T::retry_input(),
                codec::Admission::Ready(emit) => {
                    T::accept(emit(Ok(Head(Kind::Opened { accept })), state))
                }
            };
            Ok(codec::Parse::Item { head, consumed })
        });
        if step.has_consumed() {
            state.phase = Phase::Active(inbound::Inbound::new(self.config.max_message_size.get()));
            storage.progress = Progress::Active;
        }
        step
    }

    fn finish<'slot, 'id, 'd, S, T, F>(
        &self,
        mut input: storage::Owner<'slot, 'id, Storage<S>, State>,
        _: &resident::Budget<'d>,
        _: Limit,
        admit: impl FnOnce(Result<Self::Preview<'_>, &Fault>) -> codec::Admission<F>,
    ) -> (
        storage::Owner<'slot, 'id, Storage<S>, State>,
        Result<codec::Finish<T>, T>,
    )
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        T: Outcome,
        F: FnOnce(Result<Head<'d>, Fault>, &mut State) -> T::Accepted,
    {
        let (storage, state) = input.parts();
        let progress = mem::replace(&mut storage.progress, Progress::Active);
        let result = if let Progress::Failed(error) = progress {
            Err(reject(&mut storage.progress, error, state, admit))
        } else {
            storage.progress = progress;
            let actual = storage.source.as_ref().len();
            let error = match &state.phase {
                Phase::Connecting if actual != 0 => Some(Fault::OpenTail { actual }),
                Phase::Active(_) if actual != 0 => Some(Fault::FrameTail { actual }),
                Phase::Connecting | Phase::Active(_) | Phase::Closed => None,
            };
            match error {
                Some(error) => Err(reject(&mut storage.progress, error, state, admit)),
                None => Ok(codec::Finish::Empty),
            }
        };
        (input, result)
    }
}
