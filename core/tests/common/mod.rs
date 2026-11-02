use core::convert::Infallible;

use o3::buffer::segments;
use sark_core::body::{
    self,
    admit::{Length, Open, Plan, Retention, Stream, Track},
    buffered, credit, input,
};
use sark_http::payload::owned;

pub mod deadline;
pub mod decoders;
pub mod duplex;
pub mod life;
pub mod ready;
pub mod requests;
pub mod schedule;
pub mod trace;

pub const LIMITS: credit::Limits = credit::Limits::new(16, 4);

pub const CREDIT: credit::Amount = credit::Amount::new(3, segments::Count::ONE);

pub struct Store;

pub trait Seed {
    type Origin: credit::Origin;

    fn seed<E: credit::Evidence>(
        &mut self,
        ledger: &mut credit::raw::Ledger<'_, Self::Origin>,
        event: E,
    ) -> (bool, buffered::Push<(), Infallible>);
}

impl<O: credit::Origin> Seed for buffered::State<(), O> {
    type Origin = O;

    fn seed<E: credit::Evidence>(
        &mut self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        event: E,
    ) -> (bool, buffered::Push<(), Infallible>) {
        let mut empty = false;
        let pushed = self.try_push(
            ledger,
            event,
            input::Input::new((), 3, segments::Count::ONE),
            LIMITS,
            LIMITS,
            |storage, ()| {
                empty = storage.is_none();
                buffered::Retain::<(), (), Infallible>::Accepted { storage: Some(()) }
            },
        );
        (empty, pushed)
    }
}

pub trait Retained: Sized {
    fn try_start(len: usize) -> Result<Self, body::Error>;
    fn retain<'d, O: credit::Origin>(
        self,
        segment: owned::Segment<'d>,
        ledger: &mut credit::raw::Ledger<'_, O>,
    ) -> (
        buffered::State<Vec<owned::Segment<'d>>, O>,
        buffered::Push<owned::Segment<'d>, Infallible>,
    );
}

impl Retained for Track<Open> {
    fn try_start(len: usize) -> Result<Self, body::Error> {
        Plan::new(
            Retention::Retained,
            16,
            Length::Known(len),
            Stream::Streaming,
        )
        .try_open(usize::MAX)
    }

    fn retain<'d, O: credit::Origin>(
        self,
        segment: owned::Segment<'d>,
        ledger: &mut credit::raw::Ledger<'_, O>,
    ) -> (
        buffered::State<Vec<owned::Segment<'d>>, O>,
        buffered::Push<owned::Segment<'d>, Infallible>,
    ) {
        let mut buffered = buffered::State::<Vec<owned::Segment<'d>>, _>::start(self, ledger);
        let pushed = buffered.try_push(
            ledger,
            credit::Disabled::new(),
            input::Input::new(segment, 3, segments::Count::ONE),
            LIMITS,
            LIMITS,
            |storage, segment| {
                let mut storage = storage.unwrap_or_default();
                storage.push(segment);
                buffered::Retain::<Vec<owned::Segment<'_>>, owned::Segment<'_>, Infallible>::Accepted {
                    storage: Some(storage),
                }
            },
        );
        (buffered, pushed)
    }
}

pub fn discarded(max_body: usize, declared: Option<usize>) -> Result<Track<Open>, body::Error> {
    Plan::new(
        Retention::Discarded,
        max_body,
        Length::from_option(declared),
        Stream::Buffered,
    )
    .try_open(usize::MAX)
}
