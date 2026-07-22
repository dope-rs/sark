use std::pin::Pin;
use std::task::Poll;

use dope_fiber::{Context, Fiber, WaitQueue, Waiter};
use o3::cell::RawCell;
use o3::collections::{FixedHashTable, PinSlab, SlabKey};

use super::cache::Asset;
use super::loader::LoadError;

#[derive(Clone)]
pub(super) enum Outcome {
    Loaded(Asset),
    Failed(LoadError),
}

enum FlightTag {}

type FlightKey = SlabKey<FlightTag>;

struct Flight {
    hash: u64,
    key: Box<[u8]>,
    waiters: usize,
    waiter_capacity: usize,
    outcome: Option<Outcome>,
    wake: WaitQueue,
}

impl Flight {
    fn wait_queue(&self) -> Pin<&WaitQueue> {
        // SAFETY: Flight is inserted into PinSlab before this method is called,
        // and entries are never moved until every waiter has detached.
        unsafe { Pin::new_unchecked(&self.wake) }
    }
}

struct FlightIndex {
    key: FlightKey,
}

struct Flights {
    index: Option<FixedHashTable<FlightIndex>>,
    entries: PinSlab<Flight, FlightTag>,
    waiter_capacity: usize,
}

impl Flights {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            index: (capacity != 0).then(|| FixedHashTable::with_capacity(capacity)),
            entries: PinSlab::with_capacity(capacity),
            waiter_capacity: capacity,
        }
    }

    fn find(&self, hash: u64, key: &[u8]) -> Option<FlightKey> {
        self.index
            .as_ref()?
            .get(hash, |index| {
                self.entries
                    .get(index.key)
                    .is_some_and(|flight| flight.key.as_ref() == key)
            })
            .map(|index| index.key)
    }

    fn remove(&mut self, key: FlightKey) {
        let Some(flight) = self.entries.get(key) else {
            return;
        };
        let hash = flight.hash;
        if let Some(index) = self.index.as_mut() {
            index.remove(hash, |index| index.key == key);
        }
        self.entries.remove(key);
    }

    fn flight_mut(&mut self, key: FlightKey) -> Option<&mut Flight> {
        let flight = self.entries.get_mut(key)?;
        // SAFETY: only non-structural fields are mutated; the pinned WaitQueue
        // is never moved or replaced while the Flight remains in PinSlab.
        Some(unsafe { flight.get_unchecked_mut() })
    }
}

pub(super) struct Hub {
    flights: RawCell<Flights>,
}

impl Hub {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            flights: RawCell::new(Flights::with_capacity(capacity)),
        }
    }

    pub(super) fn begin<'d>(&self, hash: u64, key: &[u8]) -> Start<'_, 'd> {
        self.with_flights(|flights| {
            if flights.index.is_none() {
                return Start::Untracked;
            }
            if let Some(key) = flights.find(hash, key) {
                let flight = flights.flight_mut(key).expect("flight missing");
                if flight.waiters == flight.waiter_capacity {
                    return Start::Overloaded;
                }
                flight.waiters += 1;
                return Start::Follower(Wait {
                    hub: self,
                    key,
                    waiter: Waiter::new(),
                    done: false,
                });
            }
            let waiter_capacity = flights.waiter_capacity;
            let Some(entry) = flights.entries.vacant_entry() else {
                return Start::Overloaded;
            };
            let key = entry.insert(Flight {
                hash,
                key: Box::from(key),
                waiters: 0,
                waiter_capacity,
                outcome: None,
                wake: WaitQueue::with_capacity(waiter_capacity),
            });
            if flights
                .index
                .as_mut()
                .expect("flight index missing")
                .try_insert(hash, FlightIndex { key }, |_| false)
                .is_err()
            {
                flights.entries.remove(key);
                return Start::Overloaded;
            }
            Start::Leader(Leader {
                hub: self,
                key,
                done: false,
            })
        })
    }

    fn finish(&self, key: FlightKey, outcome: Outcome) {
        self.with_flights(|flights| {
            let Some(waiters) = flights.entries.get(key).map(|flight| flight.waiters) else {
                return;
            };
            if waiters == 0 {
                flights.remove(key);
            } else {
                let flight = flights.flight_mut(key).expect("flight missing");
                flight.outcome = Some(outcome);
                flight.wait_queue().wake();
            }
        });
    }

    fn with_flights<R>(&self, operation: impl FnOnce(&mut Flights) -> R) -> R {
        // SAFETY: Hub is owned by Rc-backed, thread-local ServeDir state. The
        // closure completes synchronously and no Hub operation re-enters it.
        unsafe { self.flights.with_mut(operation) }
    }
}

pub(super) struct Leader<'a> {
    hub: &'a Hub,
    key: FlightKey,
    done: bool,
}

impl Leader<'_> {
    pub(super) fn finish(mut self, outcome: Outcome) {
        self.hub.finish(self.key, outcome);
        self.done = true;
    }
}

impl Drop for Leader<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.hub
                .finish(self.key, Outcome::Failed(LoadError::NotFound));
        }
    }
}

pub(super) struct Wait<'a, 'd> {
    hub: &'a Hub,
    key: FlightKey,
    waiter: Waiter<'d>,
    done: bool,
}

impl<'d> Fiber<'d> for Wait<'_, 'd> {
    type Output = Outcome;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        // SAFETY: Wait is pinned for the duration of Fiber::poll. We mutate only
        // scalar bookkeeping and never move the waiter field.
        let this = unsafe { self.get_unchecked_mut() };
        this.hub.with_flights(|flights| {
            let Some(outcome) = flights
                .entries
                .get(this.key)
                .map(|flight| flight.outcome.clone())
            else {
                this.done = true;
                return Poll::Ready(Outcome::Failed(LoadError::NotFound));
            };
            if let Some(outcome) = outcome {
                pinned_waiter(&this.waiter).unregister();
                let flight = flights.flight_mut(this.key).expect("flight missing");
                flight.waiters -= 1;
                let remove = flight.waiters == 0;
                if remove {
                    flights.remove(this.key);
                }
                this.done = true;
                return Poll::Ready(outcome);
            }
            let registered = flights.entries.get(this.key).is_some_and(|flight| {
                flight
                    .wait_queue()
                    .try_register(pinned_waiter(&this.waiter), cx.as_ref())
            });
            if !registered {
                flights
                    .flight_mut(this.key)
                    .expect("flight missing")
                    .waiters -= 1;
                this.done = true;
                return Poll::Ready(Outcome::Failed(LoadError::Overloaded));
            }
            Poll::Pending
        })
    }
}

impl Drop for Wait<'_, '_> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        pinned_waiter(&self.waiter).unregister();
        self.hub.with_flights(|flights| {
            let Some(flight) = flights.flight_mut(self.key) else {
                return;
            };
            flight.waiters -= 1;
            let remove = flight.waiters == 0 && flight.outcome.is_some();
            if remove {
                flights.remove(self.key);
            }
        });
    }
}

pub(super) enum Start<'a, 'd> {
    Leader(Leader<'a>),
    Follower(Wait<'a, 'd>),
    Untracked,
    Overloaded,
}

fn pinned_waiter<'a, 'd>(waiter: &'a Waiter<'d>) -> Pin<&'a Waiter<'d>> {
    // SAFETY: callers only pass waiter fields belonging to pinned Wait values;
    // the field is never moved until it is unregistered or dropped.
    unsafe { Pin::new_unchecked(waiter) }
}
