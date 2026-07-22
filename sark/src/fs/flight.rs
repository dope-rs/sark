use std::pin::Pin;
use std::task::Poll;

use dope_fiber::{Context, Fiber, WaitQueue, Waiter};
use o3::collections::{FixedHashTable, PinSlab, SlabKey};
use pin_project::pinned_drop;

use super::access::ProofCell;
use super::cache::Asset;
use super::loader::LoadError;

#[derive(Clone)]
pub(super) enum Outcome {
    Loaded(Asset),
    Failed(LoadError),
}

enum FlightTag {}

type FlightKey = SlabKey<FlightTag>;

#[pin_project::pin_project]
struct Flight {
    hash: u64,
    key: Box<[u8]>,
    waiters: usize,
    waiter_capacity: usize,
    outcome: Option<Outcome>,
    #[pin]
    wake: WaitQueue,
}

impl Flight {
    fn wait_queue(self: Pin<&Self>) -> Pin<&WaitQueue> {
        self.project_ref().wake
    }

    fn attach_waiter(self: Pin<&mut Self>) -> bool {
        let this = self.project();
        if *this.waiters == *this.waiter_capacity {
            return false;
        }
        *this.waiters += 1;
        true
    }

    fn detach_waiter(self: Pin<&mut Self>) -> bool {
        let waiters = self.project().waiters;
        *waiters -= 1;
        *waiters == 0
    }

    fn complete(self: Pin<&mut Self>, outcome: Outcome) {
        let this = self.project();
        *this.outcome = Some(outcome);
        this.wake.as_ref().wake();
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
}

pub(super) struct Hub {
    flights: ProofCell<Flights>,
}

impl Hub {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            flights: ProofCell::new(Flights::with_capacity(capacity)),
        }
    }

    pub(super) fn begin<'d>(&self, hash: u64, key: &[u8]) -> Start<'_, 'd> {
        self.with_flights(|flights| {
            if flights.index.is_none() {
                return Start::Untracked;
            }
            if let Some(key) = flights.find(hash, key) {
                let flight = flights.entries.get_mut(key).expect("flight missing");
                if !flight.attach_waiter() {
                    return Start::Overloaded;
                }
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
                flights
                    .entries
                    .get_mut(key)
                    .expect("flight missing")
                    .complete(outcome);
            }
        });
    }

    fn with_flights<R>(&self, operation: impl FnOnce(&mut Flights) -> R) -> R {
        self.flights.with_mut(operation)
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

#[pin_project::pin_project(PinnedDrop)]
pub(super) struct Wait<'a, 'd> {
    hub: &'a Hub,
    key: FlightKey,
    #[pin]
    waiter: Waiter<'d>,
    done: bool,
}

impl<'d> Fiber<'d> for Wait<'_, 'd> {
    type Output = Outcome;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.project();
        let hub = *this.hub;
        let key = *this.key;
        hub.with_flights(|flights| {
            let Some(outcome) = flights
                .entries
                .get(key)
                .map(|flight| flight.outcome.clone())
            else {
                *this.done = true;
                return Poll::Ready(Outcome::Failed(LoadError::NotFound));
            };
            if let Some(outcome) = outcome {
                this.waiter.as_ref().unregister();
                let remove = flights
                    .entries
                    .get_mut(key)
                    .expect("flight missing")
                    .detach_waiter();
                if remove {
                    flights.remove(key);
                }
                *this.done = true;
                return Poll::Ready(outcome);
            }
            let registered = flights.entries.get(key).is_some_and(|flight| {
                flight
                    .wait_queue()
                    .try_register(this.waiter.as_ref(), cx.as_ref())
            });
            if !registered {
                flights
                    .entries
                    .get_mut(key)
                    .expect("flight missing")
                    .detach_waiter();
                *this.done = true;
                return Poll::Ready(Outcome::Failed(LoadError::Overloaded));
            }
            Poll::Pending
        })
    }
}

#[pinned_drop]
impl PinnedDrop for Wait<'_, '_> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        if *this.done {
            return;
        }
        this.waiter.as_ref().unregister();
        let hub = *this.hub;
        let key = *this.key;
        hub.with_flights(|flights| {
            let Some(mut flight) = flights.entries.get_mut(key) else {
                return;
            };
            let completed = flight.outcome.is_some();
            let remove = flight.as_mut().detach_waiter() && completed;
            if remove {
                flights.remove(key);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropped_follower_detaches_before_leader_completion() {
        let hub = Hub::new(1);
        let start: Start<'_, 'static> = hub.begin(1, b"asset");
        let leader = match start {
            Start::Leader(leader) => leader,
            _ => panic!("first request must lead"),
        };
        let start: Start<'_, 'static> = hub.begin(1, b"asset");
        let follower = match start {
            Start::Follower(follower) => follower,
            _ => panic!("second request must follow"),
        };

        drop(follower);
        hub.with_flights(|flights| {
            let key = flights.find(1, b"asset").expect("flight missing");
            assert_eq!(flights.entries.get(key).expect("flight missing").waiters, 0);
        });

        leader.finish(Outcome::Failed(LoadError::NotFound));
        hub.with_flights(|flights| assert!(flights.find(1, b"asset").is_none()));
    }
}
