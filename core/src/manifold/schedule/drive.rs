use o3::depot::{
    array::Index,
    queue::indexed::lanes::{Table, entry},
};

use crate::manifold::{
    self,
    schedule::{
        Block, CONTROL_READY, ControlTurn, DATA_READY, Driver, LANES, Report, Settlement, Step,
        Terminal, Wait, WorkClass, policy::Policy,
    },
};

pub(super) trait Source {
    type Key: manifold::raw::Key;

    fn len(&self) -> usize;

    fn back(&self, lane: Index<LANES>) -> Option<Self::Key>;

    fn is_vacant(&self, lane: Index<LANES>) -> bool;

    fn turn(&mut self, lane: Index<LANES>, class: WorkClass) -> Turn<Self::Key>;
}

pub(super) struct Drive<'a, S> {
    domain: S,
    policy: &'a mut Policy,
    report: Report,
    remaining: usize,
}

pub(super) struct Uniform<'a, K: manifold::raw::Key, T, D> {
    domain: &'a mut Table<K, T, LANES>,
    driver: &'a mut D,
}

impl<'a, K, T, D> Drive<'a, Uniform<'a, K, T, D>>
where
    K: manifold::raw::Key,
    D: Driver<T, Key = K>,
{
    pub(super) fn new(
        domain: &'a mut Table<K, T, LANES>,
        policy: &'a mut Policy,
        driver: &'a mut D,
    ) -> Self {
        Self::with(Uniform { domain, driver }, policy)
    }
}

impl<'a, S: Source> Drive<'a, S> {
    pub(super) fn with(domain: S, policy: &'a mut Policy) -> Self {
        let left = domain.len();
        Self {
            domain,
            policy,
            report: Report::default(),
            remaining: left,
        }
    }

    pub(super) fn run<const ONE_ROUND: bool>(mut self) -> Report {
        while self.remaining != 0 {
            let tail = if self.policy.is_blocked() {
                None
            } else {
                self.domain.back(DATA_READY)
            };
            let has_data = tail.is_some();
            let control = !self.domain.is_vacant(CONTROL_READY);
            if !has_data && !control {
                break;
            }
            let both = has_data && control;
            let first = if both {
                Some(if self.policy.is_data_next() {
                    WorkClass::Data
                } else {
                    WorkClass::Control
                })
            } else {
                None
            };
            let mut activity = None;
            let mut done = false;

            let stop = 'round: {
                if matches!(first, Some(WorkClass::Data))
                    && let Some(last) = tail
                {
                    let outcome = match self.one(DATA_READY, WorkClass::Data) {
                        Turn::Ready(outcome) => outcome,
                        Turn::Empty | Turn::Blocked => break 'round true,
                    };
                    done = outcome.stream == last;
                    activity = Some(outcome.activity);
                }

                if control && self.remaining != 0 {
                    let outcome = match self.one(CONTROL_READY, WorkClass::Control) {
                        Turn::Ready(outcome) => outcome,
                        Turn::Empty | Turn::Blocked => break 'round true,
                    };
                    activity = Some(outcome.activity);
                }

                while tail.is_some() && !done && self.remaining != 0 && !self.policy.is_blocked() {
                    let outcome = match self.one(DATA_READY, WorkClass::Data) {
                        Turn::Ready(outcome) => outcome,
                        Turn::Empty => break,
                        Turn::Blocked => break 'round true,
                    };
                    done = Some(outcome.stream) == tail;
                    activity = Some(outcome.activity);
                }
                false
            };

            if let Some(activity) = activity {
                self.policy.finish_round(first.unwrap_or(activity));
            }
            if stop || ONE_ROUND {
                break;
            }
        }
        self.report
    }

    fn one(&mut self, source: Index<LANES>, class: WorkClass) -> Turn<S::Key> {
        const {
            assert!(size_of::<Turn<S::Key>>() == size_of::<Option<TurnOutcome<S::Key>>>());
        }
        let outcome = match self.domain.turn(source, class) {
            Turn::Ready(outcome) => outcome,
            turn => return turn,
        };
        match outcome.effect {
            Effect::None => {}
            Effect::Complete => self.report.completed += 1,
            Effect::BlockData => self.policy.block_data(),
        }
        self.remaining -= 1;
        self.report.turns += 1;
        Turn::Ready(outcome)
    }
}

impl<K, T, D> Source for Uniform<'_, K, T, D>
where
    K: manifold::raw::Key,
    D: Driver<T, Key = K>,
{
    type Key = K;

    fn len(&self) -> usize {
        self.domain.len()
    }

    fn back(&self, lane: Index<LANES>) -> Option<K> {
        self.domain.back(lane)
    }

    fn is_vacant(&self, lane: Index<LANES>) -> bool {
        self.domain.is_vacant(lane)
    }

    fn turn(&mut self, source: Index<LANES>, class: WorkClass) -> Turn<K> {
        let driver = &mut *self.driver;
        let Some(mut current) = self.domain.front(source) else {
            return Turn::Empty;
        };
        let Some(permit) = driver.permit() else {
            return Turn::Blocked;
        };
        let stream = current.key();
        let result = match class {
            WorkClass::Control => driver
                .control(permit, stream, current.get_mut())
                .map(ControlTurn::into_parts),
            WorkClass::Data => driver
                .data(permit, stream, current.get_mut())
                .map(|step| (step, WorkClass::Data)),
        };
        let outcome = match result {
            Ok((step, activity)) => borrowed(driver, current, stream, step, activity),
            Err(error) => finish(driver, current, stream, Terminal::Failed(error), class),
        };
        Turn::Ready(outcome)
    }
}

pub(super) enum Turn<K> {
    Empty,
    Blocked,
    Ready(TurnOutcome<K>),
}

pub(super) enum Effect {
    None,
    Complete,
    BlockData,
}

fn borrowed<K, T, D>(
    driver: &mut D,
    current: entry::Occupied<'_, K, T, LANES>,
    stream: K,
    step: Step<D::Block>,
    activity: WorkClass,
) -> TurnOutcome<K>
where
    K: manifold::raw::Key,
    D: Driver<T, Key = K>,
{
    match step {
        Step::Complete => finish(driver, current, stream, Terminal::Complete, activity),
        Step::Progress(next) => {
            current.move_to_back(next.ready_lane());
            TurnOutcome::new(stream, activity, Effect::None)
        }
        Step::Blocked(block) => {
            let wait = block.wait();
            let blocked = matches!((wait, activity), (Wait::Egress, WorkClass::Data));
            if !blocked || current.lane() != Some(DATA_READY) {
                current.move_to_back(activity.blocked_lane(Some(wait)));
            }
            if blocked {
                TurnOutcome::new(stream, activity, Effect::BlockData)
            } else {
                TurnOutcome::new(stream, activity, Effect::None)
            }
        }
        Step::Idle => finish(driver, current, stream, Terminal::Idle, activity),
    }
}

fn finish<K, T, D>(
    driver: &mut D,
    mut current: entry::Occupied<'_, K, T, LANES>,
    stream: K,
    terminal: Terminal<D::Error>,
    activity: WorkClass,
) -> TurnOutcome<K>
where
    K: manifold::raw::Key,
    D: Driver<T, Key = K>,
{
    let effect = match &terminal {
        Terminal::Complete => Effect::Complete,
        Terminal::Idle | Terminal::Failed(_) => Effect::None,
    };
    match driver.settle(stream, current.get_mut(), terminal) {
        Settlement::Keep(()) => current.unlink(),
        Settlement::Remove => current.discard(),
    }
    TurnOutcome::new(stream, activity, effect)
}

pub(super) struct TurnOutcome<K> {
    stream: K,
    activity: WorkClass,
    effect: Effect,
}

impl<K> TurnOutcome<K> {
    pub(super) const fn new(stream: K, activity: WorkClass, effect: Effect) -> Self {
        Self {
            stream,
            activity,
            effect,
        }
    }
}
