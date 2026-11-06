use o3::depot::{array::Index, queue::indexed::lanes::typed};
use typed::{Vacancy, Value};

use crate::manifold::{
    self,
    schedule::{
        Block, CONNECTION_WAIT, DATA_READY, LANES, STREAM_WAIT, Settlement, Terminal, Turn, Wait,
        WorkClass,
        drive::{self, Effect, TurnOutcome},
        typed::{Control, Driver, Next, place},
    },
};

pub(super) struct Source<'a, K: manifold::raw::Key, C, D, I, F> {
    domain: &'a mut typed::Table<K, C, D, I, LANES>,
    driver: &'a mut F,
}

impl<'a, K: manifold::raw::Key, C, D, I, F> Source<'a, K, C, D, I, F> {
    pub(super) fn new(domain: &'a mut typed::Table<K, C, D, I, LANES>, driver: &'a mut F) -> Self {
        Self { domain, driver }
    }
}

impl<K, C, D, I, F> drive::Source for Source<'_, K, C, D, I, F>
where
    K: manifold::raw::Key,
    F: Driver<C, D, I, Key = K>,
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

    fn turn(&mut self, source: Index<LANES>, _: WorkClass) -> drive::Turn<K> {
        let Some(current) = self.domain.front(source) else {
            return drive::Turn::Empty;
        };
        let Some(permit) = self.driver.permit() else {
            return drive::Turn::Blocked;
        };
        let outcome = match current {
            typed::Front::Control(current) => {
                let stream = current.key();
                let (vacant, value) = current.take();
                let result = self.driver.control(permit, stream, value);
                control(self.driver, vacant, stream, result)
            }
            typed::Front::Data(current) => data(self.driver, current, permit),
        };
        drive::Turn::Ready(outcome)
    }
}

fn control<K, C, D, I, F>(
    driver: &mut F,
    vacant: typed::Withdrawn<'_, K, C, D, I, typed::Control, LANES>,
    stream: K,
    result: Control<C, D, I, F::Block, F::Error>,
) -> TurnOutcome<K>
where
    K: manifold::raw::Key,
    F: Driver<C, D, I, Key = K>,
{
    match result {
        Control::Control { value, turn } => next(
            driver,
            vacant,
            stream,
            typed::Active::Control(value),
            turn,
            WorkClass::Control,
        ),
        Control::Data { value, turn } => next(
            driver,
            vacant,
            stream,
            typed::Active::Data(value),
            turn,
            WorkClass::Control,
        ),
        Control::Fused { value, turn } => next(
            driver,
            vacant,
            stream,
            typed::Active::Data(value),
            turn,
            WorkClass::Data,
        ),
        Control::Idle(value) => {
            vacant.insert::<typed::Idle>(value);
            TurnOutcome::new(stream, WorkClass::Control, Effect::None)
        }
        Control::Failed { value, error } => settle(
            driver,
            vacant,
            stream,
            value,
            Terminal::Failed(error),
            WorkClass::Control,
        ),
        Control::Complete => TurnOutcome::new(stream, WorkClass::Control, Effect::Complete),
        Control::Remove => TurnOutcome::new(stream, WorkClass::Control, Effect::None),
    }
}

fn next<K, C, D, I, F>(
    driver: &mut F,
    vacant: typed::Withdrawn<'_, K, C, D, I, typed::Control, LANES>,
    stream: K,
    value: typed::Active<C, D>,
    turn: Turn<F::Block>,
    activity: WorkClass,
) -> TurnOutcome<K>
where
    K: manifold::raw::Key,
    F: Driver<C, D, I, Key = K>,
{
    let blocked = match turn {
        Turn::Progress => None,
        Turn::Blocked(block) => Some(block),
        Turn::Complete | Turn::Idle => {
            let terminal = match turn {
                Turn::Complete => Terminal::Complete,
                _ => Terminal::Idle,
            };
            let value = match value {
                typed::Active::Control(value) => Value::Control(value),
                typed::Active::Data(value) => Value::Data(value),
            };
            return settle(driver, vacant, stream, value, terminal, activity);
        }
    };
    let next = match value {
        typed::Active::Control(value) => Next::Control(value, blocked),
        typed::Active::Data(value) => Next::Data(value, blocked),
    };
    let effect = place::enqueue(vacant, next);
    TurnOutcome::new(stream, activity, effect)
}

fn data<K, C, D, I, F>(
    driver: &mut F,
    mut current: typed::Slot<'_, K, C, D, I, typed::Data, LANES>,
    permit: F::Permit,
) -> TurnOutcome<K>
where
    K: manifold::raw::Key,
    F: Driver<C, D, I, Key = K>,
{
    let stream = current.key();
    let turn = match driver.data(permit, stream, current.get_mut()) {
        Ok(turn) => turn,
        Err(error) => {
            let (vacant, value) = current.take();
            return settle(
                driver,
                vacant,
                stream,
                Value::Data(value),
                Terminal::Failed(error),
                WorkClass::Data,
            );
        }
    };
    let effect = match turn {
        Turn::Progress => {
            current.move_to_back(DATA_READY);
            Effect::None
        }
        Turn::Blocked(block) => match block.wait() {
            Wait::Connection => {
                current.move_to_back(CONNECTION_WAIT);
                Effect::None
            }
            Wait::Stream => {
                current.move_to_back(STREAM_WAIT);
                Effect::None
            }
            Wait::Egress => Effect::BlockData,
        },
        Turn::Complete | Turn::Idle => {
            let terminal = match turn {
                Turn::Complete => Terminal::Complete,
                _ => Terminal::Idle,
            };
            let (vacant, value) = current.take();
            return settle(
                driver,
                vacant,
                stream,
                Value::Data(value),
                terminal,
                WorkClass::Data,
            );
        }
    };
    TurnOutcome::new(stream, WorkClass::Data, effect)
}

fn settle<'a, K, C, D, I, F>(
    driver: &mut F,
    vacant: impl Vacancy<'a, K, C, D, I, LANES>,
    stream: K,
    value: Value<C, D, I>,
    terminal: Terminal<F::Error>,
    activity: WorkClass,
) -> TurnOutcome<K>
where
    K: manifold::raw::Key + 'a,
    C: 'a,
    D: 'a,
    I: 'a,
    F: Driver<C, D, I, Key = K>,
{
    let effect = match &terminal {
        Terminal::Complete => Effect::Complete,
        Terminal::Idle | Terminal::Failed(_) => Effect::None,
    };
    if let Settlement::Keep(value) = driver.settle(stream, value, terminal) {
        place::store(vacant, value);
    }
    TurnOutcome::new(stream, activity, effect)
}
