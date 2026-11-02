use o3::depot::queue::indexed::lanes::typed;
use typed::Vacancy;

use crate::manifold::{
    self,
    schedule::{Block, LANES, Wait, WorkClass, drive::Effect, policy::Policy, typed::Next},
};

pub(super) fn insert<'a, K, C, D, I, B>(
    vacant: impl Vacancy<'a, K, C, D, I, LANES>,
    next: Next<C, D, I, B>,
    policy: &mut Policy,
) where
    K: manifold::raw::Key + 'a,
    C: 'a,
    D: 'a,
    I: 'a,
    B: Block,
{
    if matches!(enqueue(vacant, next), Effect::BlockData) {
        policy.block_data();
    }
}

pub(super) fn enqueue<'a, K, C, D, I, B>(
    vacant: impl Vacancy<'a, K, C, D, I, LANES>,
    next: Next<C, D, I, B>,
) -> Effect
where
    K: manifold::raw::Key + 'a,
    C: 'a,
    D: 'a,
    I: 'a,
    B: Block,
{
    match next {
        Next::Control(value, blocked) => {
            vacant
                .insert::<typed::Control>(value)
                .move_to_back(WorkClass::Control.blocked_lane(blocked.map(Block::wait)));
            Effect::None
        }
        Next::Data(value, blocked) => {
            let wait = blocked.map(Block::wait);
            vacant
                .insert::<typed::Data>(value)
                .move_to_back(WorkClass::Data.blocked_lane(wait));
            if matches!(wait, Some(Wait::Egress)) {
                Effect::BlockData
            } else {
                Effect::None
            }
        }
        Next::Idle(value) => {
            vacant.insert::<typed::Idle>(value);
            Effect::None
        }
    }
}

pub(super) fn store<'a, K, C, D, I>(
    vacant: impl Vacancy<'a, K, C, D, I, LANES>,
    value: typed::Value<C, D, I>,
) where
    K: manifold::raw::Key + 'a,
    C: 'a,
    D: 'a,
    I: 'a,
{
    match value {
        typed::Value::Control(value) => {
            vacant.insert::<typed::Control>(value);
        }
        typed::Value::Data(value) => {
            vacant.insert::<typed::Data>(value);
        }
        typed::Value::Idle(value) => {
            vacant.insert::<typed::Idle>(value);
        }
    }
}
