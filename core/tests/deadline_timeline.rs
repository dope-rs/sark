pub mod common;
use std::convert::Infallible;

use common::deadline::{Generation, Range};
use o3::{
    base::time::Positive,
    depot::{
        arena::{hash, hash::runtime},
        capacity::Count,
    },
};
use sark_core::deadline::{self, config::Config, timeline};

fn add(now: u64, window: Positive) -> u64 {
    now + window.get().as_secs()
}

#[test]
fn exchange_lifetime() {
    let secs_100 = Positive::from_secs::<100>();
    let secs_10 = Positive::from_secs::<10>();
    let secs_5 = Positive::from_secs::<5>();
    let secs_20 = Positive::from_secs::<20>();
    let secs_3 = Positive::from_secs::<3>();
    let config = Config::new(deadline::Phase::Total, secs_100)
        .with(deadline::Phase::Head, secs_10)
        .with(deadline::Phase::BodyIdle, secs_5)
        .with(deadline::Phase::Handler, secs_20)
        .with(deadline::Phase::ResponseAdmission, secs_3);
    let mut timeline = timeline::Timeline::EMPTY;

    let opened = timeline.transition(config, timeline::Transition::AwaitingHead, 0, add);
    assert!((opened) == (deadline::Mutation::Earliest));
    assert!(
        (timeline.next())
            == (Some(timeline::Expiry {
                at: 10,
                phase: deadline::Phase::Head,
            }))
    );

    timeline.transition(config, timeline::Transition::HeadAdmitted, 1, add);
    assert!(
        (timeline.next())
            == (Some(timeline::Expiry {
                at: 6,
                phase: deadline::Phase::BodyIdle,
            }))
    );
    assert!(
        (timeline.transition(config, timeline::Transition::BodyProgress, 5, add,))
            == (deadline::Mutation::Contents)
    );
    assert!(timeline.pop_due(6).is_none());
    assert!(
        (timeline.next())
            == (Some(timeline::Expiry {
                at: 10,
                phase: deadline::Phase::BodyIdle,
            }))
    );

    timeline.transition(config, timeline::Transition::HandlerStarted, 6, add);
    timeline.transition(config, timeline::Transition::InputFinished, 7, add);
    timeline.transition(config, timeline::Transition::ResponseReady, 8, add);
    assert!(
        (timeline.next())
            == (Some(timeline::Expiry {
                at: 11,
                phase: deadline::Phase::ResponseAdmission,
            }))
    );
    timeline.transition(config, timeline::Transition::ResponseCommitted, 9, add);
    assert!(
        (timeline.next())
            == (Some(timeline::Expiry {
                at: 100,
                phase: deadline::Phase::Total,
            }))
    );
    timeline.transition(config, timeline::Transition::Retired, 9, add);
    assert!(timeline.is_empty());

    timeline.transition(config, timeline::Transition::AwaitingHead, 10, add);
    timeline.transition(config, timeline::Transition::InputAdmitted, 11, add);
    assert!(
        (timeline.next())
            == (Some(timeline::Expiry {
                at: 110,
                phase: deadline::Phase::Total,
            }))
    );
}

#[test]
fn head_replaces_progress() {
    let config = Config::new(deadline::Phase::BodyIdle, Positive::from_secs::<10>())
        .with(deadline::Phase::Head, Positive::from_secs::<1>());
    let mut timeline = timeline::Timeline::EMPTY;
    timeline.transition(config, timeline::Transition::HeadAdmitted, 0, add);
    timeline.transition(config, timeline::Transition::BodyProgress, 1, add);
    timeline.transition(config, timeline::Transition::AwaitingHead, 2, add);
    assert!(
        timeline.pop_due(3)
            == Some(timeline::Expiry {
                at: 3,
                phase: deadline::Phase::Head,
            })
    );
    assert!(timeline.is_empty());
}

#[test]
fn total_precedes_phase() {
    let secs_5 = Positive::from_secs::<5>();
    let config = Config::new(deadline::Phase::Total, secs_5)
        .with(deadline::Phase::Head, secs_5)
        .with(deadline::Phase::Handler, secs_5);
    let mut timeline = timeline::Timeline::EMPTY;
    timeline.transition(config, timeline::Transition::AwaitingHead, 0, add);
    timeline.transition(config, timeline::Transition::HandlerStarted, 0, add);

    assert!(
        (timeline.pop_due(5))
            == (Some(timeline::Expiry {
                at: 5,
                phase: deadline::Phase::Total,
            }))
    );
    assert!(
        (timeline.pop_due(5))
            == (Some(timeline::Expiry {
                at: 5,
                phase: deadline::Phase::Head,
            }))
    );
    assert!(
        (timeline.pop_due(5))
            == (Some(timeline::Expiry {
                at: 5,
                phase: deadline::Phase::Handler,
            }))
    );
}

#[test]
fn resident_generation() {
    let plan = (hash::Plan::new(1)).expect("the fixture capacity must fit");
    let mut registry =
        (runtime::Registry::<u64, Generation>::try_new(plan)).expect("registry allocation");
    let agenda = deadline::Agenda::<_, _>::try_new(Count::new(1));
    let mut agenda = agenda.expect("agenda allocation");
    let first = (registry.insert(7)).expect("first generation insertion");
    assert!(
        deadline::Book::schedule(
            &mut agenda,
            first,
            deadline::Scheduled::expire(11_u64, deadline::Phase::Head),
        ) == Ok(())
    );
    assert_eq!(registry.entry(first).map(|entry| entry.remove()), Some(7));

    let next = (registry.insert(9)).expect("next generation insertion");
    assert!(
        deadline::Book::schedule(
            &mut agenda,
            next,
            deadline::Scheduled::expire(13_u64, deadline::Phase::Head),
        ) == Err(deadline::Error::Conflict {
            requested: next,
            current: first,
        })
    );
    assert!(
        deadline::Book::pop_due(&mut agenda, 11)
            == Some(deadline::Expired {
                key: first,
                scheduled: deadline::Scheduled::expire(11, deadline::Phase::Head),
            })
    );
}

#[test]
fn range_error_keeps_domain() {
    let plan = (hash::Plan::new(2)).expect("the fixture capacity must fit");
    let mut registry =
        (runtime::Registry::<u64, Range>::try_new(plan)).expect("registry allocation");
    let first = (registry.insert(7)).expect("first insertion");
    let outside = (registry.insert(9)).expect("second insertion");
    assert_eq!(first.index(), 0);
    assert_eq!(outside.index(), 1);

    let item = deadline::Scheduled::expire(11_u64, deadline::Phase::Head);
    let error = deadline::Error::OutOfRange {
        key: outside,
        capacity: 1,
    };
    let exact = deadline::Exact::try_new(Count::new(1));
    let mut exact = exact.expect("exact allocation");
    let agenda = deadline::Agenda::<_, _>::try_new(Count::new(1));
    let mut agenda = agenda.expect("agenda allocation");
    assert!(deadline::Book::schedule(&mut exact, outside, item) == Err(error));
    assert!(deadline::Book::schedule(&mut agenda, outside, item) == Err(error));
}

#[test]
fn replace_preserves_lanes() {
    let plan = hash::Plan::new(1).expect("fixture capacity");
    let mut registry =
        runtime::Registry::<u64, Generation>::try_new(plan).expect("registry allocation");
    let key = registry.insert(7).expect("first insertion");
    let mut agenda =
        deadline::Agenda::<_, _, Infallible>::try_new(Count::new(1)).expect("agenda allocation");
    for (previous, phase) in [
        (deadline::Phase::Head, deadline::Phase::BodyIdle),
        (deadline::Phase::Handler, deadline::Phase::ResponseAdmission),
    ] {
        let old = deadline::Scheduled::expire(10_u64, previous);
        let total = deadline::Scheduled::expire(15, deadline::Phase::Total);
        let next = deadline::Scheduled::expire(20, phase);
        assert!(deadline::Book::schedule(&mut agenda, key, old).is_ok());
        assert!(deadline::Book::schedule(&mut agenda, key, total).is_ok());
        assert!(deadline::Book::schedule(&mut agenda, key, next) == Ok(()));
        assert!(deadline::Book::pop_due(&mut agenda, 10).is_none());
        for item in [total, next] {
            assert!(
                deadline::Book::pop_due(&mut agenda, item.at)
                    == Some(deadline::Expired {
                        key,
                        scheduled: item
                    })
            );
        }
        assert!(deadline::Book::is_empty(&agenda));
    }
}

#[test]
fn wake_follows_expiry() {
    let plan = hash::Plan::new(1).expect("fixture capacity");
    let mut registry =
        runtime::Registry::<u64, Generation>::try_new(plan).expect("registry allocation");
    let key = registry.insert(7).expect("first insertion");
    let mut agenda = deadline::Agenda::try_new(Count::new(1)).expect("agenda allocation");
    let items = [
        deadline::Scheduled::expire(10_u64, deadline::Phase::Total),
        deadline::Scheduled::expire(10, deadline::Phase::Handler),
        deadline::Scheduled::wake(10, deadline::WakeReason::DeferredOutput),
    ];
    for item in items.into_iter().rev() {
        assert!(deadline::Book::schedule(&mut agenda, key, item).is_ok());
    }
    for item in items {
        assert!(
            deadline::Book::pop_due(&mut agenda, 10)
                == Some(deadline::Expired {
                    key,
                    scheduled: item
                })
        );
    }
}

#[test]
fn poll_bounds_renewals() {
    use deadline::{Book, Poll};

    let plan = hash::Plan::new(3).expect("fixture capacity");
    let mut registry =
        runtime::Registry::<u64, Generation>::try_new(plan).expect("registry allocation");
    let keys = [
        registry.insert(1).expect("first insertion"),
        registry.insert(2).expect("second insertion"),
    ];
    let due = registry.insert(3).expect("third insertion");
    let mut agenda = deadline::Agenda::<_, _>::try_new(Count::new(3)).expect("agenda allocation");
    let config = Config::new(deadline::Phase::BodyIdle, Positive::from_secs::<5>());
    for key in keys {
        assert!(
            agenda
                .transition(key, config, timeline::Transition::HeadAdmitted, 0, add)
                .is_ok()
        );
        assert!(
            agenda
                .transition(key, config, timeline::Transition::BodyProgress, 3, add)
                .is_ok()
        );
    }
    let total = deadline::Scheduled::expire(6, deadline::Phase::Total);
    assert!(agenda.schedule(due, total).is_ok());

    assert!(agenda.poll_due(6) == Poll::Renewed);
    assert!(agenda.peek().is_some_and(|(_, item)| item.at == 5));
    assert!(agenda.poll_due(6) == Poll::Renewed);
    assert!(
        agenda.poll_due(6)
            == Poll::Expired(deadline::Expired {
                key: due,
                scheduled: total,
            })
    );
    assert!(agenda.poll_due(6) == Poll::Idle);

    let mut left = keys.to_vec();
    for _ in keys {
        match agenda.poll_due(8) {
            Poll::Expired(expired) => {
                let len = left.len();
                left.retain(|key| *key != expired.key);
                assert_eq!(left.len() + 1, len);
            }
            actual => assert!(matches!(actual, Poll::Expired(_))),
        }
    }
    assert!(agenda.is_empty());
}
