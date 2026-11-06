use std::{cell::Cell, collections::VecDeque};

use o3::buffer::resident;
use sark_http::payload::owned;
pub mod common;
use sark_core::{
    duplex::{admit, input, machine},
    manifold::stream::Boundary,
};

use crate::common::duplex::*;

#[test]
fn orders_input_and_fin() {
    let events = Cell::new(Vec::new());
    let mut input = input::State::open(Session(&events), Buf(VecDeque::new()));

    (input.push(b"first", Boundary::More))
        .as_ref()
        .expect("test operation must succeed");
    (input.push(b"second", Boundary::End))
        .as_ref()
        .expect("test operation must succeed");

    for want in [
        input::Feed::Consumed(5),
        input::Feed::Consumed(6),
        input::Feed::Idle,
        input::Feed::Idle,
    ] {
        let turn = input.transact(Session::admit);
        let (step, used) = turn.into_parts();
        assert!(matches!(step, machine::Transaction::Committed { .. }));
        assert!(used == want);
    }

    assert_eq!(
        events.take().as_slice(),
        [
            Event::First,
            Event::Second,
            Event::Activate,
            Event::Finished
        ]
    );
    assert!(matches!(input.work(), input::Work::Idle));

    let fail = input
        .push(b"after-fin", Boundary::More)
        .expect_err("input after FIN must be rejected");
    match fail {
        input::PushError::Finished(owner) => assert_eq!(owner.as_ptr(), b"after-fin".as_ptr()),
        failure => assert!(
            matches!(failure, input::PushError::Finished(_)),
            "input after FIN must retain its owner"
        ),
    }
}

#[test]
fn cancellation_returns_queued_input() {
    let events = Cell::new(Vec::new());
    let mut input = input::State::open(Session(&events), Buf(VecDeque::new()));
    input.push(b"first", Boundary::More).expect("first input");
    input.push(b"second", Boundary::End).expect("queued input");
    let (step, used) = input.transact(Session::admit).into_parts();
    assert!(matches!(step, machine::Transaction::Committed { .. }));
    assert!(matches!(used, input::Feed::Consumed(5)));

    let mut buffer = input.into_buffer();
    assert_eq!(
        buffer.0.pop_front().expect("queued owner").as_ref(),
        b"second"
    );
    assert!(buffer.0.is_empty());
    assert_eq!(events.take().as_slice(), [Event::First]);
}

#[test]
fn finished_empty_input_becomes_idle() {
    let events = Cell::new(Vec::new());
    let mut input = input::State::finished(Idle(&events), Buf(VecDeque::new()));

    for after in [machine::After::Turn, machine::After::Idle] {
        assert!(matches!(input.work(), input::Work::Ready));
        let (step, used) = input.transact(Idle::admit).into_parts();
        assert!(matches!(
            step,
            machine::Transaction::Committed { after: actual, .. } if actual == after
        ));
        assert!(matches!(used, input::Feed::Idle));
    }

    assert!(matches!(input.work(), input::Work::Idle));
    assert_eq!(input.finish(), Err(machine::FinishError::Finished));
    let (step, used) = input.transact(Idle::admit).into_parts();
    assert!(matches!(step, machine::Transaction::Idle));
    assert!(matches!(used, input::Feed::Idle));
    assert_eq!(events.take().as_slice(), [Event::Activate, Event::Finished]);
}

#[test]
fn rejects_late_input() {
    let mut input = input::State::open(Terminal, Buf(VecDeque::new()));
    let step = input.transact(Terminal::admit).into_parts().0;
    assert!(matches!(
        step,
        machine::Transaction::Committed {
            after: machine::After::Finished,
            ..
        }
    ));

    let owner = b"late";
    let fail = input
        .push(owner, Boundary::More)
        .expect_err("late input must be rejected");
    match fail {
        input::PushError::Finished(fail) => assert_eq!(fail.as_ptr(), owner.as_ptr()),
        failure => assert!(
            matches!(failure, input::PushError::Finished(_)),
            "late input must retain its owner"
        ),
    }
}

#[test]
fn fin_preempts_reactivation_and_waits_for_admission() {
    let events = Cell::new(Vec::new());
    let mut machine = machine::Machine::new(Ending(&events));
    assert!(matches!(
        machine.transact(&mut [], &mut Vec::new(), |_, emit| Ending::admit(emit)),
        machine::Transaction::Committed { .. }
    ));
    machine.finish(()).expect("first input finish").0.commit();

    for pending in [true, true, false] {
        let turn = machine.transact(&mut [], &mut Vec::new(), |written, emit| {
            assert_eq!(written, 0);
            let (payload, end, after) = emit.into_parts();
            assert!(matches!(end, machine::End::FinishOutput));
            assert!(matches!(after, machine::After::Finished));
            match payload {
                admit::Payload::Present { output, permit } if pending => {
                    permit.pending::<(), ()>(output, ())
                }
                admit::Payload::Present { output, permit } => {
                    {
                        let output = owned::Segments::from(output);
                        assert_eq!(output.contiguous(), Some(b"end".as_slice()));
                    }
                    permit.commit(())
                }
                admit::Payload::Vacant(permit) => permit.reject(()),
            }
        });
        if pending {
            assert!(matches!(turn, machine::Transaction::Pending { .. }));
            assert!(matches!(machine.hold(), machine::Hold::Held));
            assert!(matches!(machine.progress(), machine::Progress::Turn));
        } else {
            assert!(matches!(
                turn,
                machine::Transaction::Committed {
                    after: machine::After::Finished,
                    ..
                }
            ));
            assert!(matches!(machine.hold(), machine::Hold::Released));
            assert!(matches!(machine.progress(), machine::Progress::Finished));
        }
    }
    assert_eq!(events.take().as_slice(), &[Event::Finished]);
    assert!(matches!(machine.input(), machine::Access::Blocked));
    assert!(matches!(
        machine.finish(()),
        Err((machine::FinishError::Closed, ()))
    ));
}

#[test]
fn late_input_finish_does_not_repeat_output_fin() {
    let calls = Cell::new(0);
    let mut machine = machine::Machine::new(Prefix {
        calls: &calls,
        written: 0,
        body: false,
    });
    for (finish, end, after) in [
        (false, machine::End::FinishOutput, machine::After::Idle),
        (true, machine::End::Open, machine::After::Finished),
    ] {
        if finish {
            machine.finish(()).expect("late input finish").0.commit();
        }
        let turn = machine.transact(&mut [], &mut Vec::new(), |_, emit| {
            let (payload, actual, _) = emit.into_parts();
            assert!(actual == end);
            match payload {
                admit::Payload::Vacant(permit) => permit.commit::<_, ()>(()),
                admit::Payload::Present { .. } => panic!("unexpected output"),
            }
        });
        assert!(matches!(
            turn,
            machine::Transaction::Committed { after: actual, .. } if actual == after
        ));
    }
    assert_eq!(calls.get(), 1);
}

#[test]
fn rejects_invalid_prefix_before_admission() {
    for (written, finished, expected) in [
        (
            2,
            false,
            machine::Violation::PrefixCapacity {
                written: 2,
                capacity: 1,
            },
        ),
        (1, true, machine::Violation::OutputAfterFinish),
    ] {
        let calls = Cell::new(0);
        let mut machine = machine::Machine::new(Prefix {
            calls: &calls,
            written,
            body: false,
        });
        if finished {
            assert!(matches!(
                machine.transact(&mut [], &mut Vec::new(), |_, emit| Prefix::admit(emit)),
                machine::Transaction::Committed { .. }
            ));
        }
        let machine::Access::Vacant(input) = machine.input() else {
            panic!("vacant input");
        };
        input.commit(resident::Shared::from(b"input".as_slice()).into());
        let turn = machine.transact::<(), ()>(&mut [0], &mut Vec::new(), |_, _| {
            panic!("invalid prefix admitted")
        });
        assert!(matches!(turn, machine::Transaction::Violation(actual) if actual == expected));
        assert_eq!(calls.get(), 1);
        assert!(matches!(machine.progress(), machine::Progress::Finished));
        assert!(matches!(
            machine.transact::<(), ()>(&mut [], &mut Vec::new(), |_, _| panic!("retired callback")),
            machine::Transaction::Idle
        ));
    }
}

#[test]
fn prefix_cannot_be_pending_or_blocked() {
    for body in [false, true] {
        let calls = Cell::new(0);
        let mut machine = machine::Machine::new(Prefix {
            calls: &calls,
            written: 1,
            body,
        });
        let machine::Access::Vacant(input) = machine.input() else {
            panic!("vacant input");
        };
        input.commit(resident::Shared::from(b"input".as_slice()).into());
        machine.finish(()).expect("input finish").0.commit();
        let mut output = [0];
        let turn = machine.transact(&mut output, &mut Vec::new(), |written, emit| {
            assert_eq!(written, 1);
            let (payload, end, _) = emit.into_parts();
            assert!(matches!(end, machine::End::FinishOutput));
            match payload {
                admit::Payload::Present { output, permit } => permit.pending::<(), ()>(output, ()),
                admit::Payload::Vacant(permit) => permit.blocked::<(), ()>(()),
            }
        });
        assert!(matches!(
            turn,
            machine::Transaction::Violation(machine::Violation::PrefixPending)
        ));
        assert_eq!(output, *b"p");
        assert_eq!(calls.get(), 1);
        assert!(matches!(machine.hold(), machine::Hold::Released));
        assert!(matches!(machine.progress(), machine::Progress::Finished));
        machine.retire();
        assert!(matches!(
            machine.transact::<(), ()>(&mut [], &mut Vec::new(), |_, _| panic!("retired callback")),
            machine::Transaction::Idle
        ));
    }
}
