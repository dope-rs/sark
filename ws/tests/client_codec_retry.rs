pub mod common;

use std::{convert::Infallible, pin::Pin};

use common::{
    app::borrowed::Borrowed, client::topology, protocol::TEXT_PREFIX, upgrade::client::RESPONSE,
};
use dope::{
    core::driver::life::recovery,
    manifold::client::session::{
        Session as _,
        codec::{
            self, Codec as _, Stateful as _,
            staging::{Buffered, Policy as _},
            storage::Slot,
        },
        response::Direct,
    },
};
use dope_test::{
    alloc::Tracker,
    harness::{observe::Trace, signals::Gate},
};
use o3::{
    alloc,
    base::cell::region::Token,
    buffer::{resident, storage::Shared},
};
use sark_ws::{
    client::{
        self, Fault, channel, mode,
        protocol::{Codec, Head, State},
    },
    inbound,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn retry_keeps_progress() {
    let events = Trace::new();
    let closed = Gate::new();
    let config = client::Config::new("localhost", "/").expect("configuration");
    topology::Plan::new("127.0.0.1:1".parse().expect("address"), config, 1, 1)
        .enter(
            |domain| {
                domain.run(
                    channel::App::<_, mode::Egress>::new(Borrowed {
                        events: &events,
                        closed: &closed,
                        message: &|_| {},
                    }),
                    |mut app| {
                        app.inspect(|runtime| {
                            let codec = runtime.manifold().get_ref().session().codec();
                            let limit = Buffered::limit(codec.staging().expect("staging"));
                            let mut domain = resident::Domain::new();
                            Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
                                let budget = domain
                                    .get_mut()
                                    .try_budget(1024, token.proof())
                                    .expect("resident budget");
                                let mut state = codec.state(budget);
                                for wire in [RESPONSE, b"\x81\x02ok", b"\x01\0", b"\x80\0"] {
                                    let mut source = codec.storage(
                                        Shared::try_copy(wire).expect("retry source"),
                                        &state,
                                    );
                                    for accept in [false, false, true] {
                                        let step: codec::Step<Direct> = codec.decode(
                                            &mut source,
                                            &mut state,
                                            &budget,
                                            limit,
                                            |preview| {
                                                assert!(matches!(preview, Ok(None)));
                                                if !accept {
                                                    return codec::Admission::RetryInput;
                                                }
                                                codec::Admission::Ready(
                                                    |head: Result<Head<'_>, Fault>, _: &mut State| {
                                                        assert!(head.is_ok());
                                                    },
                                                )
                                            },
                                        );
                                        assert_eq!(step.has_consumed(), accept);
                                        assert_eq!(
                                            Codec::source(&source).as_ref(),
                                            if accept { &[] } else { wire }
                                        );
                                    }
                                }
                                let mut source = codec.storage(
                                    Shared::try_copy(TEXT_PREFIX).expect("fragment source"),
                                    &state,
                                );
                                let decline = |preview: Result<
                                    Option<inbound::Reply<'_>>,
                                    &Fault,
                                >| match preview {
                                    Err(error) => {
                                        assert!(matches!(
                                            error,
                                            Fault::Frame(inbound::Fault::Allocation(
                                                alloc::Error::Exhausted
                                            ))
                                        ));
                                        codec::Admission::RetryInput
                                    }
                                    Ok(_) => codec::Admission::Ready(
                                        |head: Result<Head<'_>, Fault>, _: &mut State| {
                                            assert!(matches!(
                                                head,
                                                Err(Fault::Frame(inbound::Fault::Allocation(
                                                    alloc::Error::Exhausted
                                                )))
                                            ));
                                        },
                                    ),
                                };
                                let step: codec::Step<Direct> = Tracker::fail_after(0, || {
                                    codec.decode(&mut source, &mut state, &budget, limit, decline)
                                });
                                assert!(!step.has_consumed());
                                let step: codec::Step<Direct> =
                                    codec.decode(&mut source, &mut state, &budget, limit, decline);
                                assert!(!step.has_consumed());
                                assert_eq!(Codec::source(&source).as_ref(), TEXT_PREFIX);
                                let (next, result) = Slot::new((source, state)).run(|input| {
                                    codec.finish::<_, Direct, _>(
                                    input,
                                    &budget,
                                    limit,
                                    |preview| {
                                        assert!(matches!(
                                            preview,
                                            Err(Fault::Frame(inbound::Fault::Allocation(
                                                alloc::Error::Exhausted
                                            )))
                                        ));
                                        codec::Admission::Ready(
                                            |head: Result<Head<'_>, Fault>, _: &mut State| {
                                                assert!(matches!(
                                                    head,
                                                    Err(Fault::Frame(inbound::Fault::Allocation(
                                                        alloc::Error::Exhausted
                                                    )))
                                                ));
                                            },
                                        )
                                    },
                                    )
                                });
                                (source, state) = next.into_parts();
                                assert!(matches!(result, Err(Direct::Committed)));
                                assert_eq!(Codec::source(&source).as_ref(), TEXT_PREFIX);
                                source = codec.storage(
                                    Shared::try_copy(&TEXT_PREFIX[..2]).expect("frame tail"),
                                    &state,
                                );
                                for accept in [false, true] {
                                    let (next, result) = Slot::new((source, state)).run(|input| {
                                        codec.finish::<_, Direct, _>(
                                        input,
                                        &budget,
                                        limit,
                                        |preview| {
                                            assert!(matches!(
                                                preview,
                                                Err(Fault::FrameTail { actual: 2 })
                                            ));
                                            if !accept {
                                                return codec::Admission::RetryInput;
                                            }
                                            codec::Admission::Ready(
                                                |head: Result<Head<'_>, Fault>, _: &mut State| {
                                                    assert!(matches!(
                                                        head,
                                                        Err(Fault::FrameTail { actual: 2 })
                                                    ));
                                                },
                                            )
                                        },
                                        )
                                    });
                                    (source, state) = next.into_parts();
                                    assert!(matches!(
                                        result,
                                        Err(outcome) if matches!(outcome, Direct::Committed) == accept
                                    ));
                                    assert_eq!(Codec::source(&source).as_ref(), &TEXT_PREFIX[..2]);
                                }
                            });
                        });
                    },
                )
            },
            recovery::Policy::new(
                |_| {},
                |failure: recovery::Recovery<'_, '_>| -> Infallible {
                    panic!("{:?}", failure.cause())
                },
            ),
            |result| result.expect("client execution"),
        )
        .as_ref()
        .map_err(|error| error.kind())
        .expect("client setup");
}
