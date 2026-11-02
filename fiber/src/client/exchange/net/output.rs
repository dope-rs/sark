use std::mem;

use dope::{
    core::{
        driver,
        driver::route::{Identity, KeyTag},
    },
    manifold::client::{
        close,
        session::{response, turns},
    },
};
use dope_fiber::reply::usage;
use o3::buffer::pool;
use protocol::codec::{self, output};
use sark_client::{capacity, protocol};

use crate::client::{
    channel,
    conn::{Kind, route::Route},
    exchange::{
        Reuse,
        net::{self, pools::entries},
        request, reuse,
    },
};

/// Prepared output retaining the connection and owner used by its decoder.
#[doc(hidden)]
#[repr(C)]
pub struct Output<'s, 'd, ID: Identity, P: Kind<'d, ID>>
where
    P::Tag: Copy,
{
    owner: &'s P,
    current: entries::Bound<'s, 'd, ID, P::Tag, P::State, P::Position>,
    output: output::Output<'d>,
}

impl<'s, 'd, ID: Identity, P: Kind<'d, ID>> Output<'s, 'd, ID, P>
where
    P::Tag: Copy,
{
    pub(in crate::client) fn prepare<'f: 's, 'e: 's, 'b: 'e, 'c: 'b>(
        root: &'s P,
        parsed: Result<output::Parsed<'d>, protocol::Error>,
        parser: &mut codec::State<'d>,
        ctx: &mut turns::Context<'_, '_, 'd, Reuse, pool::Cursor, ID>,
    ) -> Option<Self>
    where
        'd: 'c + 'e + 'f,
        P: Route<'f, 'e, 'b, 'c, 'd, ID>,
    {
        let protocol = root.protocol();
        let pool = &protocol.transport.pool;
        let current = pool.directory.try_current(ctx.conn_id);
        let (current, output) = match (parsed, current) {
            (Ok(parsed), Ok(current)) => {
                let output = protocol.codec.admit(parser, parsed, || {
                    pool.responses
                        .lane(current.position())
                        .control()
                        .front_tag(ctx.region())
                        .copied()
                        .map(request::Kind::transition)
                });
                (current, output)
            }
            (Err(error), Ok(current)) => {
                ctx.request(close::Directive::reconnect(close::Reason::Protocol));
                (current, codec::Codec::reject(parser, error))
            }
            (parsed, Err(cause)) => {
                if parsed.is_err() {
                    ctx.request(close::Directive::reconnect(close::Reason::Protocol));
                }
                ctx.request(cause.close());
                return None;
            }
        };
        Some(Self {
            owner: root,
            current,
            output,
        })
    }

    pub(in crate::client) fn publish<'f: 's, 'e: 's, 'b: 'e, 'c: 'b>(
        prepared: response::Committed<Self>,
        driver: &mut driver::Context<'_, 'd>,
        reuse: &mut Reuse,
    ) -> Option<close::Directive>
    where
        'd: 'c + 'e + 'f,
        P: Route<'f, 'e, 'b, 'c, 'd, ID>,
    {
        let Self {
            owner,
            current,
            output,
        } = prepared.into_inner();
        let owner = &owner.protocol().transport;
        let end = output.completion();
        let control = output.control();
        match control.map(|control| reuse.observe(owner.idle_policy, control)) {
            Some(reuse::Decision::Retire) => owner.pool.seal(driver.region_token(), &current),
            Some(reuse::Decision::Reuse) | None => {}
        }
        let mut cause = None;
        for event in output.into_events() {
            if event.is_err() {
                reuse.reject();
                owner.pool.seal(driver.region_token(), &current);
            }
            let outcome = net::Outcome::from_result(event);
            let Err(rejected) = owner
                .pool
                .responses
                .lane(current.position())
                .select(driver.region_token())
                .try_push(outcome)
            else {
                continue;
            };
            reuse.reject();
            owner.pool.seal(driver.region_token(), &current);
            cause = Some(match rejected {
                usage::InsertError::Backpressure { cause, .. } => {
                    capacity::Error::ResponseBackpressure(cause)
                }
                usage::InsertError::Capacity { cause, .. } => {
                    capacity::Error::ResponseCapacity(cause)
                }
                usage::InsertError::Front { front, .. } => capacity::Error::ResponseFront(front),
            });
            break;
        }
        if cause.is_none() {
            match end {
                output::Completion::Pending => return None,
                output::Completion::Complete => {}
            }
        }
        let budget = reuse.finish();
        let retire = matches!(budget, reuse::Budget::Retired);
        if let Some(tag) = owner.pool.complete(driver, &current, budget, cause) {
            owner.waiters.wake(tag);
        }
        retire.then_some(close::Directive::reconnect(close::Reason::Local(
            close::LocalOrigin::ScheduledRotation,
        )))
    }
}

const _: () = {
    assert!(
        mem::size_of::<
            Output<
                'static,
                'static,
                KeyTag<0>,
                channel::Port<'static, 'static, 'static, 'static, 'static, 'static>,
            >,
        >() == mem::size_of::<&channel::Port<'static, 'static, 'static, 'static, 'static, 'static>>(
        ) + mem::size_of::<entries::Positioned<'static, 'static, 'static, KeyTag<0>, (), ()>>(
        ) + mem::size_of::<output::Output<'static>>()
    );
    assert!(
        mem::size_of::<
            response::Commit<
                Output<
                    'static,
                    'static,
                    KeyTag<0>,
                    channel::Port<'static, 'static, 'static, 'static, 'static, 'static>,
                >,
            >,
        >() == 0
    );
};
