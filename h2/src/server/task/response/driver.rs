use std::{
    convert::{Infallible, identity},
    ops, pin,
};

use dope::{
    core::driver::{route::Identity, schedule::app},
    manifold::network::listener::conn::Id,
};
use o3::buffer::{hold, resident};
use sark_core::{
    life::{self, exchange, owners},
    manifold::{schedule, schedule::typed, stream::Boundary, task::app::Runtime},
};
use sark_http::owner::{self, cause};

use crate::{
    conn::{self, machine::Connection},
    frame::numbers,
    server::{
        self,
        response::{Handoff, Pending, Status},
        state::{self, exchange::Call},
        task::{
            response::{Count, Response, emission, opening},
            *,
        },
    },
    stream,
};

pub(in crate::server::task) struct Driver<'a, 'd, ID: Identity, R: Runtime<'d>, M> {
    pub(in crate::server::task) router: pin::Pin<&'a mut R>,
    pub(in crate::server::task) connection_id: Id<'d, ID>,
    pub(in crate::server::task) connection:
        &'a mut Connection<crate::ServerRole, R::Plan, resident::Shared<'d>>,
    pub(in crate::server::task) limits: server::Config,
    pub(in crate::server::task) work: app::Work<'a, 'd>,
    pub(in crate::server::task) mode: &'a mut M,
}

impl<'d, ID: Identity + 'd, R, M> Driver<'_, 'd, ID, R, M>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    M: state::Transitions<Id<'d, ID>>,
    M::Retention: Handoff,
{
    fn drive_body(
        &mut self,
        stream: stream::Key,
        pending: &mut Pending<'d>,
    ) -> Result<schedule::Turn<conn::DataBlock>, numbers::ErrorCode> {
        let mut owner = owners::Sink::new(&mut *self.mode, self.connection_id, identity);
        pending
            .turn_seen(self.connection, stream, &mut owner)
            .map_err(conn::Error::code)
    }

    pub(super) fn acquire_out(&mut self, stream: stream::Key, written: usize, held: usize) {
        if M::ENABLED && written != 0 {
            owners::Projection::new(self.mode).apply(owner::Effect::new(
                identify(self.connection_id, stream),
                hold::Charge::from_owner(written, held),
                owner::Change::StagingAcquired(cause::Acquire::StreamChunk),
            ));
        }
    }

    fn removed(
        &mut self,
        stream: stream::Key,
        error: numbers::ErrorCode,
    ) -> typed::Control<Reply<'d, R>, Data<'d, R>, Infallible, conn::DataBlock, numbers::ErrorCode>
    {
        self.reject(identify(self.connection_id, stream), error);
        typed::Control::Remove
    }

    fn cancel(
        &mut self,
        exchange: exchange::Id<Id<'d, ID>, stream::Key>,
        error: numbers::ErrorCode,
    ) {
        let write = self.connection.streams().retire(exchange.stream(), error);
        if let Some(write) = write {
            release_retired(
                self.mode,
                exchange.connection(),
                write,
                cause::Staging::Reset,
            );
        }
    }

    fn reject(
        &mut self,
        exchange: exchange::Id<Id<'d, ID>, stream::Key>,
        error: numbers::ErrorCode,
    ) {
        self.cancel(exchange, error);
        exchange.retire(self.mode, life::Outcome::Rejected);
    }
}

impl<'a, 'd, ID: Identity + 'd, R, M> typed::Settle<Reply<'d, R>, Data<'d, R>, Infallible>
    for Driver<'a, 'd, ID, R, M>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    M: state::Transitions<Id<'d, ID>>,
    M::Retention: Handoff,
{
    type Key = stream::Key;
    type Error = numbers::ErrorCode;

    fn settle(
        &mut self,
        stream: Self::Key,
        response: Resident<'d, R>,
        terminal: schedule::Terminal<Self::Error>,
    ) -> schedule::Settlement<Resident<'d, R>> {
        if matches!(terminal, schedule::Terminal::Idle)
            && matches!(&response, Value::Data(response::Data::Duplex { .. }))
        {
            return schedule::Settlement::Keep(response);
        }
        let exchange = identify(self.connection_id, stream);
        match terminal {
            schedule::Terminal::Failed(error) => match response {
                Value::Data(response::Data::Body { pending, .. }) => {
                    release_pending(self.mode, exchange, pending, cause::Staging::Reset);
                    self.reject(exchange, error);
                }
                Value::Control(Response::Encoded(receipt)) => {
                    self.reject(exchange, error);
                    drop(receipt);
                }
                Value::Control(Response::Head { head, .. } | Response::DuplexHead { head, .. }) => {
                    head.discard(self.connection);
                    self.reject(exchange, error);
                }
                _ => self.reject(exchange, error),
            },
            schedule::Terminal::Complete => match response {
                Value::Data(response::Data::Body { outcome, .. }) => {
                    exchange.retire(self.mode, outcome);
                }
                Value::Data(response::Data::Duplex { .. }) => {
                    exchange.retire(self.mode, life::Outcome::Completed);
                }
                _ => {}
            },
            schedule::Terminal::Idle => {}
        }
        schedule::Settlement::Remove
    }
}

impl<'a, 'd, ID: Identity + 'd, R, M> typed::Driver<Reply<'d, R>, Data<'d, R>, Infallible>
    for Driver<'a, 'd, ID, R, M>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    M: state::Transitions<Id<'d, ID>>,
    M::Retention: Handoff,
{
    type Block = conn::DataBlock;
    type Permit = app::Permit<'a, 'd>;

    fn permit(&mut self) -> Option<Self::Permit> {
        if self.connection.output().len() >= self.limits.max_outbound_bytes {
            return None;
        }
        self.work.permit()
    }

    fn control(
        &mut self,
        permit: Self::Permit,
        stream: Self::Key,
        response: Reply<'d, R>,
    ) -> typed::Control<Reply<'d, R>, Data<'d, R>, Infallible, Self::Block, Self::Error> {
        let driver = self;
        permit.run(|| {
            let exchange = identify(driver.connection_id, stream);
            let fresh = Count::Pending;
            let (driven, outcome, count) = match response {
                work @ (Response::Status(_)
                | Response::Sync(_)
                | Response::Completion(_)
                | Response::DuplexOpen { .. })
                    if !driver.connection.streams().headers().is_ready() =>
                {
                    return typed::Control::Control {
                        value: work,
                        turn: schedule::Turn::Blocked(conn::DataBlock::Egress),
                    };
                }
                Response::Status(status) => {
                    let head =
                        match Status::new(status).prepare(driver.connection, stream, Boundary::End)
                        {
                            Ok(head) => head,
                            Err(error) if error.is_pressure() => {
                                return typed::Control::Control {
                                    value: Response::Status(status),
                                    turn: schedule::Turn::Blocked(conn::DataBlock::Egress),
                                };
                            }
                            Err(error) => {
                                return typed::Control::Failed {
                                    value: Value::Control(Response::Status(status)),
                                    error: error.code(),
                                };
                            }
                        };
                    let driven = match head.drive(driver.connection) {
                        Ok(driven) => driven,
                        Err(error) => {
                            return driver.removed(stream, error.code());
                        }
                    };
                    (driven, life::Outcome::Rejected, fresh)
                }
                Response::Sync(request) => {
                    let result = Call::new(request).try_dispatch::<R, M::Retention, _>(
                        driver.router.as_ref().get_ref(),
                        driver.connection,
                        stream,
                    );
                    match result {
                        Ok(driven) => (driven, life::Outcome::Completed, fresh),
                        Err((error, failed)) => return driver.removed(failed, error),
                    }
                }
                Response::Completion(done) => {
                    match (emission::Emission { driver }).complete(stream, done) {
                        Ok((driven, outcome)) => (driven, outcome, fresh),
                        Err((error, failed)) => {
                            return driver.removed(failed, error.code());
                        }
                    }
                }
                Response::Head {
                    head,
                    outcome,
                    count,
                } => match head.drive(driver.connection) {
                    Err(error) => {
                        return driver.removed(stream, error.code());
                    }
                    Ok(driven) => (driven, outcome, count),
                },
                Response::Encoded(receipt) => (
                    ops::ControlFlow::Continue(receipt),
                    life::Outcome::Rejected,
                    Count::Settled,
                ),
                Response::DuplexOpen { request } => {
                    return match (opening::Opening { driver }).open(stream, request) {
                        Ok(control) => control,
                        Err(error) => driver.removed(stream, error),
                    };
                }
                Response::DuplexHead { head, input } => {
                    return match (opening::Opening { driver }).resume(stream, head, input) {
                        Ok(control) => control,
                        Err(error) => driver.removed(stream, error),
                    };
                }
            };
            let receipt = match driven {
                ops::ControlFlow::Continue(receipt) => receipt,
                ops::ControlFlow::Break(head) => {
                    return typed::Control::Control {
                        value: Response::Head {
                            head,
                            outcome,
                            count,
                        },
                        turn: schedule::Turn::Blocked(conn::DataBlock::Egress),
                    };
                }
            };
            let (pending, linked, copied) = receipt.into_parts();

            if let Some(copied) = copied {
                driver.mode.observe(life::Event::ResponseBodyCopied {
                    exchange,
                    read: copied.read,
                    written: copied.written,
                    cause: copied.cause,
                });
            }
            if M::ENABLED {
                let (bytes, charge) =
                    pending
                        .as_ref()
                        .map_or((0, hold::Charge::default()), |pending| {
                            let charge = pending.retention();
                            (charge.logical_bytes, charge)
                        });
                driver.mode.observe(life::Event::ResponseCommitted {
                    exchange,
                    body_bytes: bytes,
                    charge,
                    linked,
                });
            } else if M::EXCHANGE_DEADLINES {
                driver.mode.observe(life::Event::ResponseCommitted {
                    exchange,
                    body_bytes: 0,
                    charge: hold::Charge::default(),
                    linked: hold::Charge::default(),
                });
            }
            match pending {
                Some(mut pending) => {
                    let turn = driver.drive_body(stream, &mut pending);
                    match turn {
                        Ok(turn) => typed::Control::Fused {
                            value: response::Data::Body { pending, outcome },
                            turn,
                        },
                        Err(error) => typed::Control::Failed {
                            value: Value::Data(response::Data::Body { pending, outcome }),
                            error,
                        },
                    }
                }
                None => {
                    exchange.retire(driver.mode, outcome);
                    match count {
                        Count::Pending => typed::Control::Complete,
                        Count::Settled => typed::Control::Remove,
                    }
                }
            }
        })
    }

    fn data(
        &mut self,
        permit: Self::Permit,
        stream: stream::Key,
        response: &mut Data<'d, R>,
    ) -> Result<schedule::Turn<Self::Block>, Self::Error> {
        permit.run(|| match response {
            response::Data::Body { pending, .. } => self.drive_body(stream, pending),
            response::Data::Duplex { input } => {
                emission::Emission { driver: self }.drive(stream, input)
            }
        })
    }
}
