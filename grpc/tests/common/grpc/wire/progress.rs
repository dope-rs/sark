use std::cell::Cell;

use dope::core::random;
use o3::{
    alloc,
    buffer::{hold, pool, storage},
    depot::capacity,
};
use sark_core::manifold::{
    client::{Connection, domain, event, io},
    schedule::{self, data},
    stream::Boundary,
};
use sark_grpc::protocol::section;
use sark_h2::{
    ClientRole, client,
    conn::{self, config, machine},
    stream,
};
use sark_http::field;

type Native = machine::Connection<ClientRole, section::Plan>;
type NativePlan = client::Plan<section::Plan>;

#[derive(Clone, Copy)]
pub struct Plan<'a> {
    native: NativePlan,
    written: &'a [usize],
    attempts: &'a Cell<usize>,
    stale: bool,
}

impl<'a> Plan<'a> {
    pub fn new(
        written: &'a [usize],
        attempts: &'a Cell<usize>,
        stale: bool,
    ) -> Result<Self, config::Error> {
        Ok(Self {
            native: NativePlan::new(1)?,
            written,
            attempts,
            stale,
        })
    }

    fn bind(self, native: Native) -> Carrier<'a> {
        Carrier { native, plan: self }
    }
}

pub struct Carrier<'a> {
    native: Native,
    plan: Plan<'a>,
}

impl Carrier<'_> {
    fn written(&mut self) -> Option<usize> {
        let index = self.plan.attempts.get();
        self.plan.attempts.set(index + 1);
        self.plan.written.get(index).copied()
    }
}

impl Connection for Carrier<'_> {
    type Error = conn::Error;
    type Owner = storage::Shared;
    type Key = stream::Key;
    type Block = conn::DataBlock;
    type Message = <Native as Connection>::Message;
    type Fields = section::Owner;
    type Selection = ();
    type Data = <Native as Connection>::Data;
    type GoAway = <Native as Connection>::GoAway;

    fn start<S: field::Source>(
        &mut self,
        fields: S,
        boundary: Boundary,
    ) -> Result<Self::Key, Self::Error> {
        self.native.start(fields, boundary)
    }

    fn send(
        &mut self,
        key: Self::Key,
        prefix: &[u8],
        body: &[u8],
        offset: usize,
        boundary: Boundary,
    ) -> Result<io::Outcome<Self::Block>, Self::Error> {
        match self.written() {
            Some(count) => Ok(io::Outcome::Written(count)),
            None => self.native.send(key, prefix, body, offset, boundary),
        }
    }

    fn send_owned(
        &mut self,
        key: Self::Key,
        prefix: &[u8],
        body: &storage::Shared,
        offset: usize,
        boundary: Boundary,
    ) -> Result<io::Outcome<Self::Block>, Self::Error> {
        match self.written() {
            Some(count) => Ok(io::Outcome::Written(count)),
            None => self.native.send_owned(key, prefix, body, offset, boundary),
        }
    }

    fn reset(&mut self, key: Self::Key, reason: event::Reset) -> Result<(), Self::Key> {
        if self.plan.stale {
            self.native.reset(key, reason)?;
        }
        self.native.reset(key, reason)
    }

    fn cancel(&mut self, key: Self::Key, reason: event::Reset) {
        self.native.cancel(key, reason);
    }

    fn poll(&mut self) -> Option<event::Response<Self>> {
        use event::Response;
        Some(match self.native.poll()? {
            Response::Head {
                message,
                fields,
                selection: select,
                section,
            } => Response::Head {
                message,
                fields,
                selection: select,
                section,
            },
            Response::Data { message, data } => Response::Data { message, data },
            Response::Reset { stream, reason } => Response::Reset { stream, reason },
            Response::GoAway { boundary, shutdown } => Response::GoAway { boundary, shutdown },
        })
    }

    fn is_accepted(&self, bound: &Self::GoAway, key: Self::Key) -> bool {
        self.native.is_accepted(bound, key)
    }
}

impl io::Read for Carrier<'_> {
    fn has_event(&mut self) -> bool {
        io::Read::has_event(&mut self.native)
    }

    fn is_blocked(&mut self) -> bool {
        io::Read::is_blocked(&mut self.native)
    }

    fn ingest(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        io::Read::ingest(&mut self.native, bytes)
    }

    fn ingest_owned(&mut self, bytes: Self::Owner) -> Result<(), Self::Error> {
        io::Read::ingest_owned(&mut self.native, bytes)
    }

    fn ingest_from<I>(&mut self, source: &mut I) -> Result<(), Self::Error>
    where
        I: Iterator<Item = Self::Owner>,
    {
        io::Read::ingest_from(&mut self.native, source)
    }

    fn resume(&mut self) -> Result<(), Self::Error> {
        io::Read::resume(&mut self.native)
    }
}

impl io::Write for Carrier<'_> {
    type Prepared<'a>
        = <Native as io::Write>::Prepared<'a>
    where
        Self: 'a;

    fn outbound(&self) -> &[u8] {
        io::Write::outbound(&self.native)
    }

    fn outbound_len(&self) -> usize {
        io::Write::outbound_len(&self.native)
    }

    fn drain(&mut self, len: usize) {
        io::Write::drain(&mut self.native, len);
    }

    fn take(&mut self, prefix: &mut [u8]) -> Result<io::Output<Self::Owner>, io::Error> {
        io::Write::take(&mut self.native, prefix)
    }

    fn prepare(&mut self, prefix: pool::Cursor) -> Result<Self::Prepared<'_>, io::Reject> {
        io::Write::prepare(&mut self.native, prefix)
    }
}

impl<'a> domain::Plan<storage::Shared> for Plan<'a> {
    type Key = stream::Key;
    type Block = conn::DataBlock;
    type Permit = <NativePlan as domain::Plan<storage::Shared>>::Permit;
    type Fields = section::Owner;
    type Selection = ();
    type Connection = Carrier<'a>;
    type Domain<T> = Domain<T>;
    type Build = conn::Build;
    type Storage = alloc::Error;

    const RETENTION: hold::Policy = <NativePlan as domain::Plan<storage::Shared>>::RETENTION;
    const MIN_PREFIX: usize = <NativePlan as domain::Plan<storage::Shared>>::MIN_PREFIX;

    fn open(self) -> Result<Self::Connection, Self::Build> {
        Ok(
            self.bind(<NativePlan as domain::Plan<storage::Shared>>::open(
                self.native,
            )?),
        )
    }

    fn open_seeded(self, seed: &random::Seed) -> Result<Self::Connection, Self::Build> {
        Ok(
            self.bind(<NativePlan as domain::Plan<storage::Shared>>::open_seeded(
                self.native,
                seed,
            )?),
        )
    }

    fn domain<T>(cap: capacity::Count) -> Result<Self::Domain<T>, Self::Storage> {
        Ok(Domain(schedule::Runtime::try_bounded(cap, cap)?))
    }
}

pub struct Domain<T>(schedule::Runtime<stream::Key, T, conn::DataBlock>);

impl<'c, T> domain::Domain<Carrier<'c>, T> for Domain<T> {
    fn capacity(&self) -> usize {
        self.0.capacity()
    }

    fn key_at(&self, index: usize) -> Option<stream::Key> {
        self.0.key_at(index)
    }

    fn get(&self, key: stream::Key) -> Option<&T> {
        self.0.get(key)
    }

    fn get_mut(&mut self, key: stream::Key) -> Option<&mut T> {
        self.0.get_mut(key)
    }

    fn remove(&mut self, key: stream::Key) -> Option<T> {
        self.0.remove(key)
    }

    fn has_work(&self) -> bool {
        self.0.has_work()
    }

    fn try_insert(
        &mut self,
        key: stream::Key,
        make: impl FnOnce() -> T,
    ) -> Result<(), schedule::Insert<stream::Key, ()>> {
        self.0.try_store(key, make)
    }

    fn transaction<P, R, E>(
        &mut self,
        key: stream::Key,
        check: impl FnOnce(&T) -> Result<(domain::Activation<conn::DataBlock>, P), E>,
        commit: impl FnOnce(&mut T, P) -> R,
    ) -> Result<R, schedule::Error<E>> {
        self.0.transaction(key, |value| {
            let (next, plan) = check(value)?;
            let next = match next {
                domain::Activation::Keep => schedule::Activation::Keep,
                domain::Activation::Data(wait) => schedule::Activation::data(wait),
            };
            Ok((next, commit(value, plan)))
        })
    }

    fn resume(&mut self, conn: &mut Carrier<'c>, egress: schedule::Egress) {
        self.0.resume(|| conn.native.flow().poll_wake(), egress);
    }

    fn drive<D: domain::Driver<Carrier<'c>, T>>(
        &mut self,
        conn: &mut Carrier<'c>,
        driver: &mut D,
    ) -> Result<schedule::Report, domain::Failure<D::Error, stream::Key>> {
        let mut drive = Drive {
            conn,
            driver,
            failure: None,
        };
        let report = self.0.drive_data(&mut drive);
        match drive.failure {
            Some(error) => Err(error),
            None => Ok(report),
        }
    }
}

struct Drive<'a, 'c, D, E> {
    conn: &'a mut Carrier<'c>,
    driver: &'a mut D,
    failure: Option<domain::Failure<E, stream::Key>>,
}

impl<'c, T, D: domain::Driver<Carrier<'c>, T>> data::Driver<T> for Drive<'_, 'c, D, D::Error> {
    type Key = stream::Key;
    type Block = conn::DataBlock;
    type Error = D::Error;

    fn can_continue(&self) -> bool {
        self.failure.is_none()
    }

    fn turn(
        &mut self,
        key: Self::Key,
        value: &mut T,
    ) -> Result<schedule::Turn<Self::Block>, Self::Error> {
        self.driver.data(self.conn, key, value)
    }

    fn settle(
        &mut self,
        key: Self::Key,
        value: &mut T,
        term: schedule::Terminal<Self::Error>,
    ) -> data::Settlement {
        match self.driver.settle(self.conn, key, value, term) {
            domain::Settlement::Keep => data::Settlement::Keep,
            domain::Settlement::Remove => data::Settlement::Remove,
            domain::Settlement::Failed {
                disposition: place,
                error,
            } => {
                self.failure = Some(error);
                match place {
                    domain::Disposition::Keep => data::Settlement::Keep,
                    domain::Disposition::Remove => data::Settlement::Remove,
                }
            }
        }
    }

    fn remove(&mut self, value: T) {
        self.driver.removed(value);
    }
}
