use std::marker;

use sark_core::{
    life::session,
    manifold::{
        client::domain,
        schedule::{self, data},
    },
};
use sark_http::field::metadata::section::rules;

use crate::{
    ClientRole,
    conn::{self, machine::Connection},
    stream,
};

/// H2-native resident call domain.
#[repr(transparent)]
pub struct Domain<T, F = rules::RetainRaw> {
    pub(super) inner: schedule::Runtime<stream::Key, T, conn::DataBlock>,
    pub(super) marker: marker::PhantomData<fn() -> F>,
}

struct Drive<'a, F, C, D, E>
where
    F: rules::Policy,
    C: session::Storage,
{
    connection: &'a mut Connection<ClientRole, F, C>,
    driver: &'a mut D,
    failure: Option<domain::Failure<E, stream::Key>>,
}

impl<F, C, T, D> data::Driver<T> for Drive<'_, F, C, D, D::Error>
where
    F: rules::Policy,
    C: session::Storage,
    D: domain::Driver<Connection<ClientRole, F, C>, T>,
{
    type Key = stream::Key;
    type Block = conn::DataBlock;
    type Error = D::Error;

    fn can_continue(&self) -> bool {
        self.failure.is_none()
    }

    fn turn(
        &mut self,
        key: stream::Key,
        value: &mut T,
    ) -> Result<schedule::Turn<Self::Block>, Self::Error> {
        self.driver.data(self.connection, key, value)
    }

    fn settle(
        &mut self,
        key: stream::Key,
        value: &mut T,
        term: schedule::Terminal<Self::Error>,
    ) -> data::Settlement {
        match self.driver.settle(self.connection, key, value, term) {
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

impl<F, C, T> domain::Domain<Connection<ClientRole, F, C>, T> for Domain<T, F>
where
    F: rules::Policy,
    C: session::Storage,
{
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    fn key_at(&self, index: usize) -> Option<stream::Key> {
        self.inner.key_at(index)
    }

    fn get(&self, stream: stream::Key) -> Option<&T> {
        self.inner.get(stream)
    }

    fn get_mut(&mut self, stream: stream::Key) -> Option<&mut T> {
        self.inner.get_mut(stream)
    }

    fn remove(&mut self, stream: stream::Key) -> Option<T> {
        self.inner.remove(stream)
    }

    fn has_work(&self) -> bool {
        self.inner.has_work()
    }

    fn try_insert(
        &mut self,
        stream: stream::Key,
        make: impl FnOnce() -> T,
    ) -> schedule::InsertResult<stream::Key, ()> {
        self.inner.try_store(stream, make)
    }

    fn transaction<P, R, E>(
        &mut self,
        key: stream::Key,
        check: impl FnOnce(&T) -> Result<(domain::Activation<conn::DataBlock>, P), E>,
        commit: impl FnOnce(&mut T, P) -> R,
    ) -> Result<R, schedule::Error<E>> {
        self.inner.transaction(key, |value| {
            let (act, plan) = check(&*value)?;
            let act = match act {
                domain::Activation::Keep => schedule::Activation::Keep,
                domain::Activation::Data(wait) => schedule::Activation::data(wait),
            };
            Ok((act, commit(value, plan)))
        })
    }

    fn resume(&mut self, conn: &mut Connection<ClientRole, F, C>, egress: schedule::Egress) {
        self.inner.resume(|| conn.flow().poll_wake(), egress);
    }

    fn drive<D: domain::Driver<Connection<ClientRole, F, C>, T>>(
        &mut self,
        conn: &mut Connection<ClientRole, F, C>,
        driver: &mut D,
    ) -> Result<schedule::Report, domain::Failure<D::Error, stream::Key>> {
        let mut turn = Drive {
            connection: conn,
            driver,
            failure: None,
        };
        let report = self.inner.drive_data(&mut turn);
        match turn.failure {
            Some(error) => Err(error),
            None => Ok(report),
        }
    }
}
