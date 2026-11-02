pub mod common;
use std::cell::Cell;

use common::cause::Detail;
use dope::{core::Errno, manifold::client::deadline};
use dope_test::alloc::Tracker;
use o3::{
    depot::{arena::pool::shared, capacity::Count},
    mem::rc,
};
use sark_client::{Error, Failure, Safety, conn};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn retains_detail() {
    let drops = Cell::new(0);
    let message = String::from("peer sent a fatal alert");
    let store = (shared::Store::try_new(Count::new(1))).expect("cause store allocation");
    let vacancy = (store.vacancy()).expect("reserved connection report");
    let slot = vacancy.reserve();

    let (owners, cost) = Tracker::usage(|| -> Result<_, rc::Error> {
        let error = conn::Error::wire(conn::Cause::new(slot.commit(Detail {
            message: &message,
            drops: &drops,
        })));
        let first = error.try_clone()?;
        Ok((first, error))
    });
    let (first, second) = owners.expect("second connection cause must retain");
    assert_eq!(cost, (0, 0));
    assert!(store.vacancy().is_none());
    let first = Failure::<_>::new(Error::Transport(first), Safety::PossiblyCommitted);
    let second = Failure::<_>::new(Error::Transport(second), Safety::PossiblyCommitted);

    {
        let first = first;
        assert!(first.safety() == Safety::PossiblyCommitted);
        let error = match first.cause() {
            Error::Transport(error) => Some(error),
            _ => None,
        }
        .expect("wire cause must retain its transport category");
        assert!(error.kind() == conn::Kind::Wire);
        let detail = (error.detail()).expect("exact detail");
        assert_eq!(detail.message, message);
        assert_eq!(drops.get(), 0);
    }
    assert_eq!(drops.get(), 0);
    assert!(store.vacancy().is_none());
    {
        let second = second;
        assert!(second.safety() == Safety::PossiblyCommitted);
        assert!(matches!(second.cause(), Error::Transport(error) if error.detail().is_some()));
    }
    assert_eq!(drops.get(), 1);

    assert!(
        store.vacancy().map(|vacancy| vacancy.reserve()).is_some(),
        "the final cause recycles its reservation"
    );
}

#[test]
fn receive_is_exact() {
    let errno = Errno::NO_BUFFER_SPACE;
    let (error, cost) = Tracker::usage(|| conn::Error::<()>::receive(errno));

    assert_eq!(cost, (0, 0));
    assert!(error.kind() == conn::Kind::Transport);
    assert!(matches!(&error, conn::Error::Receive { errno: actual } if *actual == errno));
    assert!(error.detail().is_none());
    assert_eq!(size_of::<conn::Error<()>>(), 6 * size_of::<usize>());
}

#[test]
fn classified_has_no_detail() {
    let error = conn::Error::<Detail<'_>>::Classified(conn::Kind::Timeout(deadline::Kind::Inbound));
    assert!(error.kind() == conn::Kind::Timeout(deadline::Kind::Inbound));
    assert!(error.detail().is_none());
    assert_eq!(size_of::<conn::Cause<'_, Detail<'_>>>(), size_of::<usize>());
}
