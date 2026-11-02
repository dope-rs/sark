use crate::decode::state;

#[derive(Debug)]
pub enum Sync {}
#[derive(Debug)]
pub enum Task {}
#[derive(Debug)]
pub enum Duplex {}
#[derive(Debug)]
pub enum Unsupported {}
#[derive(Debug)]
pub enum Mixed {}

/// Application-wide execution shape of selected routes.
pub trait Set {}

impl Set for Sync {}
impl Set for Task {}
impl Set for Duplex {}
impl Set for Unsupported {}
impl Set for Mixed {}

impl Kind for Sync {
    type Open<F: state::Set> = F::SyncOpen;
    type Finished<F: state::Set> = F::SyncFinished;
}

impl Kind for Task {
    type Open<F: state::Set> = F::TaskOpen;
    type Finished<F: state::Set> = F::TaskFinished;
}

impl Kind for Duplex {
    type Open<F: state::Set> = F::DuplexOpen;
    type Finished<F: state::Set> = F::DuplexFinished;
}

impl Kind for Unsupported {
    type Open<F: state::Set> = F::UnsupportedOpen;
    type Finished<F: state::Set> = F::UnsupportedFinished;
}

/// Terminal consumer for a route family proven to have one execution kind.
pub trait Fold<P, F: state::Set, K: Kind>: Sized {
    type Output;

    fn open<S: state::Slot<P>>(self, slot: S, body: K::Open<F>) -> Self::Output;

    fn finished<S: state::Slot<P>>(self, slot: S, body: K::Finished<F>) -> Self::Output;
}

/// Uniform execution selected without a runtime execution tag.
#[doc(hidden)]
pub trait Kind: Set + Sized {
    type Open<F: state::Set>;
    type Finished<F: state::Set>;
}
