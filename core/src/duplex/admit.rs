//! Affine carrier admission proofs for one duplex turn.
use core::marker;

use o3::{
    base::marker::ThreadBound,
    buffer::{resident, segments},
};

use crate::duplex::machine;

type Owner<'turn, D> = marker::PhantomData<(
    D,
    ThreadBound,
    fn(D) -> D,
    fn(&'turn mut ()) -> &'turn mut (),
)>;

/// Session output and terminal state bound to one machine turn.
#[must_use = "classify this emitted duplex turn"]
pub struct Emission<'turn, 'd, D> {
    pub(in crate::duplex) payload: Payload<'turn, 'd, D>,
    pub(in crate::duplex) end: machine::End,
    pub(in crate::duplex) after: machine::After,
}

impl<'turn, 'd, D> Emission<'turn, 'd, D> {
    pub fn into_parts(self) -> (Payload<'turn, 'd, D>, machine::End, machine::After) {
        (self.payload, self.end, self.after)
    }
}

/// Exact ownership state of the session output for this turn.
#[must_use = "consume the payload and its admission permission"]
pub enum Payload<'turn, 'd, D> {
    Present {
        output: segments::Nonempty<resident::Shared<'d>>,
        permit: Permit<'turn, Present, D>,
    },
    Vacant(Permit<'turn, Vacant, D>),
}

/// Proof that this turn owns an output chunk.
pub enum Present {}

/// Proof that this turn has no output chunk.
pub enum Vacant {}

/// Affine permission to classify one carrier admission attempt.
#[must_use = "classify this carrier admission attempt"]
pub struct Permit<'turn, M, D> {
    pub(in crate::duplex) owner: Owner<'turn, D>,
    pub(in crate::duplex) mode: marker::PhantomData<M>,
}

impl<'turn, M, D> Permit<'turn, M, D> {
    pub fn commit<'d, R, E>(self, receipt: R) -> Admission<'turn, 'd, D, R, E> {
        Admission::new(self.owner, Decision::Committed { receipt })
    }
}

impl<'turn, D> Permit<'turn, Present, D> {
    pub fn pending<'d, R, E>(
        self,
        rest: segments::Nonempty<resident::Shared<'d>>,
        receipt: R,
    ) -> Admission<'turn, 'd, D, R, E> {
        Admission::new(
            self.owner,
            Decision::Pending {
                remainder: rest,
                receipt,
            },
        )
    }

    pub fn returned<'d, R, E>(
        self,
        error: E,
        rest: segments::Nonempty<resident::Shared<'d>>,
    ) -> Admission<'turn, 'd, D, R, E> {
        Admission::new(
            self.owner,
            Decision::Returned {
                error,
                remainder: rest,
            },
        )
    }
}

impl<'turn, D> Permit<'turn, Vacant, D> {
    pub fn blocked<'d, R, E>(self, receipt: R) -> Admission<'turn, 'd, D, R, E> {
        Admission::new(self.owner, Decision::Blocked { receipt })
    }

    pub fn reject<'d, R, E>(self, error: E) -> Admission<'turn, 'd, D, R, E> {
        Admission::new(self.owner, Decision::Rejected { error })
    }
}

/// Exact carrier disposition joined to its originating machine turn.
#[must_use = "return this disposition to the duplex machine"]
#[repr(transparent)]
pub struct Admission<'turn, 'd, D, R, E> {
    pub(in crate::duplex) decision: Decision<'d, R, E>,
    pub(in crate::duplex) owner: Owner<'turn, D>,
}

impl<'turn, 'd, D, R, E> Admission<'turn, 'd, D, R, E> {
    fn new(owner: Owner<'turn, D>, decision: Decision<'d, R, E>) -> Self {
        Self { decision, owner }
    }
}

pub(in crate::duplex) enum Decision<'d, R, E> {
    Committed {
        receipt: R,
    },
    Pending {
        remainder: segments::Nonempty<resident::Shared<'d>>,
        receipt: R,
    },
    Blocked {
        receipt: R,
    },
    Rejected {
        error: E,
    },
    Returned {
        error: E,
        remainder: segments::Nonempty<resident::Shared<'d>>,
    },
}

const _: () = {
    assert!(core::mem::size_of::<Permit<'static, Present, crate::duplex::Never>>() == 0);
    assert!(core::mem::align_of::<Permit<'static, Present, crate::duplex::Never>>() == 1);
    assert!(
        core::mem::size_of::<Payload<'static, 'static, crate::duplex::Never>>()
            == core::mem::size_of::<Option<segments::Nonempty<resident::Shared<'static>>>>()
    );
    assert!(
        core::mem::align_of::<Payload<'static, 'static, crate::duplex::Never>>()
            == core::mem::align_of::<Option<segments::Nonempty<resident::Shared<'static>>>>()
    );
    assert!(
        core::mem::size_of::<Admission<'static, 'static, crate::duplex::Never, usize, usize>>()
            == core::mem::size_of::<Decision<'static, usize, usize>>()
    );
    assert!(
        core::mem::align_of::<Admission<'static, 'static, crate::duplex::Never, usize, usize>>()
            == core::mem::align_of::<Decision<'static, usize, usize>>()
    );
};
