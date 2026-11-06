use std::{cell::Cell, ops, pin::Pin};

use o3::{
    base::cell::region::Token,
    buffer::{
        self, hold, resident,
        storage::{Shared, range},
        write::consume,
    },
};

pub struct Gate {
    bytes: Shared,
    open: Cell<bool>,
    plans: Cell<usize>,
}

pub struct View<'input> {
    view: buffer::View<'input, Shared>,
    open: &'input Cell<bool>,
}

impl Gate {
    pub fn closed(bytes: Shared) -> Self {
        Self {
            bytes,
            open: Cell::new(false),
            plans: Cell::new(0),
        }
    }

    pub fn open(&self) {
        self.open.set(true);
    }

    pub fn plans(&self) -> usize {
        self.plans.get()
    }
}

impl buffer::Source for Gate {
    type View<'input>
        = View<'input>
    where
        Self: 'input;

    fn remaining(&self) -> usize {
        self.bytes.len()
    }
}

impl buffer::Cursor for Gate {
    fn try_get<'input>(
        &'input self,
        range: ops::Range<usize>,
    ) -> Result<Self::View<'input>, range::Error> {
        self.plans.set(self.plans.get() + 1);
        buffer::Cursor::try_get(&self.bytes, range).map(|view| View {
            view,
            open: &self.open,
        })
    }
}

impl AsRef<[u8]> for Gate {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}

impl<'input> buffer::Read<'input> for View<'input> {
    fn as_slice(&self) -> &'input [u8] {
        self.view.as_slice()
    }

    fn try_split<const N: usize>(self) -> Result<(&'input [u8; N], Self), consume::Error> {
        let (head, view) = buffer::Read::try_split(self.view)?;
        Ok((
            head,
            Self {
                view,
                open: self.open,
            },
        ))
    }
}

// SAFETY: The gate changes only copy/share policy; both preserve the immutable view bytes.
unsafe impl<'d> hold::raw::Retain<'d> for Gate {
    fn charge<'a>(plan: &Self::View<'a>) -> hold::Charge
    where
        Self: 'a,
    {
        <Shared as hold::raw::Retain<'d>>::charge(&plan.view)
    }

    fn try_retain<'a>(
        plan: &Self::View<'a>,
        budget: &resident::Budget<'d>,
    ) -> Result<resident::Shared<'d>, hold::Error>
    where
        Self: 'a,
    {
        if !plan.open.get() {
            return resident::Exact::try_copy(budget, plan.view.as_slice())
                .map(resident::Exact::into_shared)
                .map_err(hold::Error::Resident);
        }
        <Shared as hold::raw::Retain<'d>>::try_retain(&plan.view, budget)
    }
}

pub mod decoder;
mod output;
mod retain;
pub mod session;
mod source;
pub use output::only;
pub use retain::Retain;
pub(in crate::common::codec) use source::Source;

// SAFETY: toggling retention policy never changes the immutable fixture bytes.
unsafe impl buffer::raw::Source for Gate {}

pub trait Scope {
    fn try_scope<R>(
        capacity: usize,
        run: impl for<'d> FnOnce(resident::Budget<'d>) -> R,
    ) -> Result<R, resident::Reuse>;
}

impl Scope for resident::Domain {
    fn try_scope<R>(
        capacity: usize,
        run: impl for<'d> FnOnce(resident::Budget<'d>) -> R,
    ) -> Result<R, resident::Reuse> {
        let mut domain = Self::new();
        Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
            let budget = domain.get_mut().try_budget(capacity, token.proof())?;
            Ok(run(budget))
        })
    }
}
