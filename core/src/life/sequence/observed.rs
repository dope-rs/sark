use core::num;

use o3::buffer::hold;

/// Exact exchange identity and owner charge carried only by observed payloads.
#[doc(hidden)]
#[repr(C)]
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Tag {
    order: Option<num::NonZero<u64>>,
    charge: hold::Charge,
}

impl Tag {
    pub(in crate::life) const fn new(order: u64, charge: hold::Charge) -> Self {
        Self {
            order: num::NonZero::<u64>::new(order),
            charge,
        }
    }

    pub(in crate::life) const fn parts(self) -> Option<(u64, hold::Charge)> {
        match self.order {
            Some(order) => Some((order.get(), self.charge)),
            None => None,
        }
    }
}

/// Connection-owned sequence state present only when observation is enabled.
#[doc(hidden)]
#[derive(Clone, Copy, Default)]
pub struct Session {
    order: Option<num::NonZero<u64>>,
    staging: Option<(u64, hold::Charge)>,
}

impl Session {
    pub(in crate::life) fn advance(&mut self) -> u64 {
        let raw = self.order.map_or(1, |order| order.get().wrapping_add(1));
        let order = match num::NonZero::<u64>::new(raw) {
            Some(order) => order,
            None => num::NonZero::<u64>::MIN,
        };
        self.order = Some(order);
        order.get()
    }

    pub(in crate::life) fn current(self) -> Option<u64> {
        self.order.map(num::NonZero::<u64>::get)
    }

    pub(in crate::life) fn stage(&mut self, id: u64, charge: hold::Charge) {
        if !charge.is_empty() {
            self.staging = Some((id, charge));
        }
    }

    pub(in crate::life) fn take(&mut self) {
        self.staging = None;
    }

    pub(in crate::life) fn close(&mut self) -> Option<(u64, hold::Charge)> {
        let staging = self.staging;
        *self = Self::default();
        staging
    }
}

const _: () =
    assert!(core::mem::size_of::<Option<num::NonZero<u64>>>() == core::mem::size_of::<u64>());
