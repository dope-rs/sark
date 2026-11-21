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

/// Connection-owned sequence state present only when observation is enabled.
#[doc(hidden)]
#[derive(Clone, Copy, Default)]
pub struct Session {
    order: Option<num::NonZero<u64>>,
    staging: Option<(u64, hold::Charge)>,
}

impl super::Session<Tag> for Session {
    fn tag(order: u64, charge: hold::Charge) -> Tag {
        Tag {
            order: num::NonZero::<u64>::new(order),
            charge,
        }
    }

    fn parts(tag: Tag) -> Option<(u64, hold::Charge)> {
        tag.order.map(|order| (order.get(), tag.charge))
    }

    fn advance(&mut self) -> u64 {
        let raw = self.order.map_or(1, |order| order.get().wrapping_add(1));
        let order = match num::NonZero::<u64>::new(raw) {
            Some(order) => order,
            None => num::NonZero::<u64>::MIN,
        };
        self.order = Some(order);
        order.get()
    }

    fn current(&self) -> Option<u64> {
        self.order.map(num::NonZero::<u64>::get)
    }

    fn stage(&mut self, id: u64, charge: hold::Charge) -> bool {
        if charge.is_empty() {
            return false;
        }
        self.staging = Some((id, charge));
        true
    }

    fn clear_staged(&mut self, charge: hold::Charge) -> bool {
        if charge.is_empty() {
            return false;
        }
        self.staging = None;
        true
    }

    fn close(&mut self) -> Option<(u64, hold::Charge)> {
        let staging = self.staging;
        *self = Self::default();
        staging
    }
}

const _: () =
    assert!(core::mem::size_of::<Option<num::NonZero<u64>>>() == core::mem::size_of::<u64>());
