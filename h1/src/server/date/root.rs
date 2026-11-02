use dope::{core::driver::route::Identity, manifold::timing::interval, runtime::control};

use crate::server::date::Clock;

/// The clock borrow remains independent of the driver generation.
///
#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
#[coordinate]
#[doc(hidden)]
pub struct Root<'c, 'd, ID: Identity> {
    #[pin]
    #[manifold(control)]
    interval: interval::Interval<'d, ID>,
    #[dispatcher(state)]
    clock: &'c Clock,
}

impl<'c, 'd, ID: Identity> Root<'c, 'd, ID> {
    pub(in crate::server) const fn new(
        clock: &'c Clock,
        interval: interval::Interval<'d, ID>,
    ) -> Self {
        Self { interval, clock }
    }

    fn coordinate(mut fields: root_coord::Fields<'_, 'c, 'd, ID>) -> control::Flow {
        if fields.interval.take_tick() == interval::Tick::Ready {
            fields.clock.refresh();
        }
        control::Flow::Idle
    }
}
