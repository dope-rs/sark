use std::num::NonZero;

use dope_core::driver::{self, schedule, schedule::ready::task::owner, settings};
use sark_http::request;

pub struct Tasks;

impl settings::Profile for Tasks {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 128>();
    const SCHEDULER: settings::Schedule =
        settings::Schedule::fixed::<{ schedule::LIMIT as u32 + 2 }, 0>();
    const RECEIVE: settings::Receive = settings::Receive::fixed::<2, 4096>();
}

impl Tasks {
    pub fn with_ids(driver: driver::Context<'_, '_>, run: impl for<'d> FnOnce(request::Seq<'d>)) {
        let owners = owner::Capacity::new(NonZero::<u32>::MIN);
        let binding = driver::Binding::new(driver.driver_ref().timer(), owners);
        run(request::Seq::new(binding.sequence()));
    }
}
