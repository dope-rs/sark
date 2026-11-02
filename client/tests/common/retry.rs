use std::num::NonZero;

use sark_client::protocol::retry::Policy;

pub fn aggressive() -> Policy {
    Policy {
        idempotent_attempts: NonZero::<u8>::MIN.saturating_add(7),
        non_idempotent_attempts: NonZero::<u8>::MIN.saturating_add(7),
        reconnect_on_stale_non_idempotent: true,
    }
}
