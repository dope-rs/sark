use std::time;

const DEFAULT_IDLE_TIMEOUT: time::Duration = time::Duration::from_secs(30);
const DEFAULT_MAX_INFLIGHT: usize = 256;
const DEFAULT_RETAINED_WIRE_FAILURES: usize = 1;

pub(super) struct Limits {
    pub(super) idle_timeout: time::Duration,
    pub(super) max_inflight: usize,
    pub(super) retained_wire_failures: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_inflight: DEFAULT_MAX_INFLIGHT,
            retained_wire_failures: DEFAULT_RETAINED_WIRE_FAILURES,
        }
    }
}
