use dope::runtime::profile::{Balanced, LowLatency, Throughput};

/// ```
/// use sark_h2::tuning::Tuning;
/// use dope::runtime::profile::Throughput;
/// assert!(<Throughput as Tuning>::CONN_RECV_WINDOW >= <Throughput as Tuning>::STREAM_RECV_WINDOW);
/// ```
pub trait Tuning: 'static {
    const MAX_ACTIVE_STREAMS: usize = 256;
    const STREAM_RECV_WINDOW: u32;
    const CONN_RECV_WINDOW: u32;
    const MAX_BODY_LEN: usize;
    const MAX_CONN_BUFFERED_LEN: usize;
}

impl Tuning for Throughput {
    const STREAM_RECV_WINDOW: u32 = 4 << 20;
    const CONN_RECV_WINDOW: u32 = 16 << 20;
    const MAX_BODY_LEN: usize = 16 << 20;
    const MAX_CONN_BUFFERED_LEN: usize = 64 << 20;
}

impl Tuning for Balanced {
    const STREAM_RECV_WINDOW: u32 = 1 << 20;
    const CONN_RECV_WINDOW: u32 = 8 << 20;
    const MAX_BODY_LEN: usize = 4 << 20;
    const MAX_CONN_BUFFERED_LEN: usize = 16 << 20;
}

impl Tuning for LowLatency {
    const STREAM_RECV_WINDOW: u32 = 256 << 10;
    const CONN_RECV_WINDOW: u32 = 1 << 20;
    const MAX_BODY_LEN: usize = 1 << 20;
    const MAX_CONN_BUFFERED_LEN: usize = 4 << 20;
}
