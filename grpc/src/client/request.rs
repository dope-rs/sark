use o3::buffer::Pooled;
use o3::collections::SlabKey;
use sark_h2::{ClientRole, Conn, ConnError, StreamId};

pub(super) enum PendingRequestTag {}
pub(super) type PendingRequestKey = SlabKey<PendingRequestTag>;

pub(super) struct PendingRequest {
    pub(super) bytes: Pooled,
    pub(super) pos: usize,
    pub(super) end_stream: bool,
    pub(super) next: Option<PendingRequestKey>,
}

pub(super) enum RequestDrive {
    Complete,
    Blocked,
}

impl PendingRequest {
    pub(super) fn drive(
        &mut self,
        conn: &mut Conn<ClientRole>,
        stream_id: StreamId,
    ) -> Result<RequestDrive, ConnError> {
        while self.pos < self.bytes.len() {
            let n = conn.send_data(
                stream_id,
                &self.bytes.as_slice()[self.pos..],
                self.end_stream,
            )?;
            if n == 0 {
                return Ok(RequestDrive::Blocked);
            }
            self.pos += n;
        }
        Ok(RequestDrive::Complete)
    }
}
