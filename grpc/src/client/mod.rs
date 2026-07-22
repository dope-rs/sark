mod call;
mod egress;
mod ingress;

pub use ingress::{StreamEvent, TypedStreamEvent, UnaryResult};

use call::{CallStore, ResponseMode};
use egress::Egress;
use ingress::Ingress;

use sark_h2::{ClientRole, Conn, ErrorCode, StreamId, conn};

use crate::Codec;
use crate::headers::HeaderBlock;
use crate::metadata::Metadata;
use crate::status::{Code, Status};

#[derive(Clone, Debug)]
pub struct Config {
    pub max_in_flight: usize,
    pub max_completed: usize,
    pub max_events: usize,
    pub max_pending_msgs: usize,
    pub max_pending_len: usize,
    pub max_message_len: usize,
    pub max_buffered_len: usize,
    pub max_buffered_msgs: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_in_flight: 256,
            max_completed: 256,
            max_events: 8192,
            max_pending_msgs: 4,
            max_pending_len: 4 * 1024 * 1024 + 5,
            max_message_len: 4 * 1024 * 1024,
            max_buffered_len: 16 * 1024 * 1024,
            max_buffered_msgs: 8192,
        }
    }
}

pub struct Session {
    h2: Conn<ClientRole>,
    calls: CallStore,
    egress: Egress,
    ingress: Ingress,
    encode_buf: Vec<u8>,
}

impl Session {
    pub fn new() -> Self {
        Self::with_config(Config::default())
    }

    pub fn with_config(config: Config) -> Self {
        assert!(config.max_in_flight > 0, "max_in_flight must be positive");
        assert!(
            config.max_pending_msgs > 0,
            "max_pending_msgs must be positive"
        );
        assert!(
            config.max_pending_len > 0,
            "max_pending_len must be positive"
        );
        let h2 = Conn::<ClientRole>::with_config(conn::Config {
            stream_capacity: config.max_in_flight,
            ..conn::Config::default()
        });
        Self {
            h2,
            calls: CallStore::with_capacity(config.max_in_flight, config.max_message_len),
            egress: Egress::with_config(&config),
            ingress: Ingress::with_config(&config),
            encode_buf: Vec::with_capacity(config.max_message_len),
        }
    }

    pub fn outbound(&self) -> &[u8] {
        self.h2.outbound()
    }

    pub fn drain_outbound(&mut self, n: usize) {
        self.h2.drain_outbound(n);
    }

    pub fn ingest(&mut self, bytes: &[u8]) -> Result<(), Status> {
        let result = self.h2.ingest(bytes);
        self.ingress
            .drive(&mut self.h2, &mut self.calls, &mut self.egress, result)
    }

    pub fn resume(&mut self) -> Result<(), Status> {
        let result = self.h2.resume();
        self.ingress
            .drive(&mut self.h2, &mut self.calls, &mut self.egress, result)
    }

    pub fn start_unary_raw(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
        payload: &[u8],
    ) -> Result<StreamId, Status> {
        let stream_id =
            self.start_response_stream(path, authority, metadata, ResponseMode::Unary)?;
        if let Err(status) = self.send_message_bytes(stream_id, payload, true) {
            self.abort_stream(stream_id);
            return Err(status);
        }
        Ok(stream_id)
    }

    pub fn start_stream_raw(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
    ) -> Result<StreamId, Status> {
        self.start_response_stream(path, authority, metadata, ResponseMode::Streaming)
    }

    pub fn start_client_stream_raw(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
    ) -> Result<StreamId, Status> {
        self.start_response_stream(path, authority, metadata, ResponseMode::Unary)
    }

    fn start_response_stream(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
        mode: ResponseMode,
    ) -> Result<StreamId, Status> {
        let headers = HeaderBlock::for_request(path, authority, metadata)?;
        let h2_headers = headers.as_h2();
        let stream_id = self
            .h2
            .start_request(&h2_headers, false)
            .map_err(Status::from_conn_err)?;
        if !self.calls.insert(stream_id, mode) {
            let _ = self.h2.reset_stream(stream_id, ErrorCode::RefusedStream);
            return Err(Status::new(
                Code::ResourceExhausted,
                "too many in-flight streams",
            ));
        }
        Ok(stream_id)
    }

    pub fn send_message_raw(&mut self, stream_id: StreamId, payload: &[u8]) -> Result<(), Status> {
        self.send_message_bytes(stream_id, payload, false)
    }

    pub fn finish_send(&mut self, stream_id: StreamId) -> Result<(), Status> {
        self.egress
            .finish_send(&mut self.calls, &mut self.h2, stream_id)
    }

    pub fn start_streaming_raw<I, B>(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
        payloads: I,
    ) -> Result<StreamId, Status>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let stream_id = self.start_stream_raw(path, authority, metadata)?;
        let mut payloads = payloads.into_iter().peekable();
        if payloads.peek().is_none() {
            if let Err(status) = self.finish_send(stream_id) {
                self.abort_stream(stream_id);
                return Err(status);
            }
            return Ok(stream_id);
        }
        while let Some(payload) = payloads.next() {
            let end_stream = payloads.peek().is_none();
            if let Err(status) = self.send_message_bytes(stream_id, payload.as_ref(), end_stream) {
                self.abort_stream(stream_id);
                return Err(status);
            }
        }
        Ok(stream_id)
    }

    pub fn start_unary<C: Codec>(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<StreamId, Status> {
        let mut payload = core::mem::take(&mut self.encode_buf);
        payload.clear();
        let result = codec
            .encode(message, &mut payload)
            .and_then(|()| self.start_unary_raw(path, authority, metadata, &payload));
        self.encode_buf = payload;
        result
    }

    pub fn start_streaming<C: Codec>(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
        codec: &mut C,
        messages: &[C::Encode],
    ) -> Result<StreamId, Status> {
        let stream_id = self.start_stream_raw(path, authority, metadata)?;
        let mut payload = core::mem::take(&mut self.encode_buf);
        for (index, message) in messages.iter().enumerate() {
            payload.clear();
            if let Err(status) = codec.encode(message, &mut payload) {
                self.encode_buf = payload;
                self.abort_stream(stream_id);
                return Err(status);
            }
            let end_stream = index + 1 == messages.len();
            if let Err(status) = self.send_message_bytes(stream_id, &payload, end_stream) {
                self.encode_buf = payload;
                self.abort_stream(stream_id);
                return Err(status);
            }
        }
        self.encode_buf = payload;
        if messages.is_empty()
            && let Err(status) = self.finish_send(stream_id)
        {
            self.abort_stream(stream_id);
            return Err(status);
        }
        Ok(stream_id)
    }

    pub fn send_message<C: Codec>(
        &mut self,
        stream_id: StreamId,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<(), Status> {
        let mut payload = core::mem::take(&mut self.encode_buf);
        payload.clear();
        let result = codec
            .encode(message, &mut payload)
            .and_then(|()| self.send_message_raw(stream_id, &payload));
        self.encode_buf = payload;
        result
    }

    pub fn poll_unary(&mut self) -> Option<UnaryResult> {
        self.ingress.poll_unary()
    }

    pub fn poll_event(&mut self) -> Option<StreamEvent> {
        self.ingress.poll_event()
    }

    fn send_message_bytes(
        &mut self,
        stream_id: StreamId,
        payload: &[u8],
        end_stream: bool,
    ) -> Result<(), Status> {
        self.egress.send_message(
            &mut self.calls,
            &mut self.h2,
            stream_id,
            payload,
            end_stream,
        )
    }

    fn abort_stream(&mut self, stream_id: StreamId) {
        self.egress.abort(&mut self.calls, &mut self.h2, stream_id);
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}
