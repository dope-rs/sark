use crate::stream::StreamId;

pub trait Role: Copy + Default + 'static {
    const IS_SERVER: bool;
    const FIRST_LOCAL_STREAM_ID: StreamId;
    const FIRST_PEER_STREAM_ID: StreamId;
    const PREFACE_SENDS_FIRST: bool;
}

#[derive(Copy, Clone, Default, Debug)]
pub struct ServerRole;

impl Role for ServerRole {
    const IS_SERVER: bool = true;
    const FIRST_LOCAL_STREAM_ID: StreamId = StreamId::FIRST_SERVER;
    const FIRST_PEER_STREAM_ID: StreamId = StreamId::FIRST_CLIENT;
    const PREFACE_SENDS_FIRST: bool = false;
}

#[derive(Copy, Clone, Default, Debug)]
pub struct ClientRole;

impl Role for ClientRole {
    const IS_SERVER: bool = false;
    const FIRST_LOCAL_STREAM_ID: StreamId = StreamId::FIRST_CLIENT;
    const FIRST_PEER_STREAM_ID: StreamId = StreamId::FIRST_SERVER;
    const PREFACE_SENDS_FIRST: bool = true;
}
