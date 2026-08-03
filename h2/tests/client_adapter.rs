mod common;

use common::sid;
use dope::manifold::connector;
use dope_net::link::egress;
use dope_net::link::egress::queue::Queue;
use o3::buffer::Shared;
use o3::cell::RegionToken;
use sark_h2::client::{Codec, ConnState, Handler, Head, Session, State};
use sark_h2::frame::{self, Flags, GoAway, Settings};
use sark_h2::{CLIENT_PREFACE, ClientRole, Conn, ErrorCode, FrameHeader, conn};

const ARENA_CAPACITY: u32 = connector::state::IOV_CAP as u32;
const _: () = assert!(connector::state::IOV_CAP <= u32::MAX as usize);

struct CapturingHandler {
    events: Vec<conn::Event>,
}

impl CapturingHandler {
    fn new() -> Self {
        Self { events: Vec::new() }
    }
}

impl Handler for CapturingHandler {
    fn event(&mut self, event: conn::Event, _conn: &mut Conn<ClientRole>) {
        self.events.push(event);
    }
}

fn settings_ack_bytes() -> Vec<u8> {
    let mut out = Vec::new();
    Settings::new(true, &[]).unwrap().encode(&mut out);
    out
}

fn settings_bytes(initial_window: u32) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&4u16.to_be_bytes());
    payload.extend_from_slice(&initial_window.to_be_bytes());
    let mut out = Vec::new();
    Settings::new(false, &payload).unwrap().encode(&mut out);
    out
}

fn goaway_bytes(err: ErrorCode) -> Vec<u8> {
    let mut out = Vec::new();
    GoAway::new(sid(0), err, b"").unwrap().encode(&mut out);
    out
}

fn collect_sink<'d>(
    sink: &Queue<'_, 'd, '_, { connector::state::IOV_CAP }>,
    region: &RegionToken<'d>,
) -> Vec<u8> {
    let mut acc = Vec::new();
    let mut i = 0;
    while let Some(chunk) = sink.pending_at(region, i) {
        acc.extend_from_slice(chunk.as_slice());
        i += 1;
    }
    acc
}

#[test]
fn connect_emits_preface_and_settings() {
    RegionToken::scope(|mut region| {
        let mut session = Session::new(CapturingHandler::new());
        let mut state = ConnState::default();
        let storage = egress::storage::Storage::with_capacity(ARENA_CAPACITY);
        let mut arena = storage.shared_arena(&region, 1);
        let mut sink = arena.queue();
        session.connect(&mut state, &mut sink, &mut region);
        let bytes = collect_sink(&sink, &region);
        assert!(bytes.starts_with(CLIENT_PREFACE));
        let after = &bytes[CLIENT_PREFACE.len()..];
        let h = FrameHeader::parse(after).expect("settings header");
        assert_eq!(h.kind, frame::Type::Settings);
        assert!(h.flags.0 & Flags::ACK == 0);
    });
}

#[test]
fn codec_parse_returns_full_buffer() {
    let codec = Codec;
    let mut state = State;
    let buf = Shared::copy_from_slice(&[1u8, 2, 3, 4]);
    let (head, consumed) = connector::codec::Codec::parse(&codec, &mut state, &buf).expect("parse");
    assert_eq!(consumed, 4);
    let Head(inner) = head;
    assert_eq!(inner.as_slice(), &[1, 2, 3, 4]);

    let empty = Shared::new();
    assert!(connector::codec::Codec::parse(&codec, &mut state, &empty).is_none());
}

#[test]
fn response_ingests_and_emits_events() {
    RegionToken::scope(|mut region| {
        let mut session = Session::new(CapturingHandler::new());
        let mut state = ConnState::default();
        let storage = egress::storage::Storage::with_capacity(ARENA_CAPACITY);
        let mut arena = storage.shared_arena(&region, 1);
        {
            let mut sink = arena.queue();
            session.connect(&mut state, &mut sink, &mut region);
            let connected = collect_sink(&sink, &region).len();
            assert!(sink.try_ack(&mut region, connected));
        }
        let mut sink = arena.queue();

        let peer = settings_bytes(65_535);
        let head = Head(Shared::copy_from_slice(&peer));
        session.response(head, &mut state, &mut sink, &mut region);
        assert!(
            session
                .handler()
                .events
                .iter()
                .any(|e| matches!(e, conn::Event::SettingsApplied))
        );
        let ack_buf = collect_sink(&sink, &region);
        let h = FrameHeader::parse(&ack_buf).expect("settings ack header");
        assert_eq!(h.kind, frame::Type::Settings);
        assert!(h.flags.0 & Flags::ACK != 0);
    });
}

#[test]
fn wants_close_after_goaway_in() {
    RegionToken::scope(|mut region| {
        let mut session = Session::new(CapturingHandler::new());
        let mut state = ConnState::default();
        let storage = egress::storage::Storage::with_capacity(ARENA_CAPACITY);
        let mut arena = storage.shared_arena(&region, 1);
        {
            let mut sink = arena.queue();
            session.connect(&mut state, &mut sink, &mut region);
        }
        let mut sink = arena.queue();

        let mut feed = settings_bytes(65_535);
        feed.extend_from_slice(&settings_ack_bytes());
        feed.extend_from_slice(&goaway_bytes(ErrorCode::EnhanceYourCalm));
        let head = Head(Shared::copy_from_slice(&feed));
        session.response(head, &mut state, &mut sink, &mut region);
        assert!(
            connector::lifecycle::Lifecycle::wants_close(&state)
                == connector::lifecycle::Close::Reconnect
        );
    });
}
