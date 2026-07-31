#![allow(dead_code)]

use sark_h2::{FrameLength, StreamId, WindowIncrement};

pub fn sid(raw: u32) -> StreamId {
    assert!(raw <= StreamId::MAX, "invalid test stream ID");
    StreamId::new(raw).expect("range checked above")
}

pub fn flen(raw: u32) -> FrameLength {
    assert!(raw <= FrameLength::MAX, "invalid test frame length");
    FrameLength::new(raw).expect("range checked above")
}

pub fn win(raw: u32) -> WindowIncrement {
    assert!(
        (1..=WindowIncrement::MAX).contains(&raw),
        "invalid test window increment"
    );
    WindowIncrement::new(raw).expect("range checked above")
}
