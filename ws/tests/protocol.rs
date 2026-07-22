#![cfg(target_os = "linux")]
//! Protocol conformance over the real server loop: close-code handling and
//! malformed-frame rejection (replacing the former white-box unit tests).

use std::io::Write;

mod common;
use common::{close_code, connect, masked, next_message, run_echo};

#[test]
fn close_codes_validated() {
    run_echo(|bind| {
        let cases: &[(u16, u16)] = &[
            (1000, 1000),
            (1003, 1003),
            (1011, 1011),
            (3000, 3000),
            (4999, 4999),
            (999, 1002),
            (1004, 1002),
            (1005, 1002),
            (1015, 1002),
            (5000, 1002),
        ];
        for &(code, expect) in cases {
            let (mut sock, mut buf) = connect(bind);
            sock.write_all(&masked(0x8, true, &code.to_be_bytes()))
                .unwrap();
            let (op, payload) = next_message(&mut sock, &mut buf);
            assert_eq!(op, 0x8, "code {code}");
            assert_eq!(close_code(&payload), Some(expect), "code {code}");
        }
    });
}

#[test]
fn close_with_invalid_utf8_reason_replies_1007() {
    run_echo(|bind| {
        let (mut sock, mut buf) = connect(bind);
        let mut payload = 1000u16.to_be_bytes().to_vec();
        payload.extend_from_slice(&[0xff, 0xfe]);
        sock.write_all(&masked(0x8, true, &payload)).unwrap();
        let (op, p) = next_message(&mut sock, &mut buf);
        assert_eq!(op, 0x8);
        assert_eq!(close_code(&p), Some(1007));
    });
}

#[test]
fn valid_close_is_echoed_with_reason() {
    run_echo(|bind| {
        let (mut sock, mut buf) = connect(bind);
        let mut payload = 1000u16.to_be_bytes().to_vec();
        payload.extend_from_slice(b"bye");
        sock.write_all(&masked(0x8, true, &payload)).unwrap();
        let (op, p) = next_message(&mut sock, &mut buf);
        assert_eq!(op, 0x8);
        assert_eq!(p, payload);
    });
}

#[test]
fn oversized_frame_replies_1009() {
    run_echo(|bind| {
        let (mut sock, mut buf) = connect(bind);
        let mut frame = vec![0x82u8, 0x80 | 127];
        frame.extend_from_slice(&((16 * 1024 * 1024u64) + 1).to_be_bytes());
        frame.extend_from_slice(&[0, 0, 0, 0]); // mask
        sock.write_all(&frame).unwrap();
        let (op, p) = next_message(&mut sock, &mut buf);
        assert_eq!(op, 0x8);
        assert_eq!(close_code(&p), Some(1009));
    });
}

#[test]
fn length_overflow_replies_1002() {
    run_echo(|bind| {
        let (mut sock, mut buf) = connect(bind);
        let mut frame = vec![0x82u8, 0x80 | 127];
        frame.extend_from_slice(&0x8000_0000_0000_0000u64.to_be_bytes()); // MSB set
        frame.extend_from_slice(&[0, 0, 0, 0]);
        sock.write_all(&frame).unwrap();
        let (op, p) = next_message(&mut sock, &mut buf);
        assert_eq!(op, 0x8);
        assert_eq!(close_code(&p), Some(1002));
    });
}

#[test]
fn unmasked_client_frame_replies_1002() {
    run_echo(|bind| {
        let (mut sock, mut buf) = connect(bind);
        sock.write_all(&[0x81u8, 0x03, b'a', b'b', b'c']).unwrap(); // mask bit clear
        let (op, p) = next_message(&mut sock, &mut buf);
        assert_eq!(op, 0x8);
        assert_eq!(close_code(&p), Some(1002));
    });
}
