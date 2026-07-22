#![cfg(target_os = "linux")]
//! End-to-end echo over the real server event loop, exercising the zero-copy
//! recv fast path (`ChunkSource`): batched, large, fragmented and split frames.

use std::io::Write;
use std::time::Duration;

mod common;
use common::{connect, masked, next_message, run_echo};

#[test]
fn echo_roundtrips_through_fastpath() {
    run_echo(|bind| {
        let (mut sock, mut buf) = connect(bind);

        let mut batch = Vec::new();
        batch.extend_from_slice(&masked(0x1, true, b"alpha"));
        batch.extend_from_slice(&masked(0x2, true, b"bravo!!"));
        batch.extend_from_slice(&masked(0x1, true, b""));
        let big = vec![0xCD; 1000]; // 16-bit extended length payload
        batch.extend_from_slice(&masked(0x2, true, &big));
        sock.write_all(&batch).unwrap();

        assert_eq!(next_message(&mut sock, &mut buf), (0x1, b"alpha".to_vec()));
        assert_eq!(
            next_message(&mut sock, &mut buf),
            (0x2, b"bravo!!".to_vec())
        );
        assert_eq!(next_message(&mut sock, &mut buf), (0x1, Vec::new()));
        assert_eq!(next_message(&mut sock, &mut buf), (0x2, big));

        sock.write_all(&masked(0x1, false, b"foo")).unwrap();
        sock.write_all(&masked(0x0, true, b"bar")).unwrap();
        assert_eq!(next_message(&mut sock, &mut buf), (0x1, b"foobar".to_vec()));

        let frame = masked(0x1, true, b"charlie");
        sock.write_all(&frame[..3]).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        sock.write_all(&frame[3..]).unwrap();
        assert_eq!(
            next_message(&mut sock, &mut buf),
            (0x1, b"charlie".to_vec())
        );

        sock.write_all(&masked(0x9, true, b"hb")).unwrap();
        assert_eq!(next_message(&mut sock, &mut buf), (0xA, b"hb".to_vec()));
    });
}
