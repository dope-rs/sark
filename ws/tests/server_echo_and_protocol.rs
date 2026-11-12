use dope_test::alloc::Tracker;
pub mod common;

use o3::buffer::storage;
use sark_core::duplex::{admit, machine};
use sark_http::payload::{owned, response::Body as _};
use std::{
    cell::Cell,
    io::{Read as _, Write as _},
    net::{Shutdown, TcpStream},
};

use crate::common::{
    echo::{Echo, OPENING, run_echo},
    peer::Peer as _,
    protocol::Payload as _,
};

#[test]
fn session_retains_tail_across_owned_output() {
    assert_eq!(
        size_of::<sark_ws::server::ConnState>(),
        size_of::<sark_ws::server::ConnState<()>>()
            + size_of::<Option<dope::net::link::egress::NonEmpty<storage::Shared>>>()
    );
    for eof in [false, true] {
        let calls = Cell::new(0);
        let echo = Echo {
            calls: &calls,
            repeat: None,
            activity: &|_| {},
            failed: &|error| panic!("session failure: {error:?}"),
        };
        let mut session = machine::Machine::new(sark_ws::server::Session::new(echo, 1024, 1024));
        let mut scratch = Vec::new();
        if eof {
            assert!(matches!(
                session.transact(&mut [], &mut scratch, |_, emit| {
                    match emit.into_parts().0 {
                        admit::Payload::Vacant(permit) => permit.commit::<_, ()>(()),
                        admit::Payload::Present { .. } => panic!("initial output"),
                    }
                }),
                machine::Transaction::Committed { .. }
            ));
        }
        let mut first = [b'x'; 256].try_masked(0x2, true).expect("first frame");
        let mut tail = if eof {
            [b'y'; 256].try_masked(0x2, true)
        } else {
            b"end".try_masked(0x1, true)
        }
        .expect("tail frame");
        if !eof {
            tail.extend(1000u16.to_be_bytes().try_masked(0x8, true).expect("close"));
        }
        first.push(tail[0]);
        let input = owned::Segments::from(storage::Shared::try_copy(&first).expect("first owner"))
            .try_extend(
                storage::Shared::try_copy(&tail[1..])
                    .expect("tail owner")
                    .into(),
            )
            .expect("segmented input");
        let machine::Access::Vacant(slot) = session.input() else {
            panic!("initial input slot");
        };
        slot.commit(input);
        let mut output = Vec::new();
        let mut deferred = false;
        let mut owned = 0;
        for _ in 0..8 {
            let mut prefix = [0; 160];
            let turn = session.transact(&mut prefix, &mut scratch, |written, emit| {
                match emit.into_parts().0 {
                    admit::Payload::Present { output, permit } if !deferred => {
                        assert_eq!(calls.get(), 1);
                        deferred = true;
                        permit.pending::<_, ()>(output, (written, None))
                    }
                    admit::Payload::Present { output, permit } => {
                        assert_eq!(calls.get(), owned + 1);
                        permit.commit((written, Some(output)))
                    }
                    admit::Payload::Vacant(permit) => permit.commit((written, None)),
                }
            });
            let (written, body) = match turn {
                machine::Transaction::Committed { receipt, .. } => receipt,
                machine::Transaction::Pending { receipt } => {
                    if eof {
                        session.finish(()).expect("input EOF").0.commit();
                    }
                    receipt
                }
                _ => panic!("output admission"),
            };
            output.extend_from_slice(&prefix[..written]);
            if let Some(body) = body {
                owned += 1;
                for chunk in owned::Segments::from(body).segments() {
                    output.extend_from_slice(chunk);
                }
                if eof && owned == 2 {
                    assert!(matches!(session.progress(), machine::Progress::Finished));
                }
            }
            if matches!(session.progress(), machine::Progress::Finished) {
                break;
            }
        }
        let mut expected = b"\x82\x7e\x01\x00".to_vec();
        expected.extend_from_slice(&[b'x'; 256]);
        if eof {
            expected.extend_from_slice(b"\x82\x7e\x01\x00");
            expected.extend_from_slice(&[b'y'; 256]);
        } else {
            expected.extend_from_slice(b"\x81\x03end\x88\x02\x03\xe8");
        }
        assert_eq!(output, expected);
        assert_eq!(calls.get(), 2);
        assert!(deferred);
        assert!(matches!(session.progress(), machine::Progress::Finished));
    }
}

#[test]
fn final_fragment_waits_for_output() {
    let calls = Cell::new(0);
    let echo = Echo {
        calls: &calls,
        repeat: None,
        activity: &|_| {},
        failed: &|error| panic!("session failure: {error:?}"),
    };
    let mut session = machine::Machine::new(sark_ws::server::Session::new(echo, 1024, 1024));
    let mut wire = b"x".try_masked(0x2, true).expect("first message");
    wire.extend([b'a'; 16].try_masked(0x2, false).expect("fragment"));
    wire.extend([b'b'; 48].try_masked(0x0, true).expect("final fragment"));
    let machine::Access::Vacant(slot) = session.input() else {
        panic!("initial input slot");
    };
    slot.commit(owned::Segments::from(
        storage::Shared::try_copy(&wire).expect("input"),
    ));
    let mut scratch = Vec::new();
    let mut output = Vec::new();
    for pass in 0..2 {
        let mut prefix = [0; 160];
        let turn = session.transact(&mut prefix, &mut scratch, |written, emit| {
            match emit.into_parts().0 {
                admit::Payload::Present { output, permit } => {
                    permit.commit::<_, ()>((written, Some(output)))
                }
                admit::Payload::Vacant(permit) => permit.commit((written, None)),
            }
        });
        let machine::Transaction::Committed {
            receipt: (written, body),
            ..
        } = turn
        else {
            panic!("output admission");
        };
        output.extend_from_slice(&prefix[..written]);
        if let Some(body) = body {
            for chunk in owned::Segments::from(body).segments() {
                output.extend_from_slice(chunk);
            }
        }
        assert_eq!(calls.get(), pass + 1);
    }
    let mut expected = b"\x82\x01x\x82\x40".to_vec();
    expected.extend([b'a'; 16]);
    expected.extend([b'b'; 48]);
    assert_eq!(output, expected);
}

#[test]
fn split_writes_echo() {
    run_echo(1, None, None, |bind| {
        for split in [1, 15, OPENING.len() - 3] {
            let mut sock = TcpStream::try_socket(bind).expect("WebSocket test peer");
            sock.write_all(&OPENING[..split])
                .expect("opening prefix write");
            sock.write_all(&OPENING[split..])
                .expect("opening tail write");
            let (head, _) = sock.try_head(&mut Vec::new()).expect("WebSocket test peer");
            assert!(head.starts_with(b"HTTP/1.1 101"), "split {split}");
        }

        let mut sock = TcpStream::try_socket(bind).expect("WebSocket test peer");
        let mut input = OPENING.to_vec();
        input.extend(
            b"coalesced"
                .try_masked(0x1, true)
                .expect("WebSocket test peer"),
        );
        sock.write_all(&input).expect("coalesced opening write");
        let (head, mut tail) = sock.try_head(&mut Vec::new()).expect("WebSocket test peer");
        assert!(head.starts_with(b"HTTP/1.1 101"));
        assert_eq!(
            sock.try_next(&mut tail).expect("WebSocket test peer"),
            (0x1, b"coalesced".to_vec())
        );
    });
}

#[test]
fn echo_roundtrips() {
    run_echo(6, None, None, |bind| {
        let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket test peer");

        let big = vec![0xCD; 1000];
        let frames = [
            (0x1, b"alpha".as_slice()),
            (0x2, b"bravo!!"),
            (0x1, b""),
            (0x2, big.as_slice()),
        ];
        let mut batch = Vec::new();
        for &(code, data) in &frames {
            batch.extend(data.try_masked(code, true).expect("batch frame"));
        }
        sock.write_all(&batch).expect("echo batch write");
        for (code, data) in frames {
            assert_eq!(
                sock.try_next(&mut buf).expect("WebSocket test peer"),
                (code, data.to_vec())
            );
        }

        sock.write_all(&b"foo".try_masked(0x1, false).expect("WebSocket test peer"))
            .expect("fragment prefix write");
        sock.write_all(&b"bar".try_masked(0x0, true).expect("WebSocket test peer"))
            .expect("fragment tail write");
        assert_eq!(
            sock.try_next(&mut buf).expect("WebSocket test peer"),
            (0x1, b"foobar".to_vec())
        );

        let frame = b"charlie"
            .try_masked(0x1, true)
            .expect("WebSocket test peer");
        sock.write_all(&frame[..3]).expect("frame prefix write");
        sock.write_all(&frame[3..]).expect("frame tail write");
        assert_eq!(
            sock.try_next(&mut buf).expect("WebSocket test peer"),
            (0x1, b"charlie".to_vec())
        );

        sock.write_all(&b"hb".try_masked(0x9, true).expect("WebSocket test peer"))
            .expect("ping write");
        assert_eq!(
            sock.try_next(&mut buf).expect("WebSocket test peer"),
            (0xA, b"hb".to_vec())
        );
    });
}

#[test]
fn controls_survive_load() {
    run_echo(512, None, None, |bind| {
        let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket test peer");
        let data = [0x5a; 256];
        let mut batch = Vec::new();
        for _ in 0..256 {
            batch.extend(data.try_masked(0x2, true).expect("WebSocket test peer"));
        }
        batch.extend(
            b"causal-ping"
                .try_masked(0x9, true)
                .expect("WebSocket test peer"),
        );
        sock.write_all(&batch[..1]).expect("batch prefix write");
        sock.write_all(&batch[1..]).expect("batch tail write");

        for _ in 0..256 {
            assert_eq!(
                sock.try_next(&mut buf).expect("echo before pong"),
                (0x2, data.to_vec())
            );
        }
        assert_eq!(
            sock.try_next(&mut buf).expect("causal pong"),
            (0xa, b"causal-ping".to_vec())
        );

        let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket test peer");
        let mut batch = Vec::new();
        for _ in 0..256 {
            batch.extend(data.try_masked(0x2, true).expect("WebSocket test peer"));
        }
        let close = [0x03, 0xe8, b'b', b'y', b'e'];
        batch.extend(close.try_masked(0x8, true).expect("WebSocket test peer"));
        sock.write_all(&batch[..1])
            .expect("close batch prefix write");
        sock.write_all(&batch[1..]).expect("close batch tail write");

        for _ in 0..256 {
            assert_eq!(
                sock.try_next(&mut buf).expect("echo before close"),
                (0x2, data.to_vec())
            );
        }
        assert_eq!(
            sock.try_next(&mut buf).expect("causal close"),
            (0x8, close.to_vec())
        );
    });
}

#[test]
fn invalid_openings_reject() {
    run_echo(0, None, None, |bind| {
        for req in [
            b"GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 12\r\n\r\n".as_slice(),
            b"GET / HTTP/1.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n".as_slice(),
            b"GET / HTTP/1.1\r\nHost: one\r\nHost: two\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZ!==\r\nSec-WebSocket-Version: 13\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nX-Nul: safe\0shadow\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
        ] {
            let mut sock = TcpStream::try_socket(bind).expect("WebSocket test peer");
            sock.write_all(req).expect("invalid opening write");
            let (head, _) = sock.try_head(&mut Vec::new()).expect("WebSocket test peer");
            assert!(head.starts_with(b"HTTP/1.1 400"));
            let version = b"\r\nSec-WebSocket-Version: 13\r\n";
            assert!(head.windows(version.len()).any(|field| field == version));
            assert_eq!(sock.read(&mut [0]).expect("rejection EOF"), 0);
        }
    });
}

#[test]
fn close_codes_validated() {
    run_echo(0, None, None, |bind| {
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
            let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket test peer");
            sock.write_all(
                &code
                    .to_be_bytes()
                    .try_masked(0x8, true)
                    .expect("WebSocket test peer"),
            )
            .expect("close write");
            let (op, data) = sock.try_next(&mut buf).expect("WebSocket test peer");
            assert_eq!(op, 0x8, "code {code}");
            assert_eq!(data.close_code(), Some(expect), "code {code}");
        }
    });
}

#[test]
fn invalid_utf8_closes_1007() {
    run_echo(0, None, None, |bind| {
        let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket test peer");
        let mut data = 1000u16.to_be_bytes().to_vec();
        data.extend_from_slice(&[0xff, 0xfe]);
        sock.write_all(&data.try_masked(0x8, true).expect("WebSocket test peer"))
            .expect("invalid close write");
        let (op, p) = sock.try_next(&mut buf).expect("WebSocket test peer");
        assert_eq!(op, 0x8);
        assert_eq!(p.close_code(), Some(1007));
    });
}

#[test]
fn close_echoes_reason() {
    run_echo(0, None, None, |bind| {
        let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket test peer");
        let mut data = 1000u16.to_be_bytes().to_vec();
        data.extend_from_slice(b"bye");
        sock.write_all(&data.try_masked(0x8, true).expect("WebSocket test peer"))
            .expect("close reason write");
        let (op, p) = sock.try_next(&mut buf).expect("WebSocket test peer");
        assert_eq!(op, 0x8);
        assert_eq!(p, data);
    });
}

#[test]
fn large_frame_closes_1009() {
    run_echo(0, None, None, |bind| {
        let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket test peer");
        let mut frame = vec![0x82u8, 0x80 | 127];
        frame.extend_from_slice(&((16 * 1024 * 1024u64) + 1).to_be_bytes());
        frame.extend_from_slice(&[0, 0, 0, 0]);
        sock.write_all(&frame).expect("large frame write");
        let (op, p) = sock.try_next(&mut buf).expect("WebSocket test peer");
        assert_eq!(op, 0x8);
        assert_eq!(p.close_code(), Some(1009));
    });
}

#[test]
fn bad_length_closes_1002() {
    run_echo(0, None, None, |bind| {
        let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket test peer");
        let mut frame = vec![0x82u8, 0x80 | 127];
        frame.extend_from_slice(&0x8000_0000_0000_0000u64.to_be_bytes());
        frame.extend_from_slice(&[0, 0, 0, 0]);
        sock.write_all(&frame).expect("invalid length write");
        let (op, p) = sock.try_next(&mut buf).expect("WebSocket test peer");
        assert_eq!(op, 0x8);
        assert_eq!(p.close_code(), Some(1002));
    });
}

#[test]
fn unmasked_closes_1002() {
    run_echo(0, None, None, |bind| {
        let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket test peer");
        sock.write_all(&[0x81u8, 0x03, b'a', b'b', b'c'])
            .expect("unmasked frame write");
        let (op, p) = sock.try_next(&mut buf).expect("WebSocket test peer");
        assert_eq!(op, 0x8);
        assert_eq!(p.close_code(), Some(1002));
    });
}

/// Rejects an oversized masked PING using its header before its payload arrives.
#[test]
fn large_ping_closes_1002() {
    run_echo(0, None, None, |bind| {
        let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket test peer");
        sock.write_all(&[0x89, 0xfe, 0x00, 0x7e, 1, 2, 3, 4])
            .expect("large ping write");
        let (op, data) = sock.try_next(&mut buf).expect("WebSocket test peer");
        assert_eq!(op, 0x8);
        assert_eq!(data.close_code(), Some(1002));
    });
}

/// Accepts the aggregate limit and rejects excess at the continuation header.
#[test]
fn message_cap_closes_1009() {
    run_echo(1, None, Some(8), |bind| {
        let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket test peer");
        let first = b"abcd".try_masked(0x2, false).expect("WebSocket test peer");
        sock.write_all(&first).expect("fragment write");
        sock.write_all(&b"efgh".try_masked(0x0, true).expect("WebSocket test peer"))
            .expect("continuation write");
        assert_eq!(
            sock.try_next(&mut buf).expect("message at limit"),
            (0x2, b"abcdefgh".to_vec())
        );

        sock.write_all(&first).expect("fragment write");
        sock.write_all(&[0x80, 0x85, 1, 2, 3, 4])
            .expect("continuation head write");

        let (op, data) = sock.try_next(&mut buf).expect("WebSocket test peer");
        assert_eq!(op, 0x8);
        assert_eq!(data.close_code(), Some(1009));
    });
}

#[test]
fn zero_message_cap_keeps_control_frames() {
    run_echo(1, None, Some(0), |bind| {
        let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket test peer");
        sock.write_all(&b"".try_masked(0x2, true).expect("WebSocket test peer"))
            .expect("empty message write");
        assert_eq!(
            sock.try_next(&mut buf).expect("empty message echo"),
            (0x2, Vec::new())
        );
        sock.write_all(&b"ping".try_masked(0x9, true).expect("WebSocket test peer"))
            .expect("ping write");
        assert_eq!(
            sock.try_next(&mut buf).expect("pong at zero message cap"),
            (0xa, b"ping".to_vec())
        );

        sock.write_all(&[0x82, 0x81, 1, 2, 3, 4])
            .expect("nonempty message head write");
        let (op, data) = sock.try_next(&mut buf).expect("message exceeds zero cap");
        assert_eq!(op, 0x8);
        assert_eq!(data.close_code(), Some(1009));
    });
}

#[test]
fn noncanonical_closes_1002() {
    run_echo(0, None, None, |bind| {
        for header in [vec![0x82, 0xfe, 0x00, 0x01, 1, 2, 3, 4], {
            let mut frame = vec![0x82, 0xff];
            frame.extend_from_slice(&65535u64.to_be_bytes());
            frame.extend_from_slice(&[1, 2, 3, 4]);
            frame
        }] {
            let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket test peer");
            sock.write_all(&header).expect("noncanonical head write");
            let (op, data) = sock.try_next(&mut buf).expect("WebSocket test peer");
            assert_eq!(op, 0x8);
            assert_eq!(data.close_code(), Some(1002));
        }
    });
}

#[test]
fn valid_before_bad_closes() {
    run_echo(1, None, None, |bind| {
        let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket test peer");
        let mut batch = b"A".try_masked(0x1, true).expect("WebSocket test peer");
        batch.extend_from_slice(&[0x81, 0x03]);
        sock.write_all(&batch).expect("mixed batch write");

        assert_eq!(
            sock.try_next(&mut buf).expect("WebSocket test peer"),
            (0x1, b"A".to_vec())
        );
        let (opcode, data) = sock.try_next(&mut buf).expect("WebSocket test peer");
        assert_eq!(opcode, 0x8);
        assert_eq!(data.close_code(), Some(1002));
    });
}

#[test]
fn preserves_queued_bytes() {
    run_echo(17, None, None, |bind| {
        let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket peer");
        let mut batch = Vec::new();
        for id in 0..16u8 {
            batch.extend([id; 4096].try_masked(0x2, true).expect("4KiB frame"));
        }
        let large: Vec<_> = (0..65536).map(|index| (index % 251) as u8).collect();
        batch.extend(
            large[..32768]
                .try_masked(0x2, false)
                .expect("fragment prefix"),
        );
        batch.extend(b"between".try_masked(0x9, true).expect("fragment control"));
        batch.extend(large[32768..].try_masked(0x0, true).expect("fragment tail"));
        batch.extend([3, 232].try_masked(0x8, true).expect("close after data"));
        sock.write_all(&batch).expect("pipeline write");
        assert_eq!(sock.peek(&mut [0]).expect("queued responses"), 1);
        for id in 0..16u8 {
            assert_eq!(
                sock.try_next(&mut buf).expect("ordered 4KiB echo"),
                (0x2, vec![id; 4096])
            );
        }
        assert_eq!(
            sock.try_next(&mut buf).expect("fragment pong"),
            (0xa, b"between".to_vec())
        );
        assert_eq!(
            sock.try_next(&mut buf).expect("owned assembled echo"),
            (0x2, large)
        );
        assert_eq!(
            sock.try_next(&mut buf).expect("ordered close"),
            (0x8, vec![3, 232])
        );
    });
}

#[test]
fn amplifies_once() {
    let reply = vec![0xa7; 64 * 1024];
    run_echo(65, Some(&reply), None, |bind| {
        let (mut sock, mut buf) = TcpStream::try_connect(bind).expect("WebSocket peer");
        let mut batch = Vec::new();
        for id in 0..64u8 {
            batch.extend([id].try_masked(0x2, true).expect("small request"));
        }
        sock.write_all(&batch).expect("amplified pipeline");
        assert_eq!(sock.peek(&mut [0]).expect("queued responses"), 1);
        sock.write_all(&[64].try_masked(0x2, true).expect("later request"))
            .expect("arrival after queued output");
        sock.shutdown(Shutdown::Write)
            .expect("peer EOF preserves responses");
        for _ in 0..65 {
            assert_eq!(
                sock.try_next(&mut buf).expect("large reply"),
                (0x2, reply.clone())
            );
        }
        let mut end = [0; 1];
        assert_eq!(
            sock.read(&mut end)
                .expect("server EOF after every response"),
            0
        );
    });
}

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();
