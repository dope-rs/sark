use sark_h2::frame::{HEADER_LEN, Type};
use sark_h2::{ClientRole, Conn, Flags, FrameHeader};

#[test]
fn data_parts_emit_one_frame() {
    let mut conn = Conn::<ClientRole>::new();
    conn.drain_outbound(conn.outbound().len());
    let stream_id = conn.start_request(&[], false).unwrap();
    conn.drain_outbound(conn.outbound().len());

    let written = conn
        .send_data_parts(stream_id, b"header", b"payload", true)
        .unwrap();
    assert_eq!(written, 13);

    let out = conn.outbound();
    let header = FrameHeader::parse(out).unwrap();
    assert_eq!(header.kind, Type::Data);
    assert_eq!(header.length, 13);
    assert!(header.flags.has(Flags::END_STREAM));
    assert_eq!(&out[HEADER_LEN..], b"headerpayload");

    let tail = out[5..].to_vec();
    conn.drain_outbound(5);
    assert_eq!(conn.outbound(), tail);
}
