use sark_core::manifold::schedule::Quiescence;
use sark_h2::{
    ServerRole,
    conn::machine::{Connection, ops::Drain},
};

#[test]
fn drains_gracefully() {
    use sark_h2::frame::{layout::Header, numbers::Type};

    let mut conn = Connection::<ServerRole>::try_new().expect("server carrier");
    conn.transfer().drain(usize::MAX);
    assert!(matches!(
        conn.control().drain(Quiescence::Busy),
        Drain::Pending
    ));
    let out = conn.output().first();
    let header = Header::parse(out).expect("graceful GOAWAY");
    assert!(header.kind == Type::GoAway);
    assert!(conn.status().is_goaway());
    assert!(conn.status().is_draining());
    assert!(matches!(
        conn.control().finish(Quiescence::Quiet),
        Drain::Complete
    ));
    assert!(!conn.status().is_draining());
    assert!(matches!(
        conn.control().finish(Quiescence::Quiet),
        Drain::Pending
    ));
}

#[test]
fn awaits_quiescence() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server carrier");
    conn.transfer().drain(usize::MAX);
    assert!(matches!(
        conn.control().drain(Quiescence::Busy),
        Drain::Pending
    ));
    assert!(matches!(
        conn.control().finish(Quiescence::Busy),
        Drain::Pending
    ));
    assert!(!conn.status().is_closing());
    assert!(matches!(
        conn.control().finish(Quiescence::Quiet),
        Drain::Complete
    ));
    assert!(conn.status().is_closing());
}

#[test]
fn changes_mode() {
    use sark_h2::frame::numbers::ErrorCode;

    let mut conn = Connection::<ServerRole>::try_new().expect("server carrier");
    conn.transfer().drain(usize::MAX);
    conn.control().goaway(ErrorCode::NoError);
    let bytes = conn.output().len();
    assert!(conn.status().is_closing());
    assert!(matches!(
        conn.control().drain(Quiescence::Busy),
        Drain::Pending
    ));
    assert!(conn.status().is_draining());
    assert!(!conn.status().is_closing());
    assert_eq!(conn.output().len(), bytes);
    conn.control().goaway(ErrorCode::InternalError);
    assert!(conn.status().is_closing());
    assert!(!conn.status().is_draining());
    assert_eq!(conn.output().len(), bytes);
    assert!(matches!(
        conn.control().finish(Quiescence::Quiet),
        Drain::Pending
    ));
}
