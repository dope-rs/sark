use sark_h2::flow;
use sark_h2::flow::{Pair, Window};

#[test]
fn new_initial_value() {
    let w = Window::new();
    assert_eq!(w.value, 65_535);
    assert_eq!(w.available(), 65_535);
    assert!(!w.is_stalled());
}

#[test]
fn default_equals_new() {
    assert_eq!(Window::default(), Window::new());
}

#[test]
fn with_zero_is_stalled() {
    let w = Window::with(0);
    assert!(w.is_stalled());
    assert_eq!(w.available(), 0);
}

#[test]
fn with_negative_available_zero() {
    let w = Window::with(-5);
    assert!(w.is_stalled());
    assert_eq!(w.available(), 0);
}

#[test]
fn max_constant() {
    assert_eq!(Window::MAX, 0x7fff_ffff);
}

#[test]
fn initial_constant() {
    assert_eq!(Window::INITIAL, 65_535);
}

#[test]
fn consume_basic() {
    let mut w = Window::new();
    assert!(w.consume(100).is_ok());
    assert_eq!(w.value, 65_435);
}

#[test]
fn consume_exact() {
    let mut w = Window::with(100);
    assert!(w.consume(100).is_ok());
    assert_eq!(w.value, 0);
    assert!(w.is_stalled());
}

#[test]
fn consume_over_available_stalls() {
    let mut w = Window::new();
    assert_eq!(w.consume(70_000), Err(flow::Error::Stalled));
    assert_eq!(w.value, 65_535);
}

#[test]
fn consume_when_zero() {
    let mut w = Window::with(0);
    assert_eq!(w.consume(1), Err(flow::Error::Stalled));
    assert_eq!(w.consume(0), Ok(()));
}

#[test]
fn consume_when_negative_always_stalls() {
    let mut w = Window::with(-100);
    assert_eq!(w.consume(1), Err(flow::Error::Stalled));
    assert_eq!(w.consume(50), Err(flow::Error::Stalled));
    assert_eq!(w.value, -100);
}

#[test]
fn increase_basic() {
    let mut w = Window::new();
    assert!(w.increase(1000).is_ok());
    assert_eq!(w.value, 66_535);
}

#[test]
fn increase_zero_protocol_error() {
    let mut w = Window::new();
    assert_eq!(w.increase(0), Err(flow::Error::ZeroIncrement));
}

#[test]
fn increase_to_max_ok() {
    let mut w = Window::with(0);
    assert!(w.increase(Window::MAX as u32).is_ok());
    assert_eq!(w.value, Window::MAX);
}

#[test]
fn increase_overflow_above_max() {
    let mut w = Window::with(1);
    assert_eq!(w.increase(Window::MAX as u32), Err(flow::Error::Overflow));
}

#[test]
fn increase_u32_high_bit_overflow() {
    let mut w = Window::new();
    assert_eq!(w.increase(0xffff_ffff), Err(flow::Error::Overflow));
}

#[test]
fn increase_from_negative_recovers() {
    let mut w = Window::with(-50_000);
    assert!(w.increase(60_000).is_ok());
    assert_eq!(w.value, 10_000);
    assert!(!w.is_stalled());
}

#[test]
fn adjust_initial_positive() {
    let mut w = Window::new();
    assert!(w.adjust_initial(10_000).is_ok());
    assert_eq!(w.value, 75_535);
}

#[test]
fn adjust_initial_negative_to_negative() {
    let mut w = Window::new();
    assert!(w.adjust_initial(-100_000).is_ok());
    assert_eq!(w.value, 65_535 - 100_000);
    assert!(w.is_stalled());
    assert_eq!(w.available(), 0);
}

#[test]
fn adjust_initial_then_consume_stalls() {
    let mut w = Window::new();
    w.adjust_initial(-100_000).unwrap();
    assert_eq!(w.consume(1), Err(flow::Error::Stalled));
}

#[test]
fn adjust_initial_then_increase_recovers() {
    let mut w = Window::new();
    w.adjust_initial(-200_000).unwrap();
    assert!(w.is_stalled());
    w.increase(50_000).unwrap();
    assert_eq!(w.value, 65_535 - 200_000 + 50_000);
    assert!(w.is_stalled());
    w.increase(200_000).unwrap();
    assert!(!w.is_stalled());
}

#[test]
fn adjust_initial_overflow_positive() {
    let mut w = Window::with(Window::MAX);
    assert_eq!(w.adjust_initial(1), Err(flow::Error::Overflow));
    assert_eq!(w.value, Window::MAX);
}

#[test]
fn adjust_initial_overflow_negative() {
    let mut w = Window::with(i32::MIN + 1);
    assert_eq!(w.adjust_initial(-2), Err(flow::Error::Overflow));
}

#[test]
fn pair_available_min_of_conn_and_stream() {
    let mut c = Window::with(100);
    let mut s = Window::with(200);
    let p = Pair {
        conn: &mut c,
        stream: &mut s,
    };
    assert_eq!(p.available(), 100);
}

#[test]
fn pair_consume_drains_both() {
    let mut c = Window::with(100);
    let mut s = Window::with(200);
    let mut p = Pair {
        conn: &mut c,
        stream: &mut s,
    };
    assert!(p.consume(50).is_ok());
    assert_eq!(c.value, 50);
    assert_eq!(s.value, 150);
}

#[test]
fn pair_consume_over_min_stalls_without_partial() {
    let mut c = Window::with(10);
    let mut s = Window::with(200);
    let mut p = Pair {
        conn: &mut c,
        stream: &mut s,
    };
    assert_eq!(p.consume(100), Err(flow::Error::Stalled));
    assert_eq!(c.value, 10);
    assert_eq!(s.value, 200);
}

#[test]
fn pair_available_zero_when_conn_zero() {
    let mut c = Window::with(0);
    let mut s = Window::with(100);
    let p = Pair {
        conn: &mut c,
        stream: &mut s,
    };
    assert_eq!(p.available(), 0);
}

#[test]
fn pair_available_zero_when_stream_negative() {
    let mut c = Window::with(100);
    let mut s = Window::with(-50);
    let p = Pair {
        conn: &mut c,
        stream: &mut s,
    };
    assert_eq!(p.available(), 0);
}

#[test]
fn pair_consume_zero_ok() {
    let mut c = Window::with(0);
    let mut s = Window::with(0);
    let mut p = Pair {
        conn: &mut c,
        stream: &mut s,
    };
    assert!(p.consume(0).is_ok());
}

#[test]
fn isolation_other_stream_unaffected() {
    let mut conn = Window::new();
    let mut attacker = Window::new();
    let mut victim = Window::new();
    let attacker_value = attacker.value as usize;
    {
        let mut p = Pair {
            conn: &mut conn,
            stream: &mut attacker,
        };
        p.consume(attacker_value).unwrap();
    }
    assert_eq!(attacker.value, 0);
    assert_eq!(victim.value, 65_535);
    assert!(conn.value < 65_535);
    let mut p = Pair {
        conn: &mut conn,
        stream: &mut victim,
    };
    assert!(p.available() < 65_535);
    assert!(p.consume(p.available()).is_ok());
}

#[test]
fn window_update_above_u32_via_high_bit_overflow() {
    let mut w = Window::with(1);
    assert_eq!(w.increase(0x8000_0000), Err(flow::Error::Overflow));
}
