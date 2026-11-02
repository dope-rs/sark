use sark_h2::{
    flow,
    flow::{Pair, Window},
    frame::numbers::Increment,
};

#[test]
fn new_initial_value() {
    let w = Window::new();
    assert_eq!(w.value(), 65_535);
    assert_eq!(w.available(), 65_535);
    assert!(!w.is_stalled());
}

#[test]
fn default_equals_new() {
    assert!((Window::default()) == (Window::new()));
}

#[test]
fn with_zero_is_stalled() {
    let w = Window::with(0);
    assert!(w.is_stalled());
    assert_eq!(w.available(), 0);
}

#[test]
fn negative_has_no_credit() {
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
fn increase_basic() {
    let mut w = Window::new();
    (w.increase(Increment::new(1000).expect("positive increment")))
        .as_ref()
        .expect("test operation must succeed");
    assert_eq!(w.value(), 66_535);
}

#[test]
fn increase_to_max_ok() {
    let mut w = Window::with(0);
    (w.increase(Increment::new(Window::MAX as u32).expect("maximum increment")))
        .as_ref()
        .expect("test operation must succeed");
    assert_eq!(w.value(), Window::MAX);
}

#[test]
fn max_update_overflows() {
    let mut w = Window::with(1);
    let inc = Increment::new(Window::MAX as u32).expect("maximum increment");
    assert!(
        (w.increase(inc))
            == (Err(flow::Increase {
                value: 1,
                committed: 1,
                inc,
            }))
    );
}

#[test]
fn negative_update_recovers() {
    let mut w = Window::with(-50_000);
    (w.increase(Increment::new(60_000).expect("positive increment")))
        .as_ref()
        .expect("test operation must succeed");
    assert_eq!(w.value(), 10_000);
    assert!(!w.is_stalled());
}

#[test]
fn adjust_initial_positive() {
    let mut w = Window::new();
    (w.adjust(10_000))
        .as_ref()
        .expect("test operation must succeed");
    assert_eq!(w.value(), 75_535);
}

#[test]
fn adjust_stays_negative() {
    let mut w = Window::new();
    (w.adjust(-100_000))
        .as_ref()
        .expect("test operation must succeed");
    assert_eq!(w.value(), 65_535 - 100_000);
    assert!(w.is_stalled());
    assert_eq!(w.available(), 0);
}

#[test]
fn adjust_then_recovers() {
    let mut w = Window::new();
    w.adjust(-200_000)
        .expect("the adjusted window remains in the i32 range");
    assert!(w.is_stalled());
    (w.increase(Increment::new(50_000).expect("positive increment")))
        .as_ref()
        .expect("the first update remains below the window maximum");
    assert_eq!(w.value(), 65_535 - 200_000 + 50_000);
    assert!(w.is_stalled());
    (w.increase(Increment::new(200_000).expect("positive increment")))
        .as_ref()
        .expect("the second update remains below the window maximum");
    assert!(!w.is_stalled());
}

#[test]
fn positive_adjust_fails() {
    let mut w = Window::with(Window::MAX);
    assert_eq!(
        w.adjust(1),
        Err(flow::Adjust {
            value: Window::MAX,
            committed: Window::MAX,
            delta: 1,
        })
    );
    assert_eq!(w.value(), Window::MAX);
}

#[test]
fn negative_adjust_fails() {
    let mut w = Window::with(i32::MIN + 1);
    assert_eq!(
        w.adjust(-2),
        Err(flow::Adjust {
            value: i32::MIN + 1,
            committed: i32::MIN + 1,
            delta: -2,
        })
    );
}

#[test]
fn pair_uses_minimum() {
    let mut c = Window::with(100);
    let mut s = Window::with(200);
    let p = Pair {
        conn: &mut c,
        stream: &mut s,
    };
    assert_eq!(p.available(), 100);
}

#[test]
fn zero_conn_stalls() {
    let mut c = Window::with(0);
    let mut s = Window::with(100);
    let p = Pair {
        conn: &mut c,
        stream: &mut s,
    };
    assert_eq!(p.available(), 0);
}

#[test]
fn negative_stream_stalls() {
    let mut c = Window::with(100);
    let mut s = Window::with(-50);
    let p = Pair {
        conn: &mut c,
        stream: &mut s,
    };
    assert_eq!(p.available(), 0);
}
