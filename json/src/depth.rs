use core::cell::Cell;

use crate::Result;
use crate::error::Fail;

pub const MAX_DEPTH: u32 = 128;

thread_local! {
    static DEPTH: Cell<u32> = const { Cell::new(0) };
}

pub struct DepthGuard {
    _seal: (),
}

impl DepthGuard {
    pub fn enter() -> Result<Self> {
        DEPTH.with(|depth| {
            let level = depth.get();
            if level >= MAX_DEPTH {
                return Err(Fail::with("JSON nesting too deep"));
            }
            depth.set(level + 1);
            Ok(Self { _seal: () })
        })
    }

    pub fn level() -> u32 {
        DEPTH.with(Cell::get)
    }
}

impl Drop for DepthGuard {
    fn drop(&mut self) {
        DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_increments_and_releases() {
        assert_eq!(DepthGuard::level(), 0);
        {
            let _a = DepthGuard::enter().unwrap();
            assert_eq!(DepthGuard::level(), 1);
            {
                let _b = DepthGuard::enter().unwrap();
                assert_eq!(DepthGuard::level(), 2);
            }
            assert_eq!(DepthGuard::level(), 1);
        }
        assert_eq!(DepthGuard::level(), 0);
    }

    #[test]
    fn guard_caps_at_max_depth() {
        let mut held = Vec::new();
        for _ in 0..MAX_DEPTH {
            held.push(DepthGuard::enter().expect("under cap"));
        }
        assert_eq!(DepthGuard::level(), MAX_DEPTH);
        assert!(DepthGuard::enter().is_err());
        drop(held);
        assert_eq!(DepthGuard::level(), 0);
        DepthGuard::enter().expect("recovered after release");
    }

    fn decode_recursive(remaining: u32) -> Result<u32> {
        let _guard = DepthGuard::enter()?;
        if remaining == 0 {
            return Ok(DepthGuard::level());
        }
        decode_recursive(remaining - 1)
    }

    #[test]
    fn recursive_decode_errors_before_overflow() {
        assert!(decode_recursive(10).is_ok());
        let err = decode_recursive(MAX_DEPTH + 5_000);
        assert!(err.is_err(), "deep recursion must error, not overflow");
        assert_eq!(DepthGuard::level(), 0);
    }
}
