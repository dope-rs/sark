use http::Method;

use crate::connector::error::Error;

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub idempotent_attempts: u8,
    pub non_idempotent_attempts: u8,
    pub reconnect_on_stale_non_idempotent: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            idempotent_attempts: 3,
            non_idempotent_attempts: 2,
            reconnect_on_stale_non_idempotent: true,
        }
    }
}

impl RetryPolicy {
    pub fn is_idempotent(method: &Method) -> bool {
        matches!(
            *method,
            Method::GET | Method::HEAD | Method::OPTIONS | Method::PUT | Method::DELETE
        )
    }

    pub fn attempts(&self, method: &Method) -> u32 {
        if Self::is_idempotent(method) {
            self.idempotent_attempts.max(1) as u32
        } else {
            self.non_idempotent_attempts.max(1) as u32
        }
    }

    pub fn should_retry(&self, method: &Method, err: &Error) -> bool {
        if Self::is_idempotent(method) {
            return matches!(err, Error::Closed | Error::NotConnected | Error::Timeout);
        }
        match err {
            Error::NotConnected => true,
            Error::Closed => self.reconnect_on_stale_non_idempotent,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotent_retries_all_transient_errors() {
        let p = RetryPolicy::default();
        for err in [Error::Closed, Error::NotConnected, Error::Timeout] {
            assert!(p.should_retry(&Method::GET, &err));
        }
        assert!(!p.should_retry(&Method::GET, &Error::Parse("x".into())));
        assert!(!p.should_retry(&Method::GET, &Error::Backpressure));
    }

    #[test]
    fn non_idempotent_never_retries_on_timeout() {
        let p = RetryPolicy::default();
        assert!(!p.should_retry(&Method::POST, &Error::Timeout));
        assert!(!p.should_retry(&Method::PATCH, &Error::Timeout));
    }

    #[test]
    fn non_idempotent_retries_not_connected() {
        let p = RetryPolicy::default();
        assert!(p.should_retry(&Method::POST, &Error::NotConnected));
    }

    #[test]
    fn non_idempotent_closed_follows_reconnect_flag() {
        let mut p = RetryPolicy {
            reconnect_on_stale_non_idempotent: true,
            ..Default::default()
        };
        assert!(p.should_retry(&Method::POST, &Error::Closed));
        p.reconnect_on_stale_non_idempotent = false;
        assert!(!p.should_retry(&Method::POST, &Error::Closed));
    }

    #[test]
    fn patch_is_non_idempotent() {
        assert!(!RetryPolicy::is_idempotent(&Method::POST));
        assert!(!RetryPolicy::is_idempotent(&Method::PATCH));
        assert!(RetryPolicy::is_idempotent(&Method::PUT));
        assert!(RetryPolicy::is_idempotent(&Method::DELETE));
    }
}
