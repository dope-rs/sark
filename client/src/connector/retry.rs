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
