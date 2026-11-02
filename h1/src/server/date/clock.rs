use std::cell;

use dope::runtime::civil::{self, Source as _};
use sark_http::field::date::Stamp;

use crate::server::date::Error;

/// Driver-local civil clock for automatic HTTP/1 Date fields.
pub struct Clock {
    stamp: cell::Cell<Stamp>,
}

impl Clock {
    pub fn new() -> Result<Self, Error> {
        Ok(Self {
            stamp: cell::Cell::new(Self::read()?),
        })
    }

    pub fn load(&self) -> Stamp {
        self.stamp.get()
    }

    /// Advances the cached stamp when civil time is representable. A failed
    /// sample leaves the last valid HTTP date intact.
    pub(super) fn refresh(&self) {
        if let Ok(stamp) = Self::read() {
            self.stamp.set(stamp);
        }
    }

    fn read() -> Result<Stamp, Error> {
        let now = civil::System::NEW.read()?;
        Stamp::from_unix(now.floor_secs()).ok_or(Error::Range)
    }
}
