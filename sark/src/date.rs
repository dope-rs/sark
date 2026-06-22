use std::pin::Pin;
use std::ptr::NonNull;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dope::manifold::Manifold;
use dope::runtime::token::{Epoch, LocalIdx, Token};
use dope::sqe::{Sqe, Timespec};
use dope::{Drive, Driver};

pub struct Stamp {
    buf: [u8; 29],
}

impl Default for Stamp {
    fn default() -> Self {
        Self::new()
    }
}

impl Stamp {
    pub fn new() -> Self {
        Self {
            buf: Self::snapshot_now(),
        }
    }

    pub fn buf(&self) -> &[u8; 29] {
        &self.buf
    }

    pub fn refresh(&mut self) {
        self.buf = Self::snapshot_now();
    }

    #[cold]
    fn snapshot_now() -> [u8; 29] {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self::format(secs)
    }

    fn is_leap(year: i64) -> bool {
        year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
    }

    fn format(epoch_secs: u64) -> [u8; 29] {
        const DAYS: [&[u8; 3]; 7] = [b"Thu", b"Fri", b"Sat", b"Sun", b"Mon", b"Tue", b"Wed"];
        const MONTHS: [&[u8; 3]; 12] = [
            b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov",
            b"Dec",
        ];

        let secs = epoch_secs;
        let day_of_epoch = secs / 86400;
        let wday = (day_of_epoch % 7) as usize;

        let time_of_day = secs % 86400;
        let hour = (time_of_day / 3600) as u8;
        let minute = ((time_of_day % 3600) / 60) as u8;
        let second = (time_of_day % 60) as u8;

        let mut days = day_of_epoch as i64;
        let mut year: i64 = 1970;
        loop {
            let dy = if Self::is_leap(year) { 366 } else { 365 };
            if days < dy {
                break;
            }
            days -= dy;
            year += 1;
        }
        let leap = Self::is_leap(year);
        let mdays = [
            31,
            if leap { 29 } else { 28 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        let mut month = 0usize;
        while month < 11 && days >= mdays[month] {
            days -= mdays[month];
            month += 1;
        }
        let day = days as u8 + 1;

        let mut buf = [b' '; 29];
        buf[0..3].copy_from_slice(DAYS[wday]);
        buf[3] = b',';
        buf[5] = b'0' + day / 10;
        buf[6] = b'0' + day % 10;
        buf[8..11].copy_from_slice(MONTHS[month]);
        let y = year as u16;
        buf[12] = b'0' + (y / 1000) as u8;
        buf[13] = b'0' + ((y / 100) % 10) as u8;
        buf[14] = b'0' + ((y / 10) % 10) as u8;
        buf[15] = b'0' + (y % 10) as u8;
        buf[17] = b'0' + hour / 10;
        buf[18] = b'0' + hour % 10;
        buf[19] = b':';
        buf[20] = b'0' + minute / 10;
        buf[21] = b'0' + minute % 10;
        buf[22] = b':';
        buf[23] = b'0' + second / 10;
        buf[24] = b'0' + second % 10;
        buf[26..29].copy_from_slice(b"GMT");
        buf
    }
}

pub trait DateHost {
    fn date_stamp(&self) -> &Stamp;
}

pub struct Updater<const ID: u8> {
    stamp: Option<NonNull<Stamp>>,
    armed: bool,
}

impl<const ID: u8> Default for Updater<ID> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const ID: u8> Updater<ID> {
    pub fn new() -> Self {
        Self {
            stamp: None,
            armed: false,
        }
    }

    pub fn bind(&mut self, stamp: NonNull<Stamp>) {
        self.stamp = Some(stamp);
    }
}

impl<const ID: u8> Manifold for Updater<ID> {
    const ID: u8 = ID;

    fn dispatch(self: Pin<&mut Self>, ev: dope::Event, _driver: &mut Driver) {
        let this = self.get_mut();
        if let dope::Event::Timer(_) = ev
            && let Some(mut stamp) = this.stamp
        {
            // SAFETY: `stamp` is the App-owned `Stamp` in the same pinned per-core Dispatcher; the single-threaded loop never overlaps this refresh with a request's read of the same buffer.
            unsafe { stamp.as_mut() }.refresh();
        }
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut Driver) {
        let this = self.get_mut();
        if !this.armed && this.stamp.is_some() {
            static TS: std::sync::OnceLock<Timespec> = std::sync::OnceLock::new();
            let ts: &'static Timespec = TS.get_or_init(|| Timespec::from(Duration::from_secs(1)));
            let ud = Token::new(ID, LocalIdx::new(0), Epoch::ZERO);
            if driver.push(Sqe::interval(ts, ud)).is_ok() {
                this.armed = true;
            }
        }
    }
}
