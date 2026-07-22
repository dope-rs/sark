use std::cell::Cell;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dope::driver::token::{Epoch, SlotIndex, Token, kind};
use dope::manifold::Manifold;
use dope::platform::Platform;
use dope::runtime::Idle;
use dope::{Driver, DriverContext, Event, EventRef, Submission};

const UPDATE_INTERVAL: Duration = Duration::from_secs(1);

type Sqe = <Driver as Platform>::Sqe;
type TimerSpec = <Driver as Platform>::TimerSpec;

pub struct Stamp {
    buf: Cell<[u8; 29]>,
    _marker: core::marker::PhantomPinned,
}

impl Default for Stamp {
    fn default() -> Self {
        Self::new()
    }
}

impl Stamp {
    pub fn new() -> Self {
        Self {
            buf: Cell::new(Self::snapshot_now()),
            _marker: core::marker::PhantomPinned,
        }
    }

    pub fn load(&self) -> [u8; 29] {
        self.buf.get()
    }

    fn refresh(&self) {
        self.buf.set(Self::snapshot_now());
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
    fn stamp(self: Pin<&Self>) -> Pin<&Stamp>;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TimerState {
    Disarmed,
    Armed,
    CancelPending,
    Stopped,
}

pub struct Updater<const ID: u8> {
    stamp: Option<NonNull<Stamp>>,
    state: TimerState,
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
            state: TimerState::Disarmed,
        }
    }

    fn token() -> Token {
        Token::new(ID, SlotIndex::new(0), Epoch::INITIAL)
    }

    fn timer_spec() -> &'static TimerSpec {
        static TIMER: OnceLock<TimerSpec> = OnceLock::new();
        TIMER.get_or_init(|| TimerSpec::from(UPDATE_INTERVAL))
    }

    fn try_arm(&mut self, driver: &mut DriverContext<'_, '_>) {
        if driver
            .push(Sqe::interval(Self::timer_spec(), Self::token()))
            .is_ok()
        {
            self.state = TimerState::Armed;
        }
    }

    fn try_cancel(&mut self, driver: &mut DriverContext<'_, '_>) {
        if driver.push(Sqe::cancel(Self::token(), kind::TIMER)).is_ok() {
            self.state = TimerState::Stopped;
        }
    }

    /// # Safety
    /// `stamp` must remain pinned and live while `self` is active.
    pub(crate) unsafe fn bind(self: Pin<&mut Self>, stamp: Pin<&Stamp>) {
        self.get_mut().stamp = Some(NonNull::from(stamp.get_ref()));
    }
}

impl<'d, const ID: u8> Manifold<'d> for Updater<ID> {
    const ID: u8 = ID;

    fn dispatch(self: Pin<&mut Self>, event: Event, _driver: &mut DriverContext<'_, 'd>) {
        let this = self.get_mut();
        if this.state != TimerState::Armed {
            return;
        }
        if matches!(
            event.as_ref(),
            EventRef::Timer(token) if token.same_target(Self::token())
        ) && let Some(stamp) = this.stamp
        {
            unsafe { stamp.as_ref() }.refresh();
        }
    }

    fn pre_park(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let this = self.as_mut().get_mut();
        match this.state {
            TimerState::Disarmed if this.stamp.is_some() => this.try_arm(driver),
            TimerState::CancelPending => this.try_cancel(driver),
            TimerState::Disarmed | TimerState::Armed | TimerState::Stopped => {}
        }
    }

    fn idle(self: Pin<&Self>) -> Idle {
        Idle::Park(None)
    }

    fn shutdown(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let this = self.as_mut().get_mut();
        match this.state {
            TimerState::Armed => {
                this.state = TimerState::CancelPending;
                this.try_cancel(driver);
            }
            TimerState::Disarmed => this.state = TimerState::Stopped,
            TimerState::CancelPending => this.try_cancel(driver),
            TimerState::Stopped => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::ptr::NonNull;
    use std::time::Duration;

    use dope::driver;
    use dope::runtime::profile::Throughput;
    use dope::runtime::{Executor, ShutdownTrigger};

    use super::{Stamp, TimerState, Updater};

    #[pin_project::pin_project]
    #[derive(dope_gen::Dispatcher)]
    struct App {
        #[pin]
        #[manifold]
        date: Updater<7>,
    }

    #[test]
    fn updater_refreshes_from_the_recurring_driver_event() {
        let stamp = pin!(Stamp::new());
        let initial = stamp.load();
        let updater = Updater {
            stamp: Some(NonNull::from(stamp.as_ref().get_ref())),
            state: TimerState::Disarmed,
        };
        let trigger = ShutdownTrigger::new().expect("shutdown trigger");
        let fire = trigger.try_clone().expect("clone shutdown trigger");
        let shutdown = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(1_250));
            fire.fire().expect("fire shutdown");
        });

        let config = driver::Config::for_tcp_profile::<Throughput>(1);
        Executor::new(config)
            .expect("executor")
            .enter(|mut session| {
                trigger
                    .try_register(&mut session.driver_access())
                    .expect("register shutdown trigger");
                session
                    .with_app(App { date: updater }, |mut app| app.run())
                    .expect("run updater");
            });
        shutdown.join().expect("shutdown thread");

        assert_ne!(stamp.load(), initial);
    }
}
