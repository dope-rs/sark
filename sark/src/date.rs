use std::cell::Cell;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dope::driver::token::{Epoch, SlotIndex, Token, kind};
use dope::runtime::Idle;
use dope::{DriverContext, Event, EventRef, Sqe, Submission, TimerSpec};

const UPDATE_INTERVAL: Duration = Duration::from_secs(1);

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

    pub(crate) fn dispatch<'d>(
        &mut self,
        event: Event<'d>,
        stamp: &Stamp,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
        if self.state != TimerState::Armed {
            return;
        }
        if matches!(
            event.as_ref(),
            EventRef::Timer(token) if token.same_target(Self::token())
        ) {
            stamp.refresh();
        }
    }

    pub(crate) fn pre_park<'d>(&mut self, driver: &mut DriverContext<'_, 'd>) {
        match self.state {
            TimerState::Disarmed => self.try_arm(driver),
            TimerState::CancelPending => self.try_cancel(driver),
            TimerState::Armed | TimerState::Stopped => {}
        }
    }

    pub(crate) fn idle(&self) -> Idle {
        Idle::Park(None)
    }

    pub(crate) fn shutdown<'d>(&mut self, driver: &mut DriverContext<'_, 'd>) {
        match self.state {
            TimerState::Armed => {
                self.state = TimerState::CancelPending;
                self.try_cancel(driver);
            }
            TimerState::Disarmed => self.state = TimerState::Stopped,
            TimerState::CancelPending => self.try_cancel(driver),
            TimerState::Stopped => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::{Pin, pin};
    use std::time::Duration;

    use dope::driver;
    use dope::driver::token::Token;
    use dope::runtime::profile::Throughput;
    use dope::runtime::{Dispatcher, Executor, Idle, ShutdownTrigger};
    use dope::{DriverContext, Event};

    use super::{Stamp, TimerState, Updater};

    struct App<'a> {
        stamp: &'a Stamp,
        date: Updater<7>,
    }

    impl<'d> Dispatcher<'d> for App<'_> {
        fn dispatch(
            mut self: Pin<&mut Self>,
            event: Event<'d>,
            driver: &mut DriverContext<'_, 'd>,
        ) {
            let this = self.as_mut().get_mut();
            this.date.dispatch(event, this.stamp, driver);
        }

        fn activate(self: Pin<&mut Self>, _target: Token, _driver: &mut DriverContext<'_, 'd>) {}

        fn pre_park(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
            self.as_mut().get_mut().date.pre_park(driver);
        }

        fn idle(self: Pin<&Self>) -> Idle {
            self.get_ref().date.idle()
        }

        fn shutdown(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
            self.as_mut().get_mut().date.shutdown(driver);
        }
    }

    #[test]
    fn updater_refreshes_from_the_recurring_driver_event() {
        assert_eq!(
            std::mem::size_of::<Updater<7>>(),
            std::mem::size_of::<TimerState>(),
        );

        let stamp = pin!(Stamp::new());
        let initial = stamp.load();
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
                    .with_app(
                        App {
                            stamp: stamp.as_ref().get_ref(),
                            date: Updater::new(),
                        },
                        |mut app| app.run(),
                    )
                    .expect("run updater");
            });
        shutdown.join().expect("shutdown thread");

        assert_ne!(stamp.load(), initial);
    }
}
