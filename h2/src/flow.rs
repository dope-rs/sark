use sark_core::manifold::stream::Boundary;

use crate::{frame::numbers, stream};

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct Increase {
    pub value: i32,
    pub committed: i32,
    pub inc: numbers::Increment,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct Adjust {
    pub value: i32,
    pub committed: i32,
    pub delta: i32,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Receive {
    Stalled { available: i32, amount: u32 },
    Refill { spent: i32, held: u32 },
}

#[repr(transparent)]
#[derive(Copy, Clone)]
pub(crate) struct Target(u32);

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Window {
    /// Effective credit available to the scheduler after staged reservations.
    value: i32,
    /// Protocol credit including cancellable DATA reservations, always >= `value`.
    /// Reserving lowers only `value`; accepting or canceling consumes that exact
    /// reservation. Other operations shift both equally or replace both together.
    committed: i32,
}

impl Target {
    pub(crate) const fn new(value: u32) -> Option<Self> {
        if value <= Window::MAX as u32 {
            Some(Self(value))
        } else {
            None
        }
    }

    pub(crate) const fn get(self) -> u32 {
        self.0
    }

    pub(crate) const fn low(self) -> u32 {
        let half = self.0 / 2;
        if half == 0 { 1 } else { half }
    }

    pub(crate) fn increment(self) -> Option<numbers::Increment> {
        self.0
            .checked_sub(Window::INITIAL as u32)
            .and_then(numbers::Increment::new)
    }
}

impl Window {
    pub const INITIAL: i32 = 65_535;
    pub const MAX: i32 = 0x7fff_ffff;

    pub fn new() -> Self {
        Self::with(Self::INITIAL)
    }

    pub const fn with(value: i32) -> Self {
        Self {
            value,
            committed: value,
        }
    }

    pub(crate) const fn for_target(target: Target) -> Self {
        let target = target.get() as i32;
        let value = if target < Self::INITIAL {
            Self::INITIAL
        } else {
            target
        };
        Self::with(value)
    }

    pub const fn value(self) -> i32 {
        self.value
    }

    pub fn available(self) -> usize {
        if self.value < 0 {
            0
        } else {
            self.value as usize
        }
    }

    pub fn is_stalled(self) -> bool {
        self.value <= 0
    }

    pub fn increase(&mut self, inc: numbers::Increment) -> Result<(), Increase> {
        let n_i32 = inc.into_i32();
        let bound = i64::from(self.committed) + i64::from(n_i32);
        let next = (self.value as i64) + i64::from(n_i32);
        if bound > Self::MAX as i64 {
            return Err(Increase {
                value: self.value,
                committed: self.committed,
                inc,
            });
        }
        self.value = next as i32;
        self.committed = bound as i32;
        Ok(())
    }

    pub fn adjust(&mut self, delta: i32) -> Result<(), Adjust> {
        let next = (self.value as i64) + (delta as i64);
        let bound = i64::from(self.committed) + i64::from(delta);
        if bound > Self::MAX as i64 || next < i32::MIN as i64 {
            return Err(Adjust {
                value: self.value,
                committed: self.committed,
                delta,
            });
        }
        self.value = next as i32;
        self.committed = bound as i32;
        Ok(())
    }
}

impl Default for Window {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Pair<'a> {
    pub conn: &'a mut Window,
    pub stream: &'a mut Window,
}

pub(crate) struct Debit<'a> {
    pair: Pair<'a>,
    stream: stream::Key,
    amount: i32,
}

#[must_use = "reserved flow credit must be accepted, canceled, or transferred"]
#[derive(PartialEq, Eq)]
pub(crate) struct Reserved {
    stream: stream::Key,
    amount: u32,
}

pub(crate) struct ReleasePair<'a> {
    pub conn: &'a mut u32,
    pub stream: &'a mut u32,
    pub conn_threshold: u32,
    pub stream_threshold: u32,
}

pub(crate) struct Endpoint<'a> {
    window: &'a mut Window,
    pending: &'a mut u32,
    next: i32,
    held: u32,
    bump: Option<numbers::Increment>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum Refill {
    Enabled,
    Disabled,
}

const _: () = {
    assert!(core::mem::size_of::<Refill>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Refill>() == core::mem::align_of::<bool>());
};

pub(crate) struct Admission<'a> {
    pub conn: Endpoint<'a>,
    pub stream: Result<Endpoint<'a>, Receive>,
}

impl<'a> Pair<'a> {
    pub fn available(&self) -> usize {
        self.conn.available().min(self.stream.available())
    }

    pub(crate) fn debit_up_to(self, stream: stream::Key, limit: usize) -> Debit<'a> {
        Debit {
            amount: self.available().min(limit) as i32,
            pair: self,
            stream,
        }
    }

    pub(crate) fn receive_data(
        self,
        amount: numbers::Length,
        releases: ReleasePair<'a>,
        boundary: Boundary,
    ) -> Result<Admission<'a>, Receive> {
        let conn = Endpoint::new(
            self.conn,
            releases.conn,
            releases.conn_threshold,
            amount,
            Refill::Enabled,
        )?;
        let refill = match boundary {
            Boundary::More => Refill::Enabled,
            Boundary::End => Refill::Disabled,
        };
        let stream = Endpoint::new(
            self.stream,
            releases.stream,
            releases.stream_threshold,
            amount,
            refill,
        );
        Ok(Admission { conn, stream })
    }
}

impl Debit<'_> {
    pub(crate) fn len(&self) -> usize {
        self.amount as usize
    }

    pub(crate) fn commit(self) {
        self.pair.conn.value -= self.amount;
        self.pair.conn.committed -= self.amount;
        self.pair.stream.value -= self.amount;
        self.pair.stream.committed -= self.amount;
    }

    pub(crate) fn reserve(self) -> Reserved {
        let amount = self.amount as u32;
        self.pair.conn.value -= self.amount;
        self.pair.stream.value -= self.amount;
        Reserved {
            stream: self.stream,
            amount,
        }
    }
}

impl Reserved {
    pub(crate) const fn stream(&self) -> stream::Key {
        self.stream
    }

    pub(crate) const fn amount(&self) -> usize {
        self.amount as usize
    }

    pub(crate) fn accept(self, conn: &mut Window, stream: Option<&mut Window>) {
        Self::accept_one(conn, self.amount);
        if let Some(stream) = stream {
            Self::accept_one(stream, self.amount);
        }
    }

    pub(crate) fn cancel(self, conn: &mut Window, stream: Option<&mut Window>) {
        Self::cancel_one(conn, self.amount);
        if let Some(stream) = stream {
            Self::cancel_one(stream, self.amount);
        }
    }

    fn accept_one(window: &mut Window, amount: u32) {
        let next = i64::from(window.committed) - i64::from(amount);
        window.committed = next as i32;
    }

    fn cancel_one(window: &mut Window, amount: u32) {
        let next = i64::from(window.value) + i64::from(amount);
        window.value = next as i32;
    }
}

const _: () = {
    assert!(core::mem::size_of::<Window>() == core::mem::size_of::<[i32; 2]>());
    assert!(core::mem::size_of::<Increase>() == 3 * core::mem::size_of::<u32>());
    assert!(core::mem::size_of::<Adjust>() == 3 * core::mem::size_of::<u32>());
    assert!(core::mem::size_of::<Receive>() == 3 * core::mem::size_of::<u32>());
    assert!(core::mem::size_of::<Reserved>() == 3 * core::mem::size_of::<u32>());
    assert!(core::mem::size_of::<Option<Reserved>>() == core::mem::size_of::<Reserved>());
};

impl<'a> Endpoint<'a> {
    fn new(
        window: &'a mut Window,
        pending: &'a mut u32,
        limit: u32,
        amount: numbers::Length,
        refill: Refill,
    ) -> Result<Self, Receive> {
        let amount = amount.into_u32();
        let debit = amount as i32;
        if debit > window.value.max(0) {
            return Err(Receive::Stalled {
                available: window.value,
                amount,
            });
        }
        if amount == 0 {
            return Ok(Self {
                next: window.value,
                held: *pending,
                bump: None,
                window,
                pending,
            });
        }
        // Receive windows conserve value + pending from the admitted initial limit.
        let held = *pending + amount;
        let spent = window.value - debit;
        let (next, held, bump) = match refill {
            Refill::Disabled => (spent, held, None),
            Refill::Enabled if held < limit => (spent, held, None),
            Refill::Enabled => {
                let Some(bump) = numbers::Increment::new(held) else {
                    return Err(Receive::Refill { spent, held });
                };
                let next = spent + bump.into_i32();
                (next, 0, Some(bump))
            }
        };
        Ok(Self {
            window,
            pending,
            next,
            held,
            bump,
        })
    }

    pub(crate) const fn bump(&self) -> Option<numbers::Increment> {
        self.bump
    }

    pub(crate) fn commit(self) {
        self.window.value = self.next;
        self.window.committed = self.next;
        *self.pending = self.held;
    }
}

impl Window {
    pub(crate) fn receive_data<'a>(
        &'a mut self,
        amount: numbers::Length,
        pending: &'a mut u32,
        limit: u32,
    ) -> Result<Endpoint<'a>, Receive> {
        Endpoint::new(self, pending, limit, amount, Refill::Enabled)
    }
}
