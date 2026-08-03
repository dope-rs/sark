#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Overflow,
    ZeroIncrement,
    Stalled,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Window {
    pub value: i32,
}

impl Window {
    pub const INITIAL: i32 = 65_535;
    pub const MAX: i32 = 0x7fff_ffff;

    pub fn new() -> Self {
        Self {
            value: Self::INITIAL,
        }
    }

    pub fn with(value: i32) -> Self {
        Self { value }
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

    pub fn consume(&mut self, n: usize) -> Result<(), Error> {
        if n > self.available() {
            return Err(Error::Stalled);
        }
        let n_i32 = i32::try_from(n).map_err(|_| Error::Overflow)?;
        self.value -= n_i32;
        Ok(())
    }

    pub fn increase(&mut self, n: u32) -> Result<(), Error> {
        if n == 0 {
            return Err(Error::ZeroIncrement);
        }
        let n_i32 = i32::try_from(n).map_err(|_| Error::Overflow)?;
        let next = (self.value as i64) + (n_i32 as i64);
        if next > Self::MAX as i64 {
            return Err(Error::Overflow);
        }
        self.value = next as i32;
        Ok(())
    }

    pub fn adjust_initial(&mut self, delta: i32) -> Result<(), Error> {
        let next = (self.value as i64) + (delta as i64);
        if next > Self::MAX as i64 {
            return Err(Error::Overflow);
        }
        if next < i32::MIN as i64 {
            return Err(Error::Overflow);
        }
        self.value = next as i32;
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
    amount: i32,
}

pub(crate) struct ReleasePair<'a> {
    pub conn: &'a mut u32,
    pub stream: &'a mut u32,
    pub conn_threshold: u32,
    pub stream_threshold: u32,
}

pub(crate) struct ReceivePlan<'a> {
    debit: Debit<'a>,
    releases: ReleasePair<'a>,
    conn_window: i32,
    stream_window: i32,
    conn_pending: u32,
    stream_pending: u32,
    conn_increment: Option<u32>,
    stream_increment: Option<u32>,
}

impl<'a> Pair<'a> {
    pub fn available(&self) -> usize {
        self.conn.available().min(self.stream.available())
    }

    pub fn consume(&mut self, n: usize) -> Result<(), Error> {
        Pair {
            conn: &mut *self.conn,
            stream: &mut *self.stream,
        }
        .debit(n)?
        .commit();
        Ok(())
    }

    fn debit(self, amount: usize) -> Result<Debit<'a>, Error> {
        let debit = self.debit_up_to(amount);
        if debit.len() == amount {
            Ok(debit)
        } else {
            Err(Error::Stalled)
        }
    }

    pub(crate) fn debit_up_to(self, limit: usize) -> Debit<'a> {
        Debit {
            amount: self.available().min(limit) as i32,
            pair: self,
        }
    }

    pub(crate) fn receive(
        self,
        amount: usize,
        releases: ReleasePair<'a>,
        replenish_stream: bool,
    ) -> Result<ReceivePlan<'a>, Error> {
        let debit = self.debit(amount)?;
        let (conn_window, conn_pending, conn_increment) = receive_endpoint(
            debit.pair.conn.value,
            *releases.conn,
            releases.conn_threshold,
            debit.amount,
            true,
        )?;
        let (stream_window, stream_pending, stream_increment) = receive_endpoint(
            debit.pair.stream.value,
            *releases.stream,
            releases.stream_threshold,
            debit.amount,
            replenish_stream,
        )?;
        Ok(ReceivePlan {
            debit,
            releases,
            conn_window,
            stream_window,
            conn_pending,
            stream_pending,
            conn_increment,
            stream_increment,
        })
    }
}

impl Debit<'_> {
    pub(crate) fn len(&self) -> usize {
        self.amount as usize
    }

    pub(crate) fn commit(self) {
        self.pair.conn.value -= self.amount;
        self.pair.stream.value -= self.amount;
    }
}

impl ReceivePlan<'_> {
    pub(crate) const fn conn_increment(&self) -> Option<u32> {
        self.conn_increment
    }

    pub(crate) const fn stream_increment(&self) -> Option<u32> {
        self.stream_increment
    }

    pub(crate) fn commit(self) {
        self.debit.pair.conn.value = self.conn_window;
        self.debit.pair.stream.value = self.stream_window;
        *self.releases.conn = self.conn_pending;
        *self.releases.stream = self.stream_pending;
    }
}

fn receive_endpoint(
    window: i32,
    pending: u32,
    threshold: u32,
    amount: i32,
    replenish: bool,
) -> Result<(i32, u32, Option<u32>), Error> {
    if amount == 0 {
        return Ok((window, pending, None));
    }
    let pending = pending.saturating_add(amount as u32);
    let consumed = window - amount;
    if !replenish || pending < threshold {
        return Ok((consumed, pending, None));
    }
    if pending > Window::MAX as u32 {
        return Err(Error::Overflow);
    }
    let next = i64::from(consumed) + i64::from(pending);
    if next > i64::from(Window::MAX) {
        return Err(Error::Overflow);
    }
    Ok((next as i32, 0, Some(pending)))
}
