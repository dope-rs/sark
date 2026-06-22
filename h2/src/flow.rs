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

impl Pair<'_> {
    pub fn available(&self) -> usize {
        self.conn.available().min(self.stream.available())
    }

    pub fn consume(&mut self, n: usize) -> Result<(), Error> {
        if n > self.available() {
            return Err(Error::Stalled);
        }
        self.conn.consume(n)?;
        self.stream.consume(n)?;
        Ok(())
    }
}
