use crate::error::Result;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Known {
    Host,
    Expect,
    Connection,
    ContentLength,
    TransferEncoding,
    AcceptEncoding,
}

pub trait Visitor {
    type Parsed;

    const WANTS_KNOWN: bool = false;

    fn start_line(&mut self, parsed: &Self::Parsed, raw: &[u8]) -> Result<()> {
        let _ = (parsed, raw);
        Ok(())
    }
    fn known(&mut self, key: Known, value: &[u8]) -> Result<()> {
        let _ = (key, value);
        Ok(())
    }
    fn unknown(&mut self, name: &[u8], value: &[u8]) -> Result<()> {
        let _ = (name, value);
        Ok(())
    }
}

impl Visitor for () {
    type Parsed = ();
}
