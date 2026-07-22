use super::KnownHeader;
use crate::error::Result;

pub trait Visitor {
    type Parsed;

    const WANTS_KNOWN: bool = false;

    fn start_line(&mut self, parsed: &Self::Parsed, raw: &[u8]) -> Result<()> {
        let _ = (parsed, raw);
        Ok(())
    }
    fn known(&mut self, key: KnownHeader, value: &[u8]) -> Result<()> {
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
