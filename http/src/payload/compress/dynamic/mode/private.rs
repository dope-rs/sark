use crate::payload::compress::dynamic::table::slot;

pub trait Private {
    type Hash;
    type Index: Copy + Eq;
    type Insert: Default;
    type Record: Copy;

    const ARENAS: usize;

    fn slot(record: &Self::Record) -> slot::Slot;
}
