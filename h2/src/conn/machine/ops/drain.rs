#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Drain {
    Complete,
    Pending,
}
