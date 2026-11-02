/// Why a server-mode capacity is invalid.

#[derive(Debug)]
pub enum Limit {
    Zero,
    Domain,
}
