use sark_h2::stream::Key;

#[derive(Debug)]
pub struct Chunk {
    pub stream: Key,
    pub body_at: usize,
    pub pointer: usize,
    pub len: usize,
}
