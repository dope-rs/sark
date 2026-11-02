#[derive(Clone, Copy, Debug)]
pub enum Error {
    Input { actual: usize },
    Escape { current: usize },
}
