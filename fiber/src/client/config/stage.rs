/// Request stages with optional caps inside the total timeout.
#[derive(Clone, Copy, Debug)]
pub enum Stage {
    Dispatch,
    Head,
    Decode,
}
