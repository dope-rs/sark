/// Result of one bounded protocol step.
#[derive(Clone, Copy, PartialEq, Eq)]
#[must_use = "pending protocol work must be driven again"]
pub enum Step<T> {
    /// One semantic item was transferred to the caller.
    Item(T),
    /// Work progressed or remains queued, but no item is available yet.
    More,
    /// Neither an item nor resumable protocol work remains.
    Idle,
}
