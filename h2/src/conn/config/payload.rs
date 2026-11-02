#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Error {
    ZeroData,
    ZeroHeaders,
    SlotOverflow { data: usize, headers: usize },
    IngressOverflow { slots: usize },
    SegmentOverflow { data: usize, ingress: usize },
    ArenaTooSmall { required: usize, actual: usize },
    RetentionOverflow { segments: usize, extra: usize },
}
