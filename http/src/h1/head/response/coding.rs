/// Content decoding supported by the complete response field section.
/// Multiple or unknown content codings, or remaining transfer codings, retain
/// their original representation.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Coding {
    #[default]
    None,
    Gzip,
    Unsupported,
}

impl Coding {
    pub(super) fn observe(self, value: &[u8]) -> Self {
        if self == Self::None && value.eq_ignore_ascii_case(b"gzip") {
            Self::Gzip
        } else {
            Self::Unsupported
        }
    }
}
