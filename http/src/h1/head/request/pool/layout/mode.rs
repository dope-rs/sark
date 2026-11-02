use crate::h1::{head::request::pool::Source, switch::Intent};

#[derive(Clone, Copy)]
#[repr(u8)]
pub(super) enum Mode {
    Compact,
    Connect,
    Upgrade,
    Head,
    HeadConnect,
    HeadUpgrade,
}

impl Mode {
    pub(super) const fn new(source: Source, intent: Intent) -> Self {
        match (source, intent) {
            (Source::Compact, Intent::None) => Self::Compact,
            (Source::Compact, Intent::Connect) => Self::Connect,
            (Source::Compact, Intent::Upgrade) => Self::Upgrade,
            (Source::Head, Intent::None) => Self::Head,
            (Source::Head, Intent::Connect) => Self::HeadConnect,
            (Source::Head, Intent::Upgrade) => Self::HeadUpgrade,
        }
    }

    pub(super) const fn source(self) -> Source {
        match self {
            Self::Compact | Self::Connect | Self::Upgrade => Source::Compact,
            Self::Head | Self::HeadConnect | Self::HeadUpgrade => Source::Head,
        }
    }

    pub(super) const fn transition(self) -> Intent {
        match self {
            Self::Compact | Self::Head => Intent::None,
            Self::Connect | Self::HeadConnect => Intent::Connect,
            Self::Upgrade | Self::HeadUpgrade => Intent::Upgrade,
        }
    }
}
