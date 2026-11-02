use crate::field::metadata::block;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Request(block::Plan),
    Response(block::Response),
    Other,
    Rejected(block::Validation),
}

impl Outcome {
    pub const fn validation(&self) -> block::Validation {
        match self {
            Self::Request(_) | Self::Response(_) | Self::Other => block::Validation::Valid,
            Self::Rejected(check) => *check,
        }
    }

    pub const fn plan(&self) -> Option<block::Plan> {
        match self {
            Self::Request(plan) => Some(*plan),
            Self::Response(_) | Self::Other | Self::Rejected(_) => None,
        }
    }

    pub const fn response(&self) -> Option<block::Response> {
        match self {
            Self::Response(response) => Some(*response),
            Self::Request(_) | Self::Other | Self::Rejected(_) => None,
        }
    }

    pub fn into_parts(self) -> (Option<block::Plan>, block::Validation) {
        (self.plan(), self.validation())
    }
}
