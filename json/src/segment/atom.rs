use crate::{Error, segment::ValueKind};

#[derive(Clone, Copy)]
pub(crate) enum Atom {
    Literal {
        literal: Literal,
        matched: usize,
    },
    Number {
        state: NumberState,
        value: Option<u64>,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum Literal {
    True,
    False,
    Null,
}

impl Literal {
    fn byte(self, index: usize) -> Option<u8> {
        match self {
            Self::True => b"true".get(index).copied(),
            Self::False => b"false".get(index).copied(),
            Self::Null => b"null".get(index).copied(),
        }
    }

    fn finish(self) -> ValueKind {
        match self {
            Self::True => ValueKind::True,
            Self::False => ValueKind::False,
            Self::Null => ValueKind::Null,
        }
    }

    const fn len(self) -> usize {
        match self {
            Self::True | Self::Null => 4,
            Self::False => 5,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum NumberState {
    Start,
    Minus,
    Zero,
    NegativeZero,
    Integer,
    NegativeInteger,
    Dot,
    Fraction,
    Exponent,
    ExponentSign,
    ExponentDigits,
}

impl NumberState {
    #[inline(always)]
    fn next(self, byte: u8) -> crate::Result<Self> {
        match self {
            Self::Start if byte == b'-' => Ok(Self::Minus),
            Self::Start if byte == b'0' => Ok(Self::Zero),
            Self::Start if byte.is_ascii_digit() => Ok(Self::Integer),
            Self::Minus if byte == b'0' => Ok(Self::NegativeZero),
            Self::Minus if matches!(byte, b'1'..=b'9') => Ok(Self::NegativeInteger),
            Self::Zero | Self::NegativeZero if byte == b'.' => Ok(Self::Dot),
            Self::Zero | Self::NegativeZero if matches!(byte, b'e' | b'E') => Ok(Self::Exponent),
            Self::Integer | Self::NegativeInteger if byte == b'.' => Ok(Self::Dot),
            Self::Integer | Self::NegativeInteger if matches!(byte, b'e' | b'E') => {
                Ok(Self::Exponent)
            }
            Self::Integer | Self::NegativeInteger if byte.is_ascii_digit() => Ok(self),
            Self::Dot if byte.is_ascii_digit() => Ok(Self::Fraction),
            Self::Fraction if matches!(byte, b'e' | b'E') => Ok(Self::Exponent),
            Self::Fraction if byte.is_ascii_digit() => Ok(Self::Fraction),
            Self::Exponent if matches!(byte, b'+' | b'-') => Ok(Self::ExponentSign),
            Self::Exponent if byte.is_ascii_digit() => Ok(Self::ExponentDigits),
            Self::ExponentSign if byte.is_ascii_digit() => Ok(Self::ExponentDigits),
            Self::ExponentDigits if byte.is_ascii_digit() => Ok(Self::ExponentDigits),
            _ => Err(Error::Invalid),
        }
    }
}

impl Atom {
    pub(crate) fn new(byte: u8, retain: bool) -> crate::Result<Self> {
        let literal = match byte {
            b't' => Literal::True,
            b'f' => Literal::False,
            b'n' => Literal::Null,
            b'-' | b'0'..=b'9' => {
                return Ok(Self::Number {
                    state: NumberState::Start,
                    value: retain.then_some(0),
                });
            }
            _ => return Err(Error::Invalid),
        };
        Ok(Self::Literal {
            literal,
            matched: 0,
        })
    }

    #[inline(always)]
    pub(crate) fn step(self, byte: u8) -> crate::Result<Self> {
        match self {
            Self::Literal { literal, matched } => {
                if literal.byte(matched) != Some(byte) {
                    return Err(Error::Invalid);
                }
                Ok(Self::Literal {
                    literal,
                    matched: matched + 1,
                })
            }
            Self::Number { state, value } => {
                let next = state.next(byte)?;
                let value = if matches!(
                    next,
                    NumberState::Zero
                        | NumberState::NegativeZero
                        | NumberState::Integer
                        | NumberState::NegativeInteger
                ) {
                    value.and_then(|value| {
                        value
                            .checked_mul(10)
                            .and_then(|value| value.checked_add(u64::from(byte - b'0')))
                    })
                } else if matches!(next, NumberState::Minus) {
                    value
                } else {
                    None
                };
                Ok(Self::Number { state: next, value })
            }
        }
    }

    pub(crate) fn finish(self) -> crate::Result<ValueKind> {
        match self {
            Self::Literal { literal, matched } if matched == literal.len() => Ok(literal.finish()),
            Self::Number {
                state: NumberState::NegativeZero | NumberState::NegativeInteger,
                value: Some(value),
            } => Ok(ValueKind::Negative(value)),
            Self::Number {
                state:
                    NumberState::Zero
                    | NumberState::NegativeZero
                    | NumberState::Integer
                    | NumberState::NegativeInteger
                    | NumberState::Fraction
                    | NumberState::ExponentDigits,
                value,
            } => match value {
                Some(value) => Ok(ValueKind::Unsigned(value)),
                None => Ok(ValueKind::Number),
            },
            _ => Err(Error::Invalid),
        }
    }
}
