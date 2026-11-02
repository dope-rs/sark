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
    Integer,
    Dot,
    Fraction,
    Exponent,
    ExponentSign,
    ExponentDigits,
}

impl NumberState {
    fn next(self, byte: u8) -> crate::Result<Self> {
        match self {
            Self::Start if byte == b'-' => Ok(Self::Minus),
            Self::Start if byte == b'0' => Ok(Self::Zero),
            Self::Start if byte.is_ascii_digit() => Ok(Self::Integer),
            Self::Start => Err(Error::Invalid),
            Self::Minus if byte == b'0' => Ok(Self::Zero),
            Self::Minus if matches!(byte, b'1'..=b'9') => Ok(Self::Integer),
            Self::Minus => Err(Error::Invalid),
            Self::Zero if byte == b'.' => Ok(Self::Dot),
            Self::Zero if matches!(byte, b'e' | b'E') => Ok(Self::Exponent),
            Self::Zero => Err(Error::Invalid),
            Self::Integer if byte == b'.' => Ok(Self::Dot),
            Self::Integer if matches!(byte, b'e' | b'E') => Ok(Self::Exponent),
            Self::Integer if byte.is_ascii_digit() => Ok(Self::Integer),
            Self::Integer => Err(Error::Invalid),
            Self::Dot if byte.is_ascii_digit() => Ok(Self::Fraction),
            Self::Dot => Err(Error::Invalid),
            Self::Fraction if matches!(byte, b'e' | b'E') => Ok(Self::Exponent),
            Self::Fraction if byte.is_ascii_digit() => Ok(Self::Fraction),
            Self::Fraction => Err(Error::Invalid),
            Self::Exponent if matches!(byte, b'+' | b'-') => Ok(Self::ExponentSign),
            Self::Exponent if byte.is_ascii_digit() => Ok(Self::ExponentDigits),
            Self::Exponent => Err(Error::Invalid),
            Self::ExponentSign if byte.is_ascii_digit() => Ok(Self::ExponentDigits),
            Self::ExponentSign => Err(Error::Invalid),
            Self::ExponentDigits if byte.is_ascii_digit() => Ok(Self::ExponentDigits),
            Self::ExponentDigits => Err(Error::Invalid),
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
                let value = if matches!(next, NumberState::Zero | NumberState::Integer) {
                    value.and_then(|value| {
                        value
                            .checked_mul(10)
                            .and_then(|value| value.checked_add(u64::from(byte - b'0')))
                    })
                } else {
                    None
                };
                Ok(Self::Number { state: next, value })
            }
        }
    }

    pub(crate) fn finish(self) -> crate::Result<ValueKind> {
        use std::cmp::Ordering;

        match self {
            Self::Literal { literal, matched } => match matched.cmp(&literal.len()) {
                Ordering::Equal => Ok(literal.finish()),
                Ordering::Less | Ordering::Greater => Err(Error::Invalid),
            },
            Self::Number { state, value } => match state {
                NumberState::Zero
                | NumberState::Integer
                | NumberState::Fraction
                | NumberState::ExponentDigits => match value {
                    Some(value) => Ok(ValueKind::Unsigned(value)),
                    None => Ok(ValueKind::Number),
                },
                NumberState::Start
                | NumberState::Minus
                | NumberState::Dot
                | NumberState::Exponent
                | NumberState::ExponentSign => Err(Error::Invalid),
            },
        }
    }
}
