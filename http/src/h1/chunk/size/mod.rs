use crate::{
    field::{self, text},
    h1::chunk::{self, Error},
};

mod line;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
enum Syntax {
    #[default]
    Start,
    Digits,
    AfterDigits,
    ExtensionNameStart,
    ExtensionName,
    AfterExtensionName,
    ExtensionValueStart,
    ExtensionToken,
    ExtensionValueComplete,
    AfterExtensionValue,
    ExtensionQuoted,
    ExtensionQuotedEscape,
}

impl Syntax {
    const fn can_end(self) -> bool {
        matches!(
            self,
            Self::Digits
                | Self::ExtensionName
                | Self::ExtensionToken
                | Self::ExtensionValueComplete
        )
    }

    const fn error(self) -> chunk::Syntax {
        match self {
            Self::Start | Self::Digits => chunk::Syntax::Size,
            Self::AfterDigits => chunk::Syntax::SizeTail,
            Self::ExtensionNameStart => chunk::Syntax::NameStart,
            Self::ExtensionName => chunk::Syntax::Name,
            Self::AfterExtensionName => chunk::Syntax::NameTail,
            Self::ExtensionValueStart => chunk::Syntax::ValueStart,
            Self::ExtensionToken => chunk::Syntax::Token,
            Self::ExtensionValueComplete => chunk::Syntax::ValueEnd,
            Self::AfterExtensionValue => chunk::Syntax::ValueTail,
            Self::ExtensionQuoted => chunk::Syntax::Quoted,
            Self::ExtensionQuotedEscape => chunk::Syntax::Escape,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Progress {
    Pending,
    Complete,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Size {
    value: usize,
    line: line::Line,
    syntax: Syntax,
}

impl Size {
    pub(super) const fn new() -> Self {
        Self {
            value: 0,
            line: line::Line::new(),
            syntax: Syntax::Start,
        }
    }

    pub(super) const fn value(self) -> usize {
        self.value
    }

    pub(super) fn scan(
        mut self,
        byte: u8,
        offset: usize,
        limit: usize,
    ) -> Result<(Self, Progress), Error> {
        self.line = self.line.try_add(limit)?;
        if byte == b'\r' {
            if !self.syntax.can_end() {
                return Err(Error::Syntax {
                    offset,
                    syntax: self.syntax.error(),
                    actual: byte,
                });
            }
            Ok((self, Progress::Complete))
        } else {
            let next = self.parse(byte, offset)?;
            Ok((next, Progress::Pending))
        }
    }

    pub(super) fn finish(mut self, byte: u8, offset: usize, limit: usize) -> Result<Self, Error> {
        self.line = self.line.try_add(limit)?;
        if byte != b'\n' {
            return Err(Error::Syntax {
                offset,
                syntax: chunk::Syntax::SizeLf,
                actual: byte,
            });
        }
        Ok(self)
    }

    fn parse(self, byte: u8, offset: usize) -> Result<Self, Error> {
        match self.syntax {
            Syntax::Start | Syntax::Digits => self.digit(byte, offset),
            Syntax::AfterDigits => self.tail(byte, offset, chunk::Syntax::SizeTail),
            Syntax::ExtensionNameStart => self.name_start(byte, offset),
            Syntax::ExtensionName | Syntax::ExtensionToken if field::Error::is_name_byte(byte) => {
                Ok(self)
            }
            Syntax::ExtensionName | Syntax::AfterExtensionName => self.name_tail(byte, offset),
            Syntax::ExtensionValueStart => self.value_start(byte, offset),
            Syntax::ExtensionToken | Syntax::ExtensionValueComplete => self.value_end(byte, offset),
            Syntax::AfterExtensionValue => self.tail(byte, offset, chunk::Syntax::ValueTail),
            Syntax::ExtensionQuoted => self.quoted(byte, offset),
            Syntax::ExtensionQuotedEscape => self.escape(byte, offset),
        }
    }

    fn digit(mut self, byte: u8, offset: usize) -> Result<Self, Error> {
        if let Some(digit) = text::hex(byte) {
            let value = self
                .value
                .checked_mul(16)
                .map(|value| value | usize::from(digit))
                .ok_or(Error::Overflow {
                    axis: chunk::Axis::Size,
                    value: self.value,
                    added: usize::from(digit),
                })?;
            self.value = value;
            self.syntax = Syntax::Digits;
        } else if matches!(byte, b' ' | b'\t') && matches!(self.syntax, Syntax::Digits) {
            self.syntax = Syntax::AfterDigits;
        } else if byte == b';' && matches!(self.syntax, Syntax::Digits) {
            self.syntax = Syntax::ExtensionNameStart;
        } else {
            return Err(Error::Syntax {
                offset,
                syntax: chunk::Syntax::Size,
                actual: byte,
            });
        }
        Ok(self)
    }

    fn tail(mut self, byte: u8, offset: usize, syntax: chunk::Syntax) -> Result<Self, Error> {
        match byte {
            b' ' | b'\t' => Ok(self),
            b';' => {
                self.syntax = Syntax::ExtensionNameStart;
                Ok(self)
            }
            actual => Err(Error::Syntax {
                offset,
                syntax,
                actual,
            }),
        }
    }

    fn name_start(mut self, byte: u8, offset: usize) -> Result<Self, Error> {
        if matches!(byte, b' ' | b'\t') {
            Ok(self)
        } else if field::Error::is_name_byte(byte) {
            self.syntax = Syntax::ExtensionName;
            Ok(self)
        } else {
            Err(Error::Syntax {
                offset,
                syntax: chunk::Syntax::NameStart,
                actual: byte,
            })
        }
    }

    fn name_tail(mut self, byte: u8, offset: usize) -> Result<Self, Error> {
        self.syntax = match byte {
            b' ' | b'\t' => Syntax::AfterExtensionName,
            b'=' => Syntax::ExtensionValueStart,
            b';' => Syntax::ExtensionNameStart,
            actual => {
                return Err(Error::Syntax {
                    offset,
                    syntax: self.syntax.error(),
                    actual,
                });
            }
        };
        Ok(self)
    }

    fn value_start(mut self, byte: u8, offset: usize) -> Result<Self, Error> {
        if matches!(byte, b' ' | b'\t') {
            Ok(self)
        } else if field::Error::is_name_byte(byte) {
            self.syntax = Syntax::ExtensionToken;
            Ok(self)
        } else if byte == b'"' {
            self.syntax = Syntax::ExtensionQuoted;
            Ok(self)
        } else {
            Err(Error::Syntax {
                offset,
                syntax: chunk::Syntax::ValueStart,
                actual: byte,
            })
        }
    }

    fn value_end(mut self, byte: u8, offset: usize) -> Result<Self, Error> {
        let syntax = match byte {
            b' ' | b'\t' => Syntax::AfterExtensionValue,
            b';' => Syntax::ExtensionNameStart,
            actual => {
                return Err(Error::Syntax {
                    offset,
                    syntax: self.syntax.error(),
                    actual,
                });
            }
        };
        self.syntax = syntax;
        Ok(self)
    }

    fn quoted(mut self, byte: u8, offset: usize) -> Result<Self, Error> {
        match byte {
            b'"' => self.syntax = Syntax::ExtensionValueComplete,
            b'\\' => self.syntax = Syntax::ExtensionQuotedEscape,
            b'\t' | b' ' | b'!' | 0x23..=0x5b | 0x5d..=0x7e | 0x80..=0xff => {}
            actual => {
                return Err(Error::Syntax {
                    offset,
                    syntax: chunk::Syntax::Quoted,
                    actual,
                });
            }
        }
        Ok(self)
    }

    fn escape(mut self, byte: u8, offset: usize) -> Result<Self, Error> {
        if matches!(byte, b'\t' | b' ' | 0x21..=0x7e | 0x80..=0xff) {
            self.syntax = Syntax::ExtensionQuoted;
            Ok(self)
        } else {
            Err(Error::Syntax {
                offset,
                syntax: chunk::Syntax::Escape,
                actual: byte,
            })
        }
    }
}
