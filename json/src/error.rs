use std::{char, fmt::Display, io, str};

/// An error in JSON processing.
#[derive(Debug)]
pub struct Error {
    /// The kind of error.
    pub(super) kind: Kind,
    /// The position in the input at which the error occurred.
    pub(super) pos: usize,
}

impl Error {
    pub fn kind(&self) -> &Kind {
        &self.kind
    }

    pub fn pos(&self) -> usize {
        self.pos
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "JSON parsing error at byte offset {}: {}",
            self.pos, self.kind
        )
    }
}

/// The kind of error in processing.
#[derive(Debug)]
pub enum Kind {
    /// I/O-related error, like stream closing.
    Io(io::Error),
    /// Input ended unexpectedly when more data was needed.
    UnexpectedEOF,
    /// A byte is invalid at the current position.
    InvalidByte {
        /// The encountered, invalid byte.
        byte: u8,
        /// Reason it is invalid at this position.
        reason: String,
    },
    /// A provided escape sequence is invalid, like `\a`.
    InvalidEscapeSequence(u8),
    /// A provided character is outside the hexadecimal alphabet, such as `z`.
    InvalidHexCharacter(char),
    /// The input contained invalid Unicode.
    UnicodeError(UnicodeError),
}

/// Invalid Unicode was encountered.
#[derive(Debug)]
pub enum UnicodeError {
    /// A UTF-16 surrogate pair codepoint sequence is invalid.
    InvalidUTF16(char::DecodeUtf16Error),
    /// A high surrogate is unpaired (no valid low surrogate following).
    ///
    /// Subsequent JSON-escaped Unicode code points might form a UTF-16 surrogate pair,
    /// such as `"\ud834\udd1e"`, but the pairing might be invalid.
    ///
    /// The unpaired high surrogate codepoint is provided.
    UnpairedHighSurrogate(u16),
    /// A low surrogate occurred without a preceding high surrogate. It is "orphaned".
    UnpairedLowSurrogate(u16),
    /// A byte sequence is invalid UTF-8.
    InvalidUTF8(str::Utf8Error),
    /// The start of a non-ASCII (**assumed UTF-8**) sequence was encountered, but the
    /// encountered byte is invalid UTF-8.
    InvalidUTF8Start(u8),
}

impl From<io::Error> for Kind {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<str::Utf8Error> for Kind {
    fn from(value: str::Utf8Error) -> Self {
        Self::UnicodeError(UnicodeError::InvalidUTF8(value))
    }
}

impl Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "i/o error: {err}"),
            Self::UnexpectedEOF => {
                write!(f, "unexpected end of input")
            }
            Self::InvalidByte { byte, reason } => {
                write!(f, "invalid byte: '{:x}' ({reason})", byte)
            }
            Self::InvalidEscapeSequence(s) => {
                write!(f, "invalid escape sequence: '{:x}'", s)
            }
            Self::InvalidHexCharacter(c) => {
                write!(f, "invalid hexadecimal character: '{c}'")
            }
            Self::UnicodeError(UnicodeError::InvalidUTF16(err)) => {
                write!(f, "UTF16 error: {err}")
            }
            Self::UnicodeError(UnicodeError::UnpairedHighSurrogate(s)) => {
                write!(f, "unpaired UTF16 high surrogate: {:x}", s)
            }
            Self::UnicodeError(UnicodeError::UnpairedLowSurrogate(s)) => {
                write!(f, "orphaned UTF16 low surrogate: {:x}", s)
            }
            Self::UnicodeError(UnicodeError::InvalidUTF8(err)) => {
                write!(f, "UTF8 error: '{:?}'", err)
            }
            Self::UnicodeError(UnicodeError::InvalidUTF8Start(byte)) => {
                write!(f, "invalid UTF8 start byte: '{:x}'", byte)
            }
        }
    }
}

impl std::error::Error for Error {}
