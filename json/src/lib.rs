use std::{
    collections::HashMap,
    fmt::{Display, Write},
    io::{BufReader, Bytes, Read},
    iter::Peekable,
    num::NonZero,
    str,
};

pub mod error;
pub mod serde;

// Make all kinds usable directly, less boilerplate.
use error::Kind::*;

const STRING_QUOTE: u8 = b'"';
const STRING_ESCAPE_OPEN: u8 = b'\\';

const OBJECT_OPEN: u8 = b'{';
const OBJECT_KV_SEP: u8 = b':';
const OBJECT_ENTRIES_SEP: u8 = b',';
const OBJECT_CLOSE: u8 = b'}';

const ARRAY_OPEN: u8 = b'[';
const ARRAY_SEP: u8 = b',';
const ARRAY_CLOSE: u8 = b']';

/// Maximum permissible depth in parsing values. Documents with values (objects, arrays)
/// nested deeper than this are rejected.
const MAX_DEPTH: usize = 256;

type ParseResult<T> = std::result::Result<T, error::Error>;

/// Parse the provided input into a JSON [`Value`].
///
/// Short-hand for [`Parser::new`] followed by [`Parser::parse`].
pub fn parse(data: &[u8]) -> ParseResult<Value> {
    let mut parser = Parser::new(data);

    parser.parse()
}

/// A [JSON value](https://datatracker.ietf.org/doc/html/rfc8259#section-3).
#[derive(Debug, PartialEq)]
pub enum Value {
    String(String),
    Number(Number),
    Object(HashMap<String, Value>),
    Array(Vec<Value>),
    Bool(bool),
    Null,
}

/// Prints the JSON, non-pretty string representation of [`Value`].
impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s) => {
                let mut buf = String::with_capacity(s.len());
                for c in s.chars() {
                    // Handle escapes.
                    match c {
                        '"' => buf.push_str(r#"\""#),
                        '\\' => buf.push_str(r#"\\"#),

                        // Could legally also fall through to `\uXXXX` below but these
                        // look nicer for common values.
                        '\n' => buf.push_str(r#"\n"#),
                        '\t' => buf.push_str(r#"\t"#),
                        '\r' => buf.push_str(r#"\r"#),
                        '\x08' => buf.push_str(r#"\b"#),
                        '\x0c' => buf.push_str(r#"\f"#),

                        c if c.is_control() => write!(buf, "\\u{:04x}", c as u32)?,
                        _ => buf.push(c),
                    }
                }
                write!(f, "{}{}{}", STRING_QUOTE as char, buf, STRING_QUOTE as char)
            }
            Value::Number(num) => {
                // Assumption: echoing number back verbatim is OK because it can only be
                // constructed from parsing incoming JSON, at which point it is
                // validated and rejected if invalid.
                write!(f, "{}", num.0)
            }
            Value::Object(obj) => {
                write!(f, "{}", OBJECT_OPEN as char)?;
                let mut first = true;
                for (k, v) in obj.iter() {
                    if !first {
                        write!(f, "{} ", OBJECT_ENTRIES_SEP as char)?;
                    }
                    write!(f, "\"{}\"{} ", k, OBJECT_KV_SEP as char)?;
                    write!(f, "{}", v)?;
                    first = false;
                }
                write!(f, "{}", OBJECT_CLOSE as char)?;

                Ok(())
            }
            Value::Array(values) => {
                write!(f, "{}", ARRAY_OPEN as char)?;
                let mut first = true;
                for v in values.iter() {
                    if !first {
                        write!(f, "{} ", ARRAY_SEP as char)?;
                    }
                    write!(f, "{}", v)?;
                    first = false;
                }
                write!(f, "{}", ARRAY_CLOSE as char)?;

                Ok(())
            }
            Value::Bool(b) => write!(f, "{b}"),
            Value::Null => write!(f, "null"),
        }
    }
}

/// A JSON number.
///
/// In the JSON spec, numbers have infinite precision. While often limited to [`f64`] in
/// real-world implementations, this type helps retain original, unlimited JSON
/// precision. Parsing simply normalizes the contained [`String`] to correspond to JSON
/// rules, such that all valid instances are valid JSON. Converting to native numeric
/// types is then up to consumers. Some common conversions are provided.
#[derive(Debug)]
pub struct Number(String);

/// Compare numbers in ascending order of precision, by actual numeric value.
impl PartialEq for Number {
    fn eq(&self, other: &Self) -> bool {
        let left: Result<u64, _> = self.try_into();
        let right: Result<u64, _> = other.try_into();
        if let (Ok(l), Ok(r)) = (left, right) {
            return l == r;
        }

        let left: Result<i64, _> = self.try_into();
        let right: Result<i64, _> = other.try_into();
        if let (Ok(l), Ok(r)) = (left, right) {
            return l == r;
        }

        // Precision loss can occur here.
        let left: Result<f64, _> = self.try_into();
        let right: Result<f64, _> = other.try_into();
        if let (Ok(l), Ok(r)) = (left, right) {
            return l == r;
        }

        // Fallback to direct string comparison.
        self.0 == other.0
    }
}

/// Convenience conversion implementations for [`Value`] and common stdlib types.
///
/// This muddles the waters a bit and mixes JSON and the higher-level concept of
/// "serialization" (independent of JSON) but it works...
pub mod conversions {
    pub mod to_value {
        use crate::{Number, Value};

        impl From<String> for Value {
            fn from(value: String) -> Self {
                Self::String(value)
            }
        }

        impl From<&str> for Value {
            fn from(value: &str) -> Self {
                value.to_string().into()
            }
        }

        impl From<&[u8]> for Value {
            fn from(value: &[u8]) -> Self {
                Self::String(base64::encode(value))
            }
        }

        impl From<u64> for Value {
            fn from(value: u64) -> Self {
                Value::Number(Number(value.to_string()))
            }
        }

        impl From<i64> for Value {
            fn from(value: i64) -> Self {
                Value::Number(Number(value.to_string()))
            }
        }

        impl From<f64> for Value {
            fn from(value: f64) -> Self {
                Value::Number(Number(value.to_string()))
            }
        }

        impl<T> From<&[T]> for Value
        where
            for<'a> Value: From<&'a T>,
        {
            fn from(value: &[T]) -> Self {
                Value::Array(value.iter().map(Into::into).collect())
            }
        }

        impl<T> From<&Option<T>> for Value
        where
            for<'a> Value: From<&'a T>,
        {
            fn from(value: &Option<T>) -> Self {
                match value {
                    Some(v) => v.into(),
                    None => Value::Null,
                }
            }
        }

        impl From<bool> for Value {
            fn from(value: bool) -> Self {
                Value::Bool(value)
            }
        }
    }

    pub mod from_value {
        use crate::{Number, Value};
        use std::num::{ParseFloatError, ParseIntError};

        /// An error to signal unsuccessful conversion from [`Value`] to some target
        /// type.
        #[derive(Debug)]
        pub struct TryFromError {
            /// The invalid value for this conversion.
            pub value: Value,
            /// Underlying error, if any.
            pub reason: Option<Box<dyn std::error::Error>>,
        }

        impl std::fmt::Display for TryFromError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "conversion error for value '{:?}' ", self.value)?;
                match &self.reason {
                    Some(r) => write!(f, "(reason: {})", r),
                    None => write!(f, "(no reason)"),
                }
            }
        }

        impl std::error::Error for TryFromError {}

        impl TryFrom<Value> for String {
            type Error = TryFromError;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                if let Value::String(s) = value {
                    Ok(s)
                } else {
                    Err(TryFromError {
                        value,
                        reason: None,
                    })
                }
            }
        }

        impl TryFrom<Value> for Vec<u8> {
            type Error = TryFromError;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                match value {
                    Value::String(s) => base64::decode(&s).map_err(|err| TryFromError {
                        value: Value::String(s),
                        reason: Some(err.into()),
                    }),
                    // Could also support array of numbers 0-255, like serde.
                    _ => Err(TryFromError {
                        value,
                        reason: None,
                    }),
                }
            }
        }

        impl TryFrom<Value> for u64 {
            type Error = TryFromError;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                if let Value::Number(ref num) = value {
                    num.try_into()
                        .map_err(|e: std::num::ParseIntError| TryFromError {
                            value,
                            reason: Some(e.into()),
                        })
                } else {
                    Err(TryFromError {
                        value,
                        reason: None,
                    })
                }
            }
        }

        impl TryFrom<Value> for f64 {
            type Error = TryFromError;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                if let Value::Number(ref num) = value {
                    num.try_into().map_err(|e: ParseFloatError| TryFromError {
                        value,
                        reason: Some(e.into()),
                    })
                } else {
                    Err(TryFromError {
                        value,
                        reason: None,
                    })
                }
            }
        }

        impl<T> TryFrom<Value> for Vec<T>
        where
            T: TryFrom<Value, Error = TryFromError>,
        {
            type Error = TryFromError;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                if let Value::Array(values) = value {
                    let mut items = Vec::with_capacity(values.len());
                    for v in values {
                        let item: T = v.try_into()?;
                        items.push(item);
                    }

                    Ok(items)
                } else {
                    Err(TryFromError {
                        value,
                        reason: None,
                    })
                }
            }
        }

        impl<T> TryFrom<Value> for Option<T>
        where
            T: TryFrom<Value, Error = TryFromError>,
        {
            type Error = TryFromError;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                match value {
                    Value::Null => Ok(None),
                    v => Ok(Some(v.try_into()?)),
                }
            }
        }

        impl TryFrom<Number> for u64 {
            type Error = ParseIntError;

            fn try_from(value: Number) -> Result<Self, Self::Error> {
                value.0.parse()
            }
        }

        impl TryFrom<&Number> for u64 {
            type Error = ParseIntError;

            fn try_from(value: &Number) -> Result<Self, Self::Error> {
                value.0.parse()
            }
        }

        impl TryFrom<Number> for i64 {
            type Error = ParseIntError;

            fn try_from(value: Number) -> Result<Self, Self::Error> {
                value.0.parse()
            }
        }

        impl TryFrom<&Number> for i64 {
            type Error = ParseIntError;

            fn try_from(value: &Number) -> Result<Self, Self::Error> {
                value.0.parse()
            }
        }

        impl TryFrom<Number> for f64 {
            type Error = ParseFloatError;

            fn try_from(value: Number) -> Result<Self, Self::Error> {
                value.0.parse()
            }
        }

        impl TryFrom<&Number> for f64 {
            type Error = ParseFloatError;

            fn try_from(value: &Number) -> Result<Self, Self::Error> {
                value.0.parse()
            }
        }
    }
}

/// A fully-featured, "streaming" JSON parser.
///
/// Spec: <https://www.json.org/json-en.html>
#[derive(Debug)]
pub struct Parser<R: Read> {
    stream: Peekable<Bytes<BufReader<R>>>,
    /// Bytes read from stream so far.
    stream_pos: usize,

    /// When processing UTF16 JSON escapes like `\u1234` in JSON strings, surrogate
    /// pairs might be encountered. This slot tracks encountered high surrogates,
    /// pending pairing with a subsequent low surrogate (at which point the slot is
    /// cleared again).
    pending_high_surrogate: Option<NonZero<u16>>,
    /// Current stack depth in parsing values (in nested arrays, objects). Prevents
    /// stack overflows in the parser (best-effort, as we can't know host stack sizes).
    depth: usize,
}

impl<R: Read> Parser<R> {
    /// Constructs a new parser over the reader implementation. The parser will be
    /// buffered.
    pub fn new(reader: R) -> Self {
        let stream = BufReader::new(reader).bytes().peekable();

        Self {
            stream,
            stream_pos: 0,
            pending_high_surrogate: None,
            depth: 0,
        }
    }

    /// Entrypoint into parsing. Will try to parse the input as a [JSON
    /// *value*](https://www.json.org/json-en.html).
    pub fn parse(&mut self) -> ParseResult<Value> {
        let value = self.visit_value()?;

        // Multi-value documents are not supported: additional data in the stream is a
        // failure.
        //
        // Note, this technically reads one byte too much from the input, for a nicer
        // report ("which byte was actually problematic"); `peek` has the same issue. As
        // we return the byte to the caller in the error, this should be OK (caller can
        // reconstruct everything).
        match self.stream.next() {
            Some(Ok(more)) => Err(self.err(InvalidByte {
                byte: more,
                reason: "excessive data in input after parsing one JSON value".into(),
            })),
            Some(Err(e)) => Err(self.err(Io(e))),
            None => Ok(value),
        }
    }

    /// Get the next byte from the underlying reader. Calling this method signals a
    /// *need* for a next byte, thus if none is found (reader finished or errored), it
    /// is considered an error.
    fn advance(&mut self) -> ParseResult<u8> {
        let byte = self
            .stream
            .next()
            .ok_or(self.err(UnexpectedEOF))?
            .map_err(|io_err| self.err(Io(io_err)))?;
        self.stream_pos += 1;
        Ok(byte)
    }

    /// Construct an error of the provided kind, with information from current parser
    /// state.
    fn err(&self, kind: error::Kind) -> error::Error {
        error::Error {
            kind,
            pos: self.stream_pos,
        }
    }

    fn visit_value(&mut self) -> ParseResult<Value> {
        self.depth += 1;
        if self.depth >= MAX_DEPTH {
            return Err(self.err(NestingTooDeep(self.depth)));
        }

        self.skip_whitespace()?;

        // Peek ahead, but leave actual token consumption to implementations themselves.
        // This keeps it symmetrical. The individual functions are essentially the
        // diagrams at <https://www.json.org/json-en.html>, where e.g. `object` is
        // responsible for its own `{` and `}` handling.
        let val = match self.stream.peek() {
            Some(Ok(STRING_QUOTE)) => self.visit_string().map(Value::String)?,
            Some(Ok(OBJECT_OPEN)) => self.visit_object().map(Value::Object)?,
            Some(Ok(ARRAY_OPEN)) => self.visit_array().map(Value::Array)?,
            Some(Ok(b't')) | Some(Ok(b'f')) => self.visit_bool().map(Value::Bool)?,
            Some(Ok(b'n')) => self.visit_null().map(|_| Value::Null)?,
            Some(Ok(b'-')) | Some(Ok(b'0'..=b'9')) => self.visit_number().map(Value::Number)?,
            Some(&Ok(byte /* Copy out */)) => {
                return Err(self.err(InvalidByte {
                    byte,
                    reason: "invalid byte looking for beginning of value".into(),
                }));
            }
            _ => return Err(self.err(UnexpectedEOF)),
        };
        self.skip_whitespace()?;
        self.depth -= 1;

        Ok(val)
    }

    /// Skips all upcoming whitespace, if present.
    fn skip_whitespace(&mut self) -> ParseResult<()> {
        // If there's no whitespace, do not consume.
        while let Some(Ok(c)) = self.stream.peek() {
            if is_json_whitespace(*c) {
                // Actually consume it. I don't think this can genuinely error after
                // having just successfully peeked, but it'd be a shame to unnecessarily
                // panic; so just have this return a Result.
                self.advance()?;
            } else {
                break;
            }
        }

        Ok(())
    }

    fn visit_object(&mut self) -> ParseResult<HashMap<String, Value>> {
        assert_eq!(
            self.advance()?,
            OBJECT_OPEN,
            "should only be reachable from visiting value, where it should be peeked correctly before visiting"
        );

        self.skip_whitespace()?;

        let mut map = HashMap::new();

        // Early return for empty object
        if let Some(Ok(OBJECT_CLOSE)) = self.stream.peek() {
            let _ = self.advance()?;
            return Ok(map);
        }

        loop {
            let key = self.visit_string()?;
            self.skip_whitespace()?;
            match self.advance()? {
                OBJECT_KV_SEP => { /* OK */ }
                byte => {
                    break Err(self.err(InvalidByte {
                        byte,
                        reason: format!(
                            "parsing object, expected {} after key",
                            OBJECT_KV_SEP as char
                        ),
                    }));
                }
            }
            let value = self.visit_value()?;

            map.insert(key, value);

            match self.advance()? {
                OBJECT_CLOSE => break Ok(map),
                OBJECT_ENTRIES_SEP => {
                    self.skip_whitespace()?;
                    continue;
                }
                byte => {
                    break Err(self.err(InvalidByte {
                        byte,
                        reason: format!(
                            "parsing object, expected {} or {} after parsing a key/value pair",
                            OBJECT_CLOSE as char, OBJECT_ENTRIES_SEP as char,
                        ),
                    }));
                }
            }
        }
    }

    fn visit_string(&mut self) -> ParseResult<String> {
        {
            let byte = self.advance()?;
            if byte != STRING_QUOTE {
                return Err(self.err(InvalidByte {
                    byte,
                    reason: format!("expected {} for start of string", STRING_QUOTE as char),
                }));
            };
        }

        // Initial capacity showed ~10% higher throughput in a local benchmark. It
        // avoids reallocs for most strings.
        let mut s = String::with_capacity(32);

        loop {
            if let Some(high_surrogate) = self.pending_high_surrogate {
                // A pending high surrogate is invalid without a subsequent low
                // surrogate.
                let (STRING_ESCAPE_OPEN, b'u') = (self.advance()?, self.advance()?) else {
                    return Err(self.err(UnicodeError(
                        error::UnicodeError::UnpairedHighSurrogate(high_surrogate.get()),
                    )));
                };

                let low_surrogate_candidate = self.collect_unicode_escape_hex_digits()?;
                let mut chars = char::decode_utf16([high_surrogate.get(), low_surrogate_candidate]);
                let c = chars
                    .next()
                    .expect("should have one (potentially error) element after providing surrogate pair candidate")
                    .map_err(|e| self.err(UnicodeError(error::UnicodeError::InvalidUTF16(e))))?;
                s.push(c);
                assert!(
                    chars.next().is_none(),
                    "should not yield more than 1 character from 1 surrogate pair"
                );

                self.pending_high_surrogate = None;
            }

            match self.advance()? {
                STRING_QUOTE => break Ok(s),
                STRING_ESCAPE_OPEN => match self.advance()? {
                    b'"' => s.push('"'),
                    b'\\' => s.push('\\'),
                    b'/' => s.push('/'),
                    b'b' => s.push('\x08'),
                    b'f' => s.push('\x0C'),
                    b'n' => s.push('\n'),
                    b'r' => s.push('\r'),
                    b't' => s.push('\t'),
                    b'u' => {
                        let codepoint = self.collect_unicode_escape_hex_digits()?;
                        assert!(
                            self.pending_high_surrogate.is_none(),
                            "should not reach consecutive (high) surrogates"
                        );

                        match codepoint {
                            // https://www.unicode.org/glossary/#high_surrogate_code_unit
                            0xD800..=0xDBFF => {
                                // Register it but do not push out yet. Need a low
                                // surrogate partner, on next loop iteration.
                                self.pending_high_surrogate =
                                    Some(NonZero::new(codepoint).expect("should have value > 0 in this branch"));
                            }
                            // https://www.unicode.org/glossary/#low_surrogate_code_unit
                            0xDC00..=0xDFFF => {
                                return Err(self.err(UnicodeError(error::UnicodeError::UnpairedLowSurrogate(codepoint))))
                            }
                            val => s.push(char::from_u32(val as u32).expect("non-surrogate, u16 UTF-16 code point should always be a valid char")),
                        }
                    }
                    b => break Err(self.err(InvalidEscapeSequence(b))),
                },
                ctrl_byte @ 0x00..=0x1F => {
                    // Note: DELETE aka 0x7F is included in `u8.is_ascii_control()`, but
                    // permitted literally in JSON strings, cf.
                    // https://datatracker.ietf.org/doc/html/rfc8259#section-7.
                    return Err(self.err(InvalidByte {
                        byte: ctrl_byte,
                        reason: "unescaped control character".into(),
                    }));
                }
                byte @ 0x20..=0x7F => s.push(byte as char), // ASCII: encoding == codepoint
                utf8_byte @ 0x80.. => {
                    assert!(0b1000_0000 & utf8_byte != 0, "continuation bit is set");
                    let mut bytes = [0u8; 4];
                    bytes[0] = utf8_byte;

                    // Assume UTF8, and consume one code point. For pattern see
                    // https://en.wikipedia.org/wiki/UTF-8#Description. A bit repetitive
                    // below, but this way we only touch subsequent bytes if the start
                    // byte actually looks OK. The `str` will be stack-allocated, which
                    // is a neat bonus.
                    let i = if 0b0010_0000u8 & utf8_byte == 0 {
                        bytes[1] = self.advance()?;
                        2
                    } else if 0b0001_0000u8 & utf8_byte == 0 {
                        bytes[1] = self.advance()?;
                        bytes[2] = self.advance()?;
                        3
                    } else if 0b0000_1000u8 & utf8_byte == 0 {
                        bytes[1] = self.advance()?;
                        bytes[2] = self.advance()?;
                        bytes[3] = self.advance()?;
                        4
                    } else {
                        return Err(self.err(UnicodeError(error::UnicodeError::InvalidUTF8Start(
                            utf8_byte,
                        ))));
                    };

                    s.push_str(
                        str::from_utf8(bytes.get(..i).expect("should set indices correctly"))
                            .map_err(|utf8err| self.err(utf8err.into()))?,
                    );
                }
            }
        }
    }

    fn visit_array(&mut self) -> ParseResult<Vec<Value>> {
        assert_eq!(
            self.advance()?,
            ARRAY_OPEN,
            "should only be reachable from visiting value, where it should be peeked correctly before visiting"
        );

        self.skip_whitespace()?;

        let mut array = Vec::new();

        // Early return for empty array
        if let Some(Ok(ARRAY_CLOSE)) = self.stream.peek() {
            let _ = self.advance()?;
            return Ok(array);
        }

        loop {
            array.push(self.visit_value()?);
            match self.advance()? {
                ARRAY_CLOSE => break Ok(array),
                ARRAY_SEP => continue,
                byte => {
                    break Err(self.err(InvalidByte {
                        byte,
                        reason: format!(
                            "parsing array, need {} or {} after parsing an array element",
                            ARRAY_CLOSE as char, ARRAY_SEP as char,
                        ),
                    }));
                }
            }
        }
    }

    fn visit_bool(&mut self) -> ParseResult<bool> {
        let (expected_remainder, result) = match self.advance()? {
            b't' => (b"rue".as_slice(), true),
            b'f' => (b"alse".as_slice(), false),
            _ => unreachable!(
                "should only be reachable from visiting value, where it should be peeked correctly before visiting"
            ),
        };

        for expected in expected_remainder.iter().copied() {
            let got = self.advance()?;
            if expected != got {
                return Err(self.err(InvalidByte {
                    byte: got,
                    reason: format!(
                        "expected {} scanning boolean value, got {}",
                        expected as char, got as char
                    ),
                }));
            }
        }

        Ok(result)
    }

    fn visit_null(&mut self) -> ParseResult<()> {
        for expected in b"null".iter().copied() {
            let got = self.advance()?;
            if expected != got {
                return Err(self.err(InvalidByte {
                    byte: got,
                    reason: format!(
                        "expected {} scanning for null, got {}",
                        expected as char, got as char
                    ),
                }));
            }
        }

        Ok(())
    }

    fn visit_number(&mut self) -> ParseResult<Number> {
        let mut number = Number(String::with_capacity(1));
        number = self.visit_number_integral_part(number)?;
        number = self.visit_number_fractional_part(number)?;
        number = self.visit_number_exponent_part(number)?;

        match self.stream.peek() {
            // If there's a digit left we might have gotten a leading zero followed by
            // more digits, e.g. `01`. Without fractional or exponent parts, our parsing
            // just exits after `0`, so check.
            Some(&Ok(byte @ b'0'..=b'9')) => Err(self.err(InvalidByte {
                byte,
                reason: format!(
                    "unexpected digit remaining after processing number ('{number:?}')"
                ),
            })),
            _ => Ok(number),
        }
    }

    /// Parses the integral part ("bit before period") of a potentially fractional
    /// number. Parsing stops if no valid tokens can be consumed anymore.
    ///
    /// While objects, strings, arrays have delimiters which allow greedy fetching,
    /// numbers do not. We need to be careful not to overfetch, thus work with peeking
    /// and more manual advancing. In general, for every `push` into the number, there
    /// needs to be an advance of the reader.
    fn visit_number_integral_part(&mut self, mut number: Number) -> ParseResult<Number> {
        // First token is mandatory, there's no way we can overfetch here: so consume
        // right away.
        match self.advance()? {
            b'-' => {
                number.0.push('-');

                // Finding a digit is mandatory now: can't negate a non-number.
                match self.advance()? {
                    b'0' => {
                        number.0.push('0');

                        // Negative zero: terminal case.
                        Ok(number)
                    }
                    byte @ b'1'..=b'9' => {
                        number.0.push(byte as char);

                        loop {
                            // Further digits are optional, so do not overfetch.
                            match self.stream.peek() {
                                Some(Ok(byte @ b'0'..=b'9')) => {
                                    number.0.push(*byte as char);
                                }
                                _ => return Ok(number),
                            }
                            self.advance()?;
                        }
                    }
                    byte => Err(self.err(InvalidByte {
                        byte,
                        reason: "expected digit after - sign scanning number".into(),
                    })),
                }
            }
            b'0' => {
                number.0.push('0');
                Ok(number)
            }
            byte @ b'1'..=b'9' => {
                number.0.push(byte as char);

                // NB: duplicates logic from above.
                loop {
                    match self.stream.peek() {
                        Some(Ok(byte @ b'0'..=b'9')) => {
                            number.0.push(*byte as char);
                        }
                        _ => return Ok(number),
                    }
                    self.advance()?;
                }
            }
            _ => unreachable!(
                "should only be reachable from visiting number, which is only reachable from visiting value, where it should be peeked correctly before visiting"
            ),
        }
    }

    fn visit_number_fractional_part(&mut self, mut number: Number) -> ParseResult<Number> {
        match self.stream.peek() {
            Some(Ok(b'.')) => {
                number.0.push('.');
                self.advance()?;
            }
            _ => return Ok(number),
        }

        // At least one digit is mandatory now.
        match self.advance()? {
            byte @ b'0'..=b'9' => number.0.push(byte as char),
            byte => {
                return Err(self.err(InvalidByte {
                    byte,
                    reason: "expected at least one digit for fractional part".into(),
                }));
            }
        }

        // Any further digits are optional.
        loop {
            match self.stream.peek() {
                Some(Ok(byte @ b'0'..=b'9')) => {
                    number.0.push(*byte as char);
                }
                _ => return Ok(number),
            }
            self.advance()?;
        }
    }

    fn visit_number_exponent_part(&mut self, mut number: Number) -> ParseResult<Number> {
        match self.stream.peek() {
            Some(Ok(b'e' | b'E')) => {
                number.0.push('e'); // Case doesn't matter
                self.advance()?;
            }
            _ => return Ok(number),
        }

        // Next token is mandatory, no peeking necessary.
        match self.advance()? {
            byte @ (b'+' | b'-') => {
                number.0.push(byte as char);

                // Also mandatory.
                match self.advance()? {
                    byte @ b'0'..=b'9' => number.0.push(byte as char),
                    byte => {
                        return Err(self.err(InvalidByte {
                            byte,
                            reason: "expected digit after sign scanning number exponent".into(),
                        }));
                    }
                }
            }
            byte @ b'0'..=b'9' => number.0.push(byte as char),
            byte => {
                return Err(self.err(InvalidByte {
                    byte,
                    reason: "expected digit or sign scanning number exponent".into(),
                }));
            }
        }

        // Any further digits are optional.
        loop {
            match self.stream.peek() {
                Some(Ok(byte @ b'0'..=b'9')) => {
                    number.0.push(*byte as char);
                }
                _ => return Ok(number),
            }
            self.advance()?;
        }
    }

    /// JSON Unicode hex escapes are **required** to be 4 long.
    fn collect_unicode_escape_hex_digits(&mut self) -> ParseResult<u16> {
        self.collect_hex::<u16, 4>()
    }

    /// Collect hex digits.
    ///
    /// `T` is the target type to collect into. `N` is the number of hex chars/digits to
    /// collect. Note they're indepedent: few digits can be collected into a large `T`
    /// etc.
    fn collect_hex<T, const N: usize>(&mut self) -> ParseResult<T>
    where
        // Generic over u8, u16, ...
        T: Default
            + Copy
            + From<u8>
            + std::ops::Shl<usize, Output = T>
            + std::ops::BitOr<Output = T>,
    {
        let mut val = T::default();

        for _ in 0..N {
            let c = self.advance()? as char;

            let digit = c
                .to_digit(16)
                .ok_or_else(|| self.err(InvalidHexCharacter(c)))?;

            #[allow(
                clippy::cast_possible_truncation,
                reason = "to_digit(16) guarantees values 0-15, which fit u8"
            )]
            {
                val = (val << 4) | T::from(digit as u8); // works thanks to `From<u8>`
            }
        }

        Ok(val)
    }
}

/// Note JSON whitespace differs from Unicode whitespace, it's a narrower definition.
#[inline]
fn is_json_whitespace(c: u8) -> bool {
    c == b' ' || c == b'\n' || c == b'\r' || c == b'\t'
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to run the parser and extract the String variant.
    ///
    /// Panics if the result is not a [`Value::String`] or if parsing fails.
    fn parse_string(input: &str) -> String {
        let mut parser = Parser::new(input.as_bytes());
        let val = parser.parse().expect("Failed to parse");
        match val {
            Value::String(s) => s,
            _ => panic!("Expected Value::String, got {:?}", val),
        }
    }

    /// Helper to expect a specific error type.
    fn parse_err(input: &[u8]) -> error::Error {
        let mut parser = Parser::new(input);
        parser.parse().expect_err("Expected error but got success")
    }

    // ==========================================
    // Basic value tests
    // ==========================================

    #[test]
    fn test_parse_value_with_empty_input() {
        let err = parse_err(b"");
        match err {
            error::Error {
                kind: UnexpectedEOF,
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    // ==========================================
    // Basic String Tests
    // ==========================================

    #[test]
    fn test_empty_string() {
        assert_eq!(parse_string(r#""""#), "");
    }

    #[test]
    fn test_simple_ascii() {
        assert_eq!(parse_string(r#""hello world""#), "hello world");
    }

    #[test]
    fn test_alphanumeric() {
        assert_eq!(parse_string(r#""abc123XYZ""#), "abc123XYZ");
    }

    #[test]
    fn test_whitespace_around_string() {
        assert_eq!(parse_string("   \"trimmed\""), "trimmed");
        assert_eq!(parse_string("\n\t \"trimmed\""), "trimmed");
    }

    // ==========================================
    // Escape Sequence Tests
    // ==========================================

    #[test]
    fn test_quote_escape() {
        assert_eq!(parse_string(r#""foo\"bar""#), r#"foo"bar"#);
    }

    #[test]
    fn test_backslash_escape() {
        assert_eq!(parse_string(r#""C:\\Path""#), r#"C:\Path"#);
    }

    #[test]
    fn test_forward_slash_escape() {
        assert_eq!(parse_string(r#""\/""#), "/");
    }

    #[test]
    fn test_control_char_escapes() {
        assert_eq!(parse_string(r#""\b""#), "\x08"); // Backspace
        assert_eq!(parse_string(r#""\f""#), "\x0C"); // Form feed
        assert_eq!(parse_string(r#""\n""#), "\n"); // Newline
        assert_eq!(parse_string(r#""\r""#), "\r"); // Carriage return
        assert_eq!(parse_string(r#""\t""#), "\t"); // Tab
    }

    #[test]
    fn test_mixed_escapes() {
        assert_eq!(
            parse_string(r#""Line1\nLine2\tTabbed""#),
            "Line1\nLine2\tTabbed"
        );
    }

    // ==========================================
    // Unicode & Hex Tests
    // ==========================================

    #[test]
    fn test_basic_unicode_escape() {
        // \u0041 is 'A'
        assert_eq!(parse_string(r#""\u0041""#), "A");
        // \u00A9 is Copyright symbol
        assert_eq!(parse_string(r#""\u00A9""#), "©");
    }

    #[test]
    fn test_raw_utf8_input() {
        // 2 code points
        assert!("é".len() == 2);
        assert_eq!(parse_string(r#""José""#), "José");

        // 3 code points
        assert!("⌣".len() == 3);
        assert_eq!(parse_string(r#""⌣""#), "⌣");

        // 4 code points
        assert!("🔥".len() == 4);
        assert_eq!(parse_string(r#""🔥""#), "🔥");
    }

    #[test]
    fn test_utf8_invalid_start_byte_in_string() {
        let input = b"\"\xFF\"";

        let err = parse_err(input);
        match err {
            error::Error {
                kind: UnicodeError(error::UnicodeError::InvalidUTF8Start(0xFF)),
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_utf8_invalid_continuation_byte_in_string() {
        // 0xC3 starts a 2-byte sequence (expects 1 continuation), but followed by space.
        let input = b"\"\xC3 \"";

        let err = parse_err(input);
        match err {
            error::Error {
                kind: UnicodeError(error::UnicodeError::InvalidUTF8(_)),
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_utf8_invalid_start_byte_inside_string() {
        // 0xE2 starts a 3-byte sequence, 0x82 is a valid continuation, but 0xC0 is a
        // START byte (for 2-byte seq), not a continuation
        let input = b"\"\xE2\x82\xC0 \"";

        let err = parse_err(input);
        match err {
            error::Error {
                kind: UnicodeError(error::UnicodeError::InvalidUTF8(_)),
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_utf8_deny_overlong_encoding() {
        // More bytes than necessary should be rejected
        let input = b"\"\xC0\xAF \"";

        let err = parse_err(input);
        match err {
            error::Error {
                kind: UnicodeError(error::UnicodeError::InvalidUTF8(_)),
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_utf8_premature_end_in_string() {
        // Starts 4-byte sequence but ends right away
        let input = b"\"\xF0\"";

        let err = parse_err(input);
        match err {
            error::Error {
                kind: UnexpectedEOF,
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_surrogate_pairs() {
        // G-Clef (U+1D11E) represented as surrogate pair \uD834\uDD1E
        assert_eq!(parse_string(r#""\ud834\udd1e""#), "𝄞");
        assert_eq!(parse_string(r#""\uD834\uDD1E""#), "𝄞");

        // Emoji (Grinning Face 😀 U+1F600) -> \uD83D\uDE00
        assert_eq!(parse_string(r#""\ud83d\ude00""#), "😀");
    }

    #[test]
    fn test_mixed_unicode_escapes_and_chars() {
        // "A" + Heart (escape) + "B"
        assert_eq!(parse_string(r#""A\u2764B""#), "A❤B");
    }

    #[test]
    fn test_consecutive_unicode_escapes() {
        assert_eq!(parse_string(r#""\u0041\u0042\u0043""#), "ABC");
    }

    #[test]
    fn test_mixed_string() {
        // Tests everything there can be in a string.
        assert_eq!(
            parse_string(r#" " \u0041 \u2764 🔥 \ud834\udd1e \uD834\uDD1E " "#),
            " A ❤ 🔥 𝄞 𝄞 "
        );
    }

    // ==========================================
    // Error Handling Tests
    // ==========================================

    #[test]
    fn test_err_unexpected_eof_no_end_quote() {
        let err = parse_err(b"\"unclosed");
        match err {
            error::Error {
                kind: UnexpectedEOF,
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_invalid_escape_char() {
        // \a is not a valid JSON escape
        let err = parse_err(br#""\a""#);
        match err {
            error::Error {
                kind: InvalidEscapeSequence(b),
                ..
            } => assert_eq!(b, b'a'),
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_invalid_hex_digit() {
        // 'z' is not hex
        let err = parse_err(br#""\u123z""#);
        match err {
            error::Error {
                kind: InvalidHexCharacter(c),
                ..
            } => assert_eq!(c, 'z'),
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_invalid_hex_length() {
        // Ends prematurely
        let err = parse_err(br#""\u12""#);
        match err {
            error::Error {
                kind: InvalidHexCharacter(c),
                ..
            } => assert_eq!(c, '"'),
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_unpaired_surrogate() {
        // High surrogate \uD800 without a following Low surrogate
        let err = parse_err(br#""\ud800""#);
        match err {
            error::Error {
                kind: UnexpectedEOF,
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_high_surrogate_followed_by_literal() {
        let err = parse_err(br#""\ud800a""#);
        match err {
            error::Error {
                kind: UnicodeError(error::UnicodeError::UnpairedHighSurrogate(0xD800)),
                ..
            } => {}
            _ => panic!("Expected UnpairedUnicodeSurrogate, got {:?}", err),
        }
    }

    #[test]
    fn test_err_high_surrogate_followed_by_wrong_escape() {
        let err = parse_err(br#""\ud800\n""#);
        match err {
            error::Error {
                kind: UnicodeError(error::UnicodeError::UnpairedHighSurrogate(0xD800)),
                ..
            } => {}
            _ => panic!("Expected UnpairedUnicodeSurrogate, got {:?}", err),
        }
    }

    #[test]
    fn test_err_double_high_surrogate() {
        // Need pairs of (high, low), (high, high) is invalid
        let err = parse_err(br#""\ud800\ud800""#);
        match err {
            error::Error {
                kind: UnicodeError(error::UnicodeError::InvalidUTF16(_)),
                ..
            } => {}
            _ => panic!("Expected InvalidUTF16 error, got {:?}", err),
        }
    }

    #[test]
    fn test_err_high_surrogate_followed_by_invalid_utf16() {
        // Escape is valid code point, but it's not in the low surrogate rang
        let err = parse_err(br#""\ud800\u1234""#);
        match err {
            error::Error {
                kind: UnicodeError(error::UnicodeError::InvalidUTF16(_)),
                ..
            } => {}
            _ => panic!("Expected InvalidUTF16 error, got {:?}", err),
        }
    }

    #[test]
    fn test_err_orphaned_low_surrogate() {
        let err = parse_err(br#""\udd1e""#);
        match err {
            error::Error {
                kind: UnicodeError(error::UnicodeError::UnpairedLowSurrogate(0xDD1E)),
                ..
            } => {}
            _ => panic!("Expected UnicodeError, got {:?}", err),
        }
    }

    // ==========================================
    // Object Helpers
    // ==========================================

    /// Helper to parse input directly into a map.
    fn parse_object(input: &str) -> HashMap<String, Value> {
        let mut parser = Parser::new(input.as_bytes());
        match parser.parse().expect("Failed to parse object") {
            Value::Object(map) => map,
            val => panic!("Expected Value::Object, got {:?}", val),
        }
    }

    /// Helper to check if a value in the map exists and is a specific string.
    fn assert_is_string(map: &HashMap<String, Value>, key: &str, expected: &str) {
        match map.get(key) {
            Some(Value::String(s)) => assert_eq!(s, expected),
            Some(v) => panic!("Key '{}' exists but is not a string. Got: {:?}", key, v),
            None => panic!("Key '{}' not found in map", key),
        }
    }

    // ==========================================
    // Object Tests
    // ==========================================

    #[test]
    fn test_empty_object() {
        let map = parse_object("{}");
        assert!(map.is_empty());
    }

    #[test]
    fn test_empty_object_with_whitespace() {
        let map = parse_object("  {   }  ");
        assert!(map.is_empty());
    }

    #[test]
    fn test_simple_object() {
        let map = parse_object(r#"{"foo": "bar"}"#);
        assert_eq!(map.len(), 1);
        assert_is_string(&map, "foo", "bar");
    }

    #[test]
    fn test_object_multiple_keys() {
        let map = parse_object(r#"{"name": "Alice", "city": "Wonderland"}"#);
        assert_eq!(map.len(), 2);
        assert_is_string(&map, "name", "Alice");
        assert_is_string(&map, "city", "Wonderland");
    }

    #[test]
    fn test_object_whitespace_variations() {
        // Lots of spaces around colons and commas
        let map = parse_object(r#"{ "a" : "b" , "c" : "d" }"#);
        assert_eq!(map.len(), 2);
        assert_is_string(&map, "a", "b");
        assert_is_string(&map, "c", "d");
    }

    #[test]
    fn test_unicode_keys() {
        let map = parse_object(r#"{"🔥": "fire", "key": "🔑"}"#);
        assert_eq!(map.len(), 2);
        assert_is_string(&map, "🔥", "fire");
        assert_is_string(&map, "key", "🔑");
    }

    #[test]
    fn test_nested_object() {
        let map = parse_object(r#"{"outer": {"inner": "value"}}"#);

        // Unwrap the nested object
        let inner_val = map.get("outer").expect("outer key missing");
        match inner_val {
            Value::Object(inner_map) => {
                assert_is_string(inner_map, "inner", "value");
            }
            _ => panic!("Expected nested object, got {:?}", inner_val),
        }
    }

    #[test]
    fn test_deeply_nested_object() {
        let input = r#"
        {
            "level1": {
                "level2": {
                    "level3": "found_me"
                }
            }
        }"#;
        let map = parse_object(input);

        if let Value::Object(l2) = &map["level1"]
            && let Value::Object(l3) = &l2["level2"]
        {
            assert_is_string(l3, "level3", "found_me");
            return;
        }
        panic!("Deep structure not parsed correctly");
    }

    // ==========================================
    // Object Error Handling Tests
    // ==========================================

    #[test]
    fn test_err_object_trailing_comma() {
        // Trailing commas are not allowed in standard JSON
        let err = parse_err(br#"{"a": "b",}"#);
        match err {
            error::Error {
                kind: InvalidByte { byte: b'}', .. },
                ..
            } => {}
            _ => panic!("Expected InvalidByte or EOF, got {:?}", err),
        }
    }

    #[test]
    fn test_err_object_missing_colon() {
        let err = parse_err(br#"{"key" "value"}"#);
        match err {
            error::Error {
                kind: InvalidByte { byte: b'"', .. },
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_object_missing_comma() {
        let err = parse_err(br#"{"a": "b" "c": "d"}"#);
        match err {
            error::Error {
                kind: InvalidByte { byte: b'"', .. },
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_object_key_must_be_string() {
        // Keys must be strings.
        let err = parse_err(br#"{ 1: "b" }"#);
        match err {
            error::Error {
                kind: InvalidByte { byte: b'1', .. },
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    // ==========================================
    // Array Helpers
    // ==========================================

    /// Helper to parse input directly into a Vec.
    fn parse_array(input: &str) -> Vec<Value> {
        let mut parser = Parser::new(input.as_bytes());
        match parser.parse().expect("Failed to parse array") {
            Value::Array(vec) => vec,
            val => panic!("Expected Value::Array, got {:?}", val),
        }
    }

    /// Helper to assert an item in a slice is a specific string
    fn assert_array_string(arr: &[Value], index: usize, expected: &str) {
        match &arr[index] {
            Value::String(s) => assert_eq!(s, expected),
            val => panic!("Expected String at index {}, got {:?}", index, val),
        }
    }

    // ==========================================
    // Array Tests
    // ==========================================

    #[test]
    fn test_empty_array() {
        let arr = parse_array("[]");
        assert!(arr.is_empty());
    }

    #[test]
    fn test_empty_array_with_whitespace() {
        let arr = parse_array("  [ \n\t ]  ");
        assert!(arr.is_empty());
    }

    #[test]
    fn test_simple_string_array() {
        let arr = parse_array(r#"[ "a", "b", "c" ]"#);
        assert_eq!(arr.len(), 3);
        assert_array_string(&arr, 0, "a");
        assert_array_string(&arr, 1, "b");
        assert_array_string(&arr, 2, "c");
    }

    #[test]
    fn test_array_whitespace_variations() {
        // Spaces around elements, commas, brackets
        let arr = parse_array(r#"[ "one" , "two" ,"three" ]"#);
        assert_eq!(arr.len(), 3);
        assert_array_string(&arr, 0, "one");
        assert_array_string(&arr, 1, "two");
        assert_array_string(&arr, 2, "three");
    }

    #[test]
    fn test_nested_arrays() {
        let arr = parse_array(r#"[ ["x"], [] ]"#);
        assert_eq!(arr.len(), 2);

        // Check first element is ["x"]
        match &arr[0] {
            Value::Array(inner) => {
                assert_eq!(inner.len(), 1);
                assert_array_string(inner, 0, "x");
            }
            _ => panic!("Expected inner array at index 0"),
        }

        // Check second element is []
        match &arr[1] {
            Value::Array(inner) => assert!(inner.is_empty()),
            _ => panic!("Expected inner array at index 1"),
        }
    }

    #[test]
    fn test_array_of_objects() {
        let arr = parse_array(r#"[ {"name": "A"}, {"name": "B"} ]"#);
        assert_eq!(arr.len(), 2);

        match &arr[0] {
            Value::Object(map) => assert_is_string(map, "name", "A"),
            _ => panic!("Expected object at index 0"),
        }

        match &arr[1] {
            Value::Object(map) => assert_is_string(map, "name", "B"),
            _ => panic!("Expected object at index 1"),
        }
    }

    #[test]
    fn test_mixed_array() {
        let arr = parse_array(r#"[ "start", {"key": "val"}, ["end"] ]"#);
        assert_eq!(arr.len(), 3);

        assert_array_string(&arr, 0, "start");

        match &arr[1] {
            Value::Object(map) => assert_is_string(map, "key", "val"),
            _ => panic!("Index 1 incorrect"),
        }

        match &arr[2] {
            Value::Array(inner) => assert_array_string(inner, 0, "end"),
            _ => panic!("Index 2 incorrect"),
        }
    }

    #[test]
    fn test_object_containing_array() {
        let map = parse_object(r#"{ "tags": ["rust", "json"] }"#);

        match map.get("tags") {
            Some(Value::Array(arr)) => {
                assert_eq!(arr.len(), 2);
                assert_array_string(arr, 0, "rust");
                assert_array_string(arr, 1, "json");
            }
            val => panic!("Expected array for key 'tags', got {:?}", val),
        }
    }

    // ==========================================
    // Array Error Handling Tests
    // ==========================================

    #[test]
    fn test_err_array_missing_comma() {
        let err = parse_err(br#"[ "a" "b" ]"#);
        match err {
            error::Error {
                kind: InvalidByte { byte: b'"', reason },
                ..
            } => {
                assert!(reason.contains("need ] or ,"));
            }
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_array_unclosed() {
        let err = parse_err(br#"[ "open" "#);
        match err {
            error::Error {
                kind: UnexpectedEOF,
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_array_trailing_comma() {
        let err = parse_err(br#"[ "a", ]"#);
        match err {
            error::Error {
                kind: InvalidByte { byte: b']', reason },
                ..
            } => {
                assert!(reason.contains("looking for beginning of value"));
            }
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    // ==========================================
    // Boolean Helpers
    // ==========================================

    /// Helper to parse input directly into a bool.
    fn parse_bool(input: &str) -> bool {
        let mut parser = Parser::new(input.as_bytes());
        match parser.parse().expect("Failed to parse bool") {
            Value::Bool(b) => b,
            val => panic!("Expected Value::Bool, got {:?}", val),
        }
    }

    /// Helper to assert an item in a slice is a specific bool
    fn assert_array_bool(arr: &[Value], index: usize, expected: bool) {
        match &arr[index] {
            Value::Bool(b) => assert_eq!(*b, expected),
            val => panic!("Expected Bool at index {}, got {:?}", index, val),
        }
    }

    /// Helper to check if a value in the map exists and is a specific bool
    fn assert_is_bool(map: &HashMap<String, Value>, key: &str, expected: bool) {
        match map.get(key) {
            Some(Value::Bool(b)) => assert_eq!(*b, expected),
            Some(v) => panic!("Key '{}' exists but is not a bool. Got: {:?}", key, v),
            None => panic!("Key '{}' not found in map", key),
        }
    }

    // ==========================================
    // Boolean Tests
    // ==========================================

    #[test]
    fn test_bool_true() {
        assert!(parse_bool("true"));
    }

    #[test]
    fn test_bool_false() {
        assert!(!parse_bool("false"));
    }

    #[test]
    fn test_bool_whitespace() {
        assert!(parse_bool("  true  "));
        assert!(!parse_bool("\n\tfalse"));
    }

    #[test]
    fn test_array_of_bools() {
        let arr = parse_array("[true, false, true]");
        assert_eq!(arr.len(), 3);
        assert_array_bool(&arr, 0, true);
        assert_array_bool(&arr, 1, false);
        assert_array_bool(&arr, 2, true);
    }

    #[test]
    fn test_object_with_bools() {
        let map = parse_object(r#"{ "is_admin": true, "deleted": false }"#);
        assert_is_bool(&map, "is_admin", true);
        assert_is_bool(&map, "deleted", false);
    }

    #[test]
    fn test_mixed_types_with_bool() {
        let arr = parse_array(r#"[ "start", true, {"flag": false}, [true] ]"#);

        assert_eq!(arr.len(), 4);

        assert_array_string(&arr, 0, "start");

        assert_array_bool(&arr, 1, true);

        match &arr[2] {
            Value::Object(map) => assert_is_bool(map, "flag", false),
            _ => panic!("Index 2 incorrect"),
        }

        match &arr[3] {
            Value::Array(inner) => assert_array_bool(inner, 0, true),
            _ => panic!("Index 3 incorrect"),
        }
    }

    // ==========================================
    // Boolean Error Handling Tests
    // ==========================================

    #[test]
    fn test_err_bool_typo_true() {
        let err = parse_err(b"trus");
        match err {
            error::Error {
                kind: InvalidByte { byte: b's', reason },
                ..
            } => {
                assert!(reason.contains("expected e"));
                assert!(reason.contains("scanning boolean"));
            }
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_bool_typo_false() {
        let err = parse_err(b"falze");
        match err {
            error::Error {
                kind: InvalidByte { byte: b'z', reason },
                ..
            } => {
                assert!(reason.contains("expected s"));
            }
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_bool_incomplete() {
        let err = parse_err(b"fal");
        match err {
            error::Error {
                kind: UnexpectedEOF,
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_bool_case_sensitive() {
        let err = parse_err(b"True");
        match err {
            error::Error {
                kind: InvalidByte { byte: b'T', reason },
                ..
            } => {
                assert!(reason.contains("looking for beginning of value"));
            }
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    // ==========================================
    // Null Helpers
    // ==========================================

    /// Helper to ensure input parses specifically to Value::Null.
    fn parse_null(input: &str) {
        let mut parser = Parser::new(input.as_bytes());
        let val = parser.parse().expect("Failed to parse null");
        match val {
            Value::Null => {} // OK
            _ => panic!("Expected Value::Null, got {:?}", val),
        }
    }

    /// Helper to assert an item in a slice is Value::Null
    fn assert_array_null(arr: &[Value], index: usize) {
        match &arr[index] {
            Value::Null => {}
            val => panic!("Expected Null at index {}, got {:?}", index, val),
        }
    }

    /// Helper to check if a value in the map exists and is Value::Null
    fn assert_is_null(map: &HashMap<String, Value>, key: &str) {
        match map.get(key) {
            Some(Value::Null) => {}
            Some(v) => panic!("Key '{}' exists but is not Null. Got: {:?}", key, v),
            None => panic!("Key '{}' not found in map", key),
        }
    }

    // ==========================================
    // Null Tests
    // ==========================================

    #[test]
    fn test_null_basic() {
        parse_null("null");
    }

    #[test]
    fn test_null_whitespace() {
        parse_null("  null  ");
        parse_null("\n\tnull");
    }

    #[test]
    fn test_array_of_nulls() {
        let arr = parse_array("[null, null, null]");
        assert_eq!(arr.len(), 3);
        assert_array_null(&arr, 0);
        assert_array_null(&arr, 1);
        assert_array_null(&arr, 2);
    }

    #[test]
    fn test_mixed_array_with_null() {
        let arr = parse_array(r#"[ "s", null, true ]"#);
        assert_eq!(arr.len(), 3);
        assert_array_string(&arr, 0, "s");
        assert_array_null(&arr, 1);
        assert_array_bool(&arr, 2, true);
    }

    #[test]
    fn test_object_with_null() {
        let map = parse_object(r#"{ "empty_val": null, "defined": true }"#);
        assert_is_null(&map, "empty_val");
        assert_is_bool(&map, "defined", true);
    }

    #[test]
    fn test_deep_null() {
        let map = parse_object(r#"{ "a": [ { "b": null } ] }"#);
        if let Value::Array(arr) = map.get("a").unwrap()
            && let Value::Object(inner) = &arr[0]
        {
            assert_is_null(inner, "b");
            return;
        }
        panic!("Structure mismatch");
    }

    // ==========================================
    // Null Error Handling Tests
    // ==========================================

    #[test]
    fn test_err_null_typo() {
        let err = parse_err(b"nil");
        match err {
            error::Error {
                kind: InvalidByte { byte: b'i', .. },
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_null_typo_end() {
        let err = parse_err(b"nulL");
        match err {
            error::Error {
                kind: InvalidByte { byte: b'L', .. },
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_null_incomplete() {
        let err = parse_err(b"nu");
        match err {
            error::Error {
                kind: UnexpectedEOF,
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_null_case_sensitive() {
        let err = parse_err(b"Null");
        match err {
            error::Error {
                kind: InvalidByte { byte: b'N', reason },
                ..
            } => {
                assert!(reason.contains("looking for beginning of value"));
            }
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    // ==========================================
    // Number Helpers
    // ==========================================

    /// Helper to extract a Number variant.
    fn parse_number(input: &str) -> Number {
        let mut parser = Parser::new(input.as_bytes());
        match parser.parse().expect("Failed to parse number") {
            Value::Number(n) => n,
            val => panic!("Expected Value::Number, got {:?}", val),
        }
    }

    // ==========================================
    // Number Tests
    // ==========================================

    #[test]
    fn test_number_integer() {
        let n = parse_number("42");
        let i: i64 = n.try_into().unwrap();
        assert_eq!(i, 42);
    }

    #[test]
    fn test_number_negative_integer() {
        let n = parse_number("-123");
        let i: i64 = n.try_into().unwrap();
        assert_eq!(i, -123);
    }

    #[test]
    fn test_number_zero() {
        let n = parse_number("0");
        let i: i64 = n.try_into().unwrap();
        assert_eq!(i, 0);
    }

    #[test]
    fn test_number_negative_zero() {
        let n = parse_number("-0");
        let f: f64 = n.try_into().unwrap();
        assert_eq!(f, 0.0);
        assert!(f.is_sign_negative()); // Rust floats preserve sign of zero
    }

    #[test]
    fn test_number_simple_float() {
        let n = parse_number("12.345");
        let f: f64 = n.try_into().unwrap();
        assert_eq!(f, 12.345);
    }

    #[test]
    fn test_number_negative_float() {
        let n = parse_number("-987.654");
        let f: f64 = n.try_into().unwrap();
        assert_eq!(f, -987.654);
    }

    #[test]
    fn test_number_scientific_lowercase_e() {
        let n = parse_number("12e3");
        let f: f64 = n.try_into().unwrap();
        assert_eq!(f, 12000.0);
    }

    #[test]
    fn test_number_scientific_uppercase_e() {
        let n = parse_number("12E3");
        let f: f64 = n.try_into().unwrap();
        assert_eq!(f, 12000.0);
    }

    #[test]
    fn test_number_scientific_positive() {
        let n = parse_number("1.5e+2");
        let f: f64 = n.try_into().unwrap();
        assert_eq!(f, 150.0);
    }

    #[test]
    fn test_number_scientific_positive_multiple_digits() {
        let n = parse_number("1.5e+11");
        let f: f64 = n.try_into().unwrap();
        assert_eq!(f, 150000000000.0);
    }

    #[test]
    fn test_number_scientific_negative() {
        let n = parse_number("1234e-2");
        let f: f64 = n.try_into().unwrap();
        assert!((f - 12.34).abs() < f64::EPSILON);
    }

    #[test]
    fn test_number_combination() {
        // Combination of negative, zeroes, decimal, and negative exponent
        let n = parse_number("-1203.4506e-3");
        let f: f64 = n.try_into().unwrap();
        assert!((f - -1.2034506).abs() < f64::EPSILON);
    }

    #[test]
    fn test_number_u64_conversion() {
        let n = parse_number("9000");
        let u: u64 = n.try_into().unwrap();
        assert_eq!(u, 9000);
    }

    #[test]
    fn test_number_conversion_errors() {
        let n = parse_number("-5");
        let res: Result<u64, _> = n.try_into();
        assert!(res.is_err());

        let n2 = parse_number("12.34");
        let res2: Result<i64, _> = n2.try_into();
        assert!(res2.is_err());
    }

    #[test]
    fn test_array_of_numbers() {
        let arr = parse_array("[ 0, -100, 3.41, 1e5 ]");
        assert_eq!(arr.len(), 4);

        if let Value::Number(ref n) = arr[2] {
            let f: f64 = n.try_into().unwrap();
            assert_eq!(f, 3.41);
        } else {
            panic!("Expected number at index 2");
        }
    }

    #[test]
    fn test_object_with_numbers() {
        let map = parse_object(r#"{ "id": 123, "score": 99.9 }"#);

        match map.get("id") {
            Some(Value::Number(n)) => {
                let i: i64 = n.try_into().unwrap();
                assert_eq!(i, 123);
            }
            _ => panic!("Expected number for id"),
        }
    }

    // ==========================================
    // Number Error Handling Tests
    // ==========================================

    #[test]
    fn test_err_number_leading_zero() {
        let err = parse_err(b"01");
        match err {
            error::Error {
                kind: InvalidByte { byte: b'1', .. },
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_number_incomplete_exponent() {
        // "1e" is invalid. Must have digits.
        let err = parse_err(b"1e");
        match err {
            error::Error {
                kind: UnexpectedEOF,
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_number_exponent_sign_no_digits() {
        let err = parse_err(b"1e+");
        match err {
            error::Error {
                kind: UnexpectedEOF,
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_number_exponent_with_invalid_char() {
        let err = parse_err(b"1eA");
        match err {
            error::Error {
                kind: InvalidByte { byte: b'A', .. },
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_number_exponent_positive_with_invalid_char() {
        let err = parse_err(b"1e+A");
        match err {
            error::Error {
                kind: InvalidByte { byte: b'A', .. },
                ..
            } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_number_double_negative() {
        let err = parse_err(b"--1");
        match err {
            error::Error {
                kind: InvalidByte { byte: b'-', reason },
                ..
            } => {
                assert!(reason.contains("expected digit"));
            }
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    // ==========================================
    // Number equality
    // ==========================================

    // Helper to make the tests cleaner
    fn num(input: &str) -> Number {
        Number(input.to_string())
    }

    #[test]
    fn test_basic_equality() {
        assert_eq!(num("10"), num("10"));
        assert_eq!(num("0"), num("0"));
        assert_ne!(num("10"), num("11"));
    }

    #[test]
    fn test_cross_type_equality() {
        // Integers and floats are equal
        assert_eq!(num("1"), num("1.0"));
        assert_eq!(num("1"), num("1.00000"));
        assert_eq!(num("0"), num("0.0"));
        assert_eq!(num("-0"), num("0"));
        assert_eq!(num("-0.0"), num("0"));
    }

    #[test]
    fn test_signed_integers() {
        // These will fail the unsigned check and fall through to signed
        assert_eq!(num("-5"), num("-5"));
        assert_eq!(num("-5"), num("-5.0")); // Falls through to float
        assert_ne!(num("-5"), num("5"));
    }

    #[test]
    fn test_scientific_notation() {
        // These fall through to float
        assert_eq!(num("100"), num("1e2"));
        assert_eq!(num("100"), num("1.0e2"));
        assert_eq!(num("0.01"), num("1e-2"));

        // Complex casing (JSON allows e or E)
        assert_eq!(num("500"), num("5E2"));
    }

    #[test]
    fn test_precision_boundaries() {
        // 1. Fits in u64 - Exact comparison
        assert_eq!(u64::MAX, 18446744073709551615);
        let max_u64 = u64::MAX.to_string();
        let max_u64_minus_1 = (u64::MAX - 1).to_string();
        assert_eq!(num(&max_u64), num(&max_u64));
        assert_ne!(num(&max_u64), num(&max_u64_minus_1));

        // 2. Fits in i64, negative - Exact comparison
        assert_eq!(i64::MIN, -9223372036854775808);
        let min_i64 = i64::MIN.to_string();
        assert_eq!(num(&min_i64), num(&min_i64));
    }

    #[test]
    fn test_large_number_precision_loss() {
        // This test documents the behavior of the implementation falling back to f64.
        // These numbers are much larger than u64::MAX. In f64 representation, they
        // don't have enough precision to distinguish a difference of 1 at this scale.

        let huge_a = "1000000000000000000000000000001";
        let huge_b = "1000000000000000000000000000002";

        // They are strictly NOT equal strings, but the equality implementation will say
        // they are because of f64 fallback.
        assert_eq!(num(huge_a), num(huge_b));
    }

    #[test]
    fn test_number_special_ieee_strings_comparison() {
        // Parsed as floats but never equal as per spec. This can't actually occur in
        // JSON.
        assert_ne!(num("NaN"), num("NaN"));

        // Also can't occur in JSON.
        assert_eq!(num("inf"), num("inf"));

        // "Proof" that above is parsed as floats; arbitrary strings fall through and
        // are unequal.
        assert_ne!(num("NaN"), num("null"));
        assert_ne!(num("abc"), num("def"));
    }

    // ==========================================
    // Printing tests
    // ==========================================

    #[test]
    fn test_print_string() {
        let val = Value::String("hello world 🔥".into());

        let expected = r#""hello world 🔥""#;
        assert_eq!(val.to_string(), expected);
    }

    #[test]
    fn test_print_string_escaping() {
        let val = Value::String("hello\nworld\t \"foo\" \\ \x00 \x07 ".into());

        let expected = r#""hello\nworld\t \"foo\" \\ \u0000 \u0007 ""#;
        assert_eq!(val.to_string(), expected);
    }

    #[test]
    fn test_print_bool() {
        let val = Value::Bool(true);

        let expected = "true";
        assert_eq!(val.to_string(), expected);

        let val = Value::Bool(false);

        let expected = "false";
        assert_eq!(val.to_string(), expected);
    }

    #[test]
    fn test_print_null() {
        let val = Value::Null;

        let expected = "null";
        assert_eq!(val.to_string(), expected);
    }

    #[test]
    fn test_print_number() {
        let val = Value::Number(Number("123.2".into()));

        let expected = "123.2";
        assert_eq!(val.to_string(), expected);

        let val = Value::Number(Number("-0".into()));

        let expected = "-0";
        assert_eq!(val.to_string(), expected);
    }

    #[test]
    fn test_print_array() {
        // Single element
        let val = Value::Array(vec![Value::String("foo".into())]);

        let expected = r#"["foo"]"#;
        assert_eq!(val.to_string(), expected);

        // Multiple elements
        let val = Value::Array(vec![
            Value::String("foo".into()),
            Value::String("bar".into()),
            Value::String("baz".into()),
        ]);

        let expected = r#"["foo", "bar", "baz"]"#;
        assert_eq!(val.to_string(), expected);
    }

    #[test]
    fn test_print_object() {
        // Single element
        let val = Value::Object(HashMap::from([("foo".into(), Value::String("bar".into()))]));

        let expected = r#"{"foo": "bar"}"#;
        assert_eq!(val.to_string(), expected);

        // Multiple elements
        let val = Value::Object(HashMap::from([
            ("foo".into(), Value::String("bar".into())),
            ("baz".into(), Value::String("qux".into())),
        ]));

        // Key order is indeterministic
        let expected1 = r#"{"foo": "bar", "baz": "qux"}"#;
        let expected2 = r#"{"baz": "qux", "foo": "bar"}"#;
        let s = val.to_string();

        assert!(s == expected1 || s == expected2);
    }

    // ==========================================
    // "Integration" tests
    // ==========================================

    #[test]
    fn test_complex_integration() {
        // A complex JSON object exercising all parsers.
        let json_input = r#"
        {
            "id": 12345,
            "title": "Complex \u2764 Data",
            "is_active": true,
            "deleted": false,
            "meta": {
                "created_at": "2023-10-27T10:00:00Z",
                "counts": [ 1, 2, 3 ],
                "empty_map": {}
            },
            "values": [
                null,
                -0.5,
                1.23e+4,
                "escaped\nline"
            ],
            "empty_list": []
        }
        "#;

        // Manually build the expected tree
        let mut meta_map = HashMap::new();
        meta_map.insert(
            "created_at".to_string(),
            Value::String("2023-10-27T10:00:00Z".to_string()),
        );
        meta_map.insert(
            "counts".to_string(),
            Value::Array(vec![
                Value::Number(Number("1".to_string())),
                Value::Number(Number("2".to_string())),
                Value::Number(Number("3".to_string())),
            ]),
        );
        meta_map.insert("empty_map".to_string(), Value::Object(HashMap::new()));

        let values_list = vec![
            Value::Null,
            Value::Number(Number("-0.5".to_string())),
            Value::Number(Number("1.23e+4".to_string())), // Matches input string exactly
            Value::String("escaped\nline".to_string()),
        ];

        let mut root_map = HashMap::new();
        root_map.insert("id".to_string(), Value::Number(Number("12345".to_string())));
        root_map.insert(
            "title".to_string(),
            Value::String("Complex ❤ Data".to_string()), // \u2764 parsed to char
        );
        root_map.insert("is_active".to_string(), Value::Bool(true));
        root_map.insert("deleted".to_string(), Value::Bool(false));
        root_map.insert("meta".to_string(), Value::Object(meta_map));
        root_map.insert("values".to_string(), Value::Array(values_list));
        root_map.insert("empty_list".to_string(), Value::Array(vec![]));

        let expected = Value::Object(root_map);

        let mut parser = Parser::new(json_input.as_bytes());
        let result = parser.parse().expect("Should parse complex JSON");

        // Uses (partial) equality impl
        assert_eq!(result, expected);
    }
}
