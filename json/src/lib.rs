use std::{
    collections::HashMap,
    fmt::Display,
    io::{self, BufReader, Bytes, Read},
    iter::Peekable,
    num::{ParseFloatError, ParseIntError},
    str,
};

const STRING_QUOTE: u8 = b'"';
const STRING_ESCAPE_OPEN: u8 = b'\\';

const OBJECT_OPEN: u8 = b'{';
const OBJECT_KV_SEP: u8 = b':';
const OBJECT_ENTRIES_SEP: u8 = b',';
const OBJECT_CLOSE: u8 = b'}';

const ARRAY_OPEN: u8 = b'[';
const ARRAY_SEP: u8 = b',';
const ARRAY_CLOSE: u8 = b']';

#[derive(Debug, PartialEq)]
pub enum Value {
    String(String),
    Number(Number),
    Object(HashMap<String, Value>),
    Array(Vec<Value>),
    Bool(bool),
    Null,
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

#[derive(Debug)]
pub enum UnicodeError {
    UnpairedUnicodeSurrogate(u16),
    InvalidUTF8(str::Utf8Error),
    InvalidUTF8Start(u8),
}

#[derive(Debug)]
pub enum ParseError {
    Io(io::Error),
    UnexpectedEOF,
    InvalidByte { byte: u8, reason: String },
    InvalidEscapeSequence(u8),
    InvalidHexCharacter(char),
    UnicodeError(UnicodeError),
}

impl From<io::Error> for ParseError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<str::Utf8Error> for ParseError {
    fn from(value: str::Utf8Error) -> Self {
        Self::UnicodeError(UnicodeError::InvalidUTF8(value))
    }
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "i/o error: {err}"),
            Self::UnexpectedEOF => {
                write!(f, "unexpected end of stream")
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
            Self::UnicodeError(UnicodeError::UnpairedUnicodeSurrogate(s)) => {
                write!(f, "unpaired Unicode surrogate: '{:x}'", s)
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

impl std::error::Error for ParseError {}

/// A fully-featured, streaming JSON parser.
///
/// Spec: <https://www.json.org/json-en.html>
pub struct Parser<R: Read> {
    stream: Peekable<Bytes<BufReader<R>>>,
    stream_pos: usize,
}

impl<R: Read> Parser<R> {
    pub fn new(reader: R) -> Self {
        let stream = BufReader::new(reader).bytes().peekable();

        Self {
            stream,
            stream_pos: 0,
        }
    }

    pub fn parse(&mut self) -> Result<Value, ParseError> {
        self.visit_value()
    }

    /// Get the next byte from the underlying reader. Calling this method signals a
    /// *need* for a next byte, thus if none is found (reader finished or errored), it
    /// is considered an error.
    fn advance(&mut self) -> Result<u8, ParseError> {
        let byte = self.stream.next().ok_or(ParseError::UnexpectedEOF)??;
        self.stream_pos += 1;
        Ok(byte)
    }

    fn visit_value(&mut self) -> Result<Value, ParseError> {
        self.skip_whitespace()?;
        let val = match self.stream.peek() {
            Some(Ok(STRING_QUOTE)) => self.visit_string().map(Value::String)?,
            Some(Ok(OBJECT_OPEN)) => self.visit_object().map(Value::Object)?,
            Some(Ok(ARRAY_OPEN)) => self.visit_array().map(Value::Array)?,
            Some(Ok(b't')) | Some(Ok(b'f')) => self.visit_bool().map(Value::Bool)?,
            Some(Ok(b'n')) => self.visit_null().map(|_| Value::Null)?,
            Some(Ok(b'-')) | Some(Ok(b'0'..=b'9')) => self.visit_number().map(Value::Number)?,
            Some(Ok(byte)) => {
                return Err(ParseError::InvalidByte {
                    byte: *byte,
                    reason: "invalid byte looking for beginning of value".into(),
                });
            }
            _ => return Err(ParseError::UnexpectedEOF),
        };
        self.skip_whitespace()?;

        Ok(val)
    }

    /// Skips all upcoming whitespace, if present.
    fn skip_whitespace(&mut self) -> Result<(), ParseError> {
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

    fn visit_object(&mut self) -> Result<HashMap<String, Value>, ParseError> {
        {
            let byte = self.advance()?;
            if byte != OBJECT_OPEN {
                return Err(ParseError::InvalidByte {
                    byte,
                    reason: format!("expected {} for start of object", OBJECT_OPEN as char),
                });
            };
        }

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
                    break Err(ParseError::InvalidByte {
                        byte,
                        reason: format!("parsing object, need {} after key", OBJECT_KV_SEP as char),
                    });
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
                    break Err(ParseError::InvalidByte {
                        byte,
                        reason: format!(
                            "parsing object, need {} or {} after parsing a key/value pair",
                            OBJECT_CLOSE as char, OBJECT_ENTRIES_SEP as char,
                        ),
                    });
                }
            }
        }
    }

    fn visit_string(&mut self) -> Result<String, ParseError> {
        {
            let byte = self.advance()?;
            if byte != STRING_QUOTE {
                return Err(ParseError::InvalidByte {
                    byte,
                    reason: format!("expected {} for start of string", STRING_QUOTE as char),
                });
            };
        }

        let mut s = String::new();

        // JSON uses UTF-16 to represent high code points. It's thus possible to
        // encounter two back-to-back `\u1234` escape sequences, forming a surrogate
        // pair. These need to be processed together; individually they are invalid.
        // Thus we collect them, and flush runs of Unicode escapes out once they end.
        let mut codepoint_escapes: Vec<u16> = Vec::new();
        let flush_escapes = |cps: &mut Vec<u16>, s: &mut String| -> Result<(), ParseError> {
            let chars = char::decode_utf16(cps.iter().copied())
                .map(|r| r.map_err(|e| e.unpaired_surrogate()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|s| ParseError::UnicodeError(UnicodeError::UnpairedUnicodeSurrogate(s)))?;

            for c in chars {
                s.push(c);
            }

            cps.clear();

            Ok(())
        };

        loop {
            match self.advance()? {
                STRING_QUOTE => {
                    flush_escapes(&mut codepoint_escapes, &mut s)?;
                    break Ok(s);
                }
                STRING_ESCAPE_OPEN => match self.advance()? {
                    b'"' => {
                        flush_escapes(&mut codepoint_escapes, &mut s)?;
                        s.push('"');
                    }
                    b'\\' => {
                        flush_escapes(&mut codepoint_escapes, &mut s)?;
                        s.push('\\');
                    }
                    b'/' => {
                        flush_escapes(&mut codepoint_escapes, &mut s)?;
                        s.push('/');
                    }
                    b'b' => {
                        flush_escapes(&mut codepoint_escapes, &mut s)?;
                        s.push('\x08');
                    }
                    b'f' => {
                        flush_escapes(&mut codepoint_escapes, &mut s)?;
                        s.push('\x0C');
                    }
                    b'n' => {
                        flush_escapes(&mut codepoint_escapes, &mut s)?;
                        s.push('\n');
                    }
                    b'r' => {
                        flush_escapes(&mut codepoint_escapes, &mut s)?;
                        s.push('\r');
                    }
                    b't' => {
                        flush_escapes(&mut codepoint_escapes, &mut s)?;
                        s.push('\t');
                    }
                    b'u' => {
                        let codepoint = self.collect_hex()?;
                        codepoint_escapes.push(codepoint);
                    }
                    b => break Err(ParseError::InvalidEscapeSequence(b)),
                },
                byte @ 0..=127 => {
                    flush_escapes(&mut codepoint_escapes, &mut s)?;
                    s.push(byte as char); // ASCII: encoding == codepoint
                }
                byte => {
                    flush_escapes(&mut codepoint_escapes, &mut s)?;

                    debug_assert!(
                        0b1000_0000 & byte != 0,
                        "MSB is zero, aka continuation bit exists"
                    );
                    let mut bytes = [0u8; 4];
                    bytes[0] = byte;

                    // Assume UTF8, and consume one code point. For pattern see
                    // https://en.wikipedia.org/wiki/UTF-8#Description. A bit repetitive
                    // below, but this way we only touch subsequent bytes if the start
                    // byte actually looks OK. The `str` will be stack-allocated, which
                    // is a neat bonus.
                    if 0b0010_0000u8 & byte == 0 {
                        bytes[1] = self.advance()?;
                        s.push_str(str::from_utf8(&bytes[..2])?);
                    } else if 0b0001_0000u8 & byte == 0 {
                        bytes[1] = self.advance()?;
                        bytes[2] = self.advance()?;
                        s.push_str(str::from_utf8(&bytes[..3])?);
                    } else if 0b0000_1000u8 & byte == 0 {
                        bytes[1] = self.advance()?;
                        bytes[2] = self.advance()?;
                        bytes[3] = self.advance()?;
                        s.push_str(str::from_utf8(&bytes[..4])?);
                    } else {
                        break Err(ParseError::UnicodeError(UnicodeError::InvalidUTF8Start(
                            byte,
                        )));
                    }
                }
            }
        }
    }

    fn visit_array(&mut self) -> Result<Vec<Value>, ParseError> {
        {
            let byte = self.advance()?;
            if byte != ARRAY_OPEN {
                return Err(ParseError::InvalidByte {
                    byte,
                    reason: format!("expected {} for start of array", ARRAY_OPEN as char),
                });
            };
        }

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
                    break Err(ParseError::InvalidByte {
                        byte,
                        reason: format!(
                            "parsing array, need {} or {} after parsing an array element",
                            ARRAY_CLOSE as char, ARRAY_SEP as char,
                        ),
                    });
                }
            }
        }
    }

    fn visit_bool(&mut self) -> Result<bool, ParseError> {
        let (expected_remainder, result) = match self.advance()? {
            b't' => (b"rue".as_slice(), true),
            b'f' => (b"alse".as_slice(), false),
            byte => {
                return Err(ParseError::InvalidByte {
                    byte,
                    reason: "expected t or f looking for beginning of boolean value".into(),
                });
            }
        };

        for expected in expected_remainder.iter().copied() {
            let got = self.advance()?;
            if expected != got {
                return Err(ParseError::InvalidByte {
                    byte: got,
                    reason: format!(
                        "expected {} scanning boolean value, got {}",
                        expected as char, got as char
                    ),
                });
            }
        }

        Ok(result)
    }

    fn visit_null(&mut self) -> Result<(), ParseError> {
        for expected in b"null".iter().copied() {
            let got = self.advance()?;
            if expected != got {
                return Err(ParseError::InvalidByte {
                    byte: got,
                    reason: format!(
                        "expected {} scanning for null, got {}",
                        expected as char, got as char
                    ),
                });
            }
        }

        Ok(())
    }

    fn visit_number(&mut self) -> Result<Number, ParseError> {
        let mut number = Number(String::with_capacity(1));
        number = self.visit_number_integral_part(number)?;
        number = self.visit_number_fractional_part(number)?;
        number = self.visit_number_exponent_part(number)?;

        match self.stream.peek() {
            // If there's a digit left we might have gotten a leading zero followed by
            // more digits, e.g. `01`. Without fractional or exponent parts, our parsing
            // just exits after `0`, so check.
            Some(Ok(byte @ b'0'..=b'9')) => Err(ParseError::InvalidByte {
                byte: *byte,
                reason: format!(
                    "unexpected digit remaining after processing number ('{number:?}')"
                ),
            }),
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
    fn visit_number_integral_part(&mut self, mut number: Number) -> Result<Number, ParseError> {
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
                    byte => Err(ParseError::InvalidByte {
                        byte,
                        reason: "expected digit after - sign scanning number".into(),
                    }),
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
            byte => Err(ParseError::InvalidByte {
                byte,
                reason: "expected digit or - scanning number".into(),
            }),
        }
    }

    fn visit_number_fractional_part(&mut self, mut number: Number) -> Result<Number, ParseError> {
        match self.stream.peek() {
            Some(Ok(b'.')) => {
                number.0.push('.');
                self.advance()?;
            }
            _ => return Ok(number),
        }

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

    fn visit_number_exponent_part(&mut self, mut number: Number) -> Result<Number, ParseError> {
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
                        return Err(ParseError::InvalidByte {
                            byte,
                            reason: "expected digit after sign scanning number exponent".into(),
                        });
                    }
                }
            }
            byte @ b'0'..=b'9' => number.0.push(byte as char),
            byte => {
                return Err(ParseError::InvalidByte {
                    byte,
                    reason: "expected digit or sign scanning number exponent".into(),
                });
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

    /// Collect exactly 4 hex digits from a JSON escape sequence (which is required to
    /// be exactly 4 long).
    fn collect_hex(&mut self) -> Result<u16, ParseError> {
        let mut val = 0u16;

        for _ in 0..4 {
            // Casting works because all hex digits are ASCII, where UTF8 encoding
            // corresponds to Unicode code points directly.
            let c = self.advance()? as char;

            // If it wasn't a hex but something malformed, it's caught here.
            let digit = c.to_digit(16).ok_or(ParseError::InvalidHexCharacter(c))?;

            val = (val << 4) | (digit as u16);
        }

        Ok(val)
    }
}

/// Note JSON whitespace differs from Unicode whitespace, it's a narrower definition.
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
    fn parse_err(input: &str) -> ParseError {
        let mut parser = Parser::new(input.as_bytes());
        parser.parse().expect_err("Expected error but got success")
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
        let err = parse_err("\"unclosed");
        // Depending on implementation, might trigger IO error or UnexpectedEOF
        match err {
            ParseError::UnexpectedEOF | ParseError::Io(_) => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_invalid_escape_char() {
        // \a is not a valid JSON escape
        let err = parse_err(r#""\a""#);
        match err {
            ParseError::InvalidEscapeSequence(b) => assert_eq!(b, b'a'),
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_invalid_hex_digit() {
        // 'z' is not hex
        let err = parse_err(r#""\u123z""#);
        match err {
            ParseError::InvalidHexCharacter(c) => assert_eq!(c, 'z'),
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_invalid_hex_length() {
        // Ends prematurely
        let err = parse_err(r#""\u12""#);
        match err {
            // It hits the quote '"' while expecting a hex digit
            ParseError::InvalidHexCharacter(c) => assert_eq!(c, '"'),
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_unpaired_surrogate() {
        // High surrogate \uD800 without a following Low surrogate
        let err = parse_err(r#""\ud800""#);
        match err {
            ParseError::UnicodeError(UnicodeError::UnpairedUnicodeSurrogate(u)) => {
                assert_eq!(u, 0xd800);
            }
            _ => panic!("Wrong error type: {:?}", err),
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
        let err = parse_err(r#"{"a": "b",}"#);
        match err {
            ParseError::InvalidByte { byte: b'}', .. } | ParseError::UnexpectedEOF => {}
            _ => panic!("Expected InvalidByte or EOF, got {:?}", err),
        }
    }

    #[test]
    fn test_err_object_missing_colon() {
        let err = parse_err(r#"{"key" "value"}"#);
        match err {
            ParseError::InvalidByte { byte: b'"', .. } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_object_missing_comma() {
        let err = parse_err(r#"{"a": "b" "c": "d"}"#);
        match err {
            ParseError::InvalidByte { byte: b'"', .. } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_object_key_must_be_string() {
        // Keys must be strings.
        let err = parse_err(r#"{ 1: "b" }"#);
        match err {
            ParseError::InvalidByte { byte: b'1', .. } => {}
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
        let err = parse_err(r#"[ "a" "b" ]"#);
        match err {
            ParseError::InvalidByte { byte: b'"', reason } => {
                assert!(reason.contains("need ] or ,"));
            }
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_array_unclosed() {
        let err = parse_err(r#"[ "open" "#);
        match err {
            ParseError::UnexpectedEOF => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_array_trailing_comma() {
        let err = parse_err(r#"[ "a", ]"#);
        match err {
            ParseError::InvalidByte { byte: b']', reason } => {
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
        let err = parse_err("trus");
        match err {
            ParseError::InvalidByte { byte: b's', reason } => {
                assert!(reason.contains("expected e"));
                assert!(reason.contains("scanning boolean"));
            }
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_bool_typo_false() {
        let err = parse_err("falze");
        match err {
            ParseError::InvalidByte { byte: b'z', reason } => {
                assert!(reason.contains("expected s"));
            }
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_bool_incomplete() {
        let err = parse_err("fal");
        match err {
            ParseError::UnexpectedEOF => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_bool_case_sensitive() {
        let err = parse_err("True");
        match err {
            ParseError::InvalidByte { byte: b'T', reason } => {
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
        let err = parse_err("nil");
        match err {
            ParseError::InvalidByte { byte: b'i', .. } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_null_typo_end() {
        let err = parse_err("nulL");
        match err {
            ParseError::InvalidByte { byte: b'L', .. } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_null_incomplete() {
        let err = parse_err("nu");
        match err {
            ParseError::UnexpectedEOF => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_null_case_sensitive() {
        let err = parse_err("Null");
        match err {
            ParseError::InvalidByte { byte: b'N', reason } => {
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
        let err = parse_err("01");
        match err {
            ParseError::InvalidByte { byte: b'1', .. } => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_number_incomplete_exponent() {
        // "1e" is invalid. Must have digits.
        let err = parse_err("1e");
        match err {
            ParseError::UnexpectedEOF => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_number_exponent_sign_no_digits() {
        let err = parse_err("1e+");
        match err {
            ParseError::UnexpectedEOF => {}
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_number_double_negative() {
        let err = parse_err("--1");
        match err {
            ParseError::InvalidByte { byte: b'-', reason } => {
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
