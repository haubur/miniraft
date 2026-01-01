use std::{
    collections::HashMap,
    fmt::Display,
    io::{self, BufReader, Bytes, Read},
    iter::Peekable,
    str,
};

const STRING_QUOTE: u8 = b'"';
const STRING_ESCAPE_OPEN: u8 = b'\\';
const OBJECT_OPEN: u8 = b'{';
const OBJECT_CLOSE: u8 = b'}';

#[derive(Debug)]
pub enum Value {
    String(String),
    Number(f64),
    Object(HashMap<String, Value>),
    Array(Vec<Value>),
    True,
    False,
    Null,
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
    InvalidEscapeSequence(u8),
    InvalidHex(char),
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
            Self::InvalidEscapeSequence(s) => {
                write!(f, "invalid escape sequence: '{:x}'", s)
            }
            Self::InvalidHex(c) => {
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

pub struct Parser<R: Read> {
    reader: Peekable<Bytes<BufReader<R>>>,
}

impl<R: Read> Parser<R> {
    pub fn new(reader: R) -> Self {
        let bytes = BufReader::new(reader).bytes().peekable();
        Self { reader: bytes }
    }

    pub fn parse(&mut self) -> Result<Value, ParseError> {
        self.visit_value()
    }

    fn visit_value(&mut self) -> Result<Value, ParseError> {
        self.skip_whitespace()?;
        match self.reader.next().ok_or(ParseError::UnexpectedEOF)?? {
            STRING_QUOTE => self.visit_string(),
            _ => todo!("more value types"),
        }
    }

    fn skip_whitespace(&mut self) -> Result<(), ParseError> {
        // If there's no whitespace, do not consume.
        while let Some(Ok(c)) = self.reader.peek() {
            if is_json_whitespace(*c) {
                // Actually consume it.
                self.reader.next().ok_or(ParseError::UnexpectedEOF)??;
            } else {
                break;
            }
        }

        Ok(())
    }

    fn parse_object(&mut self) -> Result<Value, ParseError> {
        let v = Value::Object(HashMap::new());

        assert_eq!(self.reader.next().expect("was peeked")?, OBJECT_OPEN);
        self.skip_whitespace()?;

        if let Some(Ok(b'}')) = self.reader.peek() {
            // Empty object
            return Ok(v);
        }

        todo!("finish implementing object parsing")
    }

    fn visit_string(&mut self) -> Result<Value, ParseError> {
        let mut s = String::new();

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
            let c = self.reader.next().ok_or(ParseError::UnexpectedEOF)??;
            match c {
                STRING_QUOTE => {
                    flush_escapes(&mut codepoint_escapes, &mut s)?;
                    return Ok(Value::String(s));
                }
                STRING_ESCAPE_OPEN => {
                    let c = self.reader.next().ok_or(ParseError::UnexpectedEOF)??;

                    match c {
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
                        b => return Err(ParseError::InvalidEscapeSequence(b)),
                    }
                }
                0..=127 => {
                    flush_escapes(&mut codepoint_escapes, &mut s)?;
                    s.push(c as char); // ASCII: encoding == codepoint
                }
                _ => {
                    flush_escapes(&mut codepoint_escapes, &mut s)?;

                    debug_assert!(
                        0b1000_0000 & c != 0,
                        "MSB is zero, aka continuation bit exists"
                    );
                    let mut bytes = [0u8; 4];
                    bytes[0] = c;

                    // Assume UTF8, and consume one code point. For pattern see
                    // https://en.wikipedia.org/wiki/UTF-8#Description. A bit repetitive
                    // below, but this way we only touch subsequent bytes if the start
                    // byte looks OK.
                    if 0b0010_0000u8 & c == 0 {
                        bytes[1] = self.reader.next().ok_or(ParseError::UnexpectedEOF)??;
                        s.push_str(str::from_utf8(&bytes[..2])?);
                    } else if 0b0001_0000u8 & c == 0 {
                        bytes[1] = self.reader.next().ok_or(ParseError::UnexpectedEOF)??;
                        bytes[2] = self.reader.next().ok_or(ParseError::UnexpectedEOF)??;
                        s.push_str(str::from_utf8(&bytes[..3])?);
                    } else if 0b0000_1000u8 & c == 0 {
                        bytes[1] = self.reader.next().ok_or(ParseError::UnexpectedEOF)??;
                        bytes[2] = self.reader.next().ok_or(ParseError::UnexpectedEOF)??;
                        bytes[3] = self.reader.next().ok_or(ParseError::UnexpectedEOF)??;
                        s.push_str(str::from_utf8(&bytes[..4])?);
                    } else {
                        return Err(ParseError::UnicodeError(UnicodeError::InvalidUTF8Start(c)));
                    }
                }
            }
        }
    }

    /// Collect exactly 4 hex digits from a JSON escape sequence (which is required to
    /// be exactly 4 long).
    fn collect_hex(&mut self) -> Result<u16, ParseError> {
        let mut val = 0u16;

        for _ in 0..4 {
            // Casting works because all hex digits are ASCII, where UTF8 encoding
            // corresponds to Unicode code points directly.
            let c = self.reader.next().ok_or(ParseError::UnexpectedEOF)?? as char;

            // If it wasn't a hex but something malformed, it's caught here.
            let digit = c.to_digit(16).ok_or(ParseError::InvalidHex(c))?;

            val = (val << 4) | (digit as u16);
        }

        Ok(val)
    }
}

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
            ParseError::InvalidHex(c) => assert_eq!(c, 'z'),
            _ => panic!("Wrong error type: {:?}", err),
        }
    }

    #[test]
    fn test_err_invalid_hex_length() {
        // Ends prematurely
        let err = parse_err(r#""\u12""#);
        match err {
            // It hits the quote '"' while expecting a hex digit
            ParseError::InvalidHex(c) => assert_eq!(c, '"'),
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
}
