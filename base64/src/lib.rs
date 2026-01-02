use std::fmt;

const ALPHABET: [u8; 64] = *b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const PADDING: u8 = b'=';
const MASK: usize = 0b111_111;

/// Fails at compile time if out of bounds.
///
/// As we index into the alphabet at a maximum of the mask, and that's in bounds, this
/// saves some testing and correctness checking effort (compiler does it for us).
///
/// Note, the bounds check is elided in release mode/`-C opt-level=3` anyway, see
/// [Godbolt](https://godbolt.org/z/Ynjeax6jG) (no `jae` to panic handler).
const _CHECK_MASK_WITHIN_BOUNDS: u8 = ALPHABET[MASK];

/// Encodes the given data as base64, standard alphabet (`+` and `/`), with padding.
pub fn encode(data: &[u8]) -> String {
    let mut vals = Vec::with_capacity(data.len().div_ceil(3) * 4);

    let (chunks, remainder) = data.as_chunks::<3>();
    for chunk in chunks {
        let datum: u32 = (chunk[0] as u32) << 16 | (chunk[1] as u32) << 8 | chunk[2] as u32;
        vals.push(ALPHABET[(datum >> 18) as usize & MASK]);
        vals.push(ALPHABET[(datum >> 12) as usize & MASK]);
        vals.push(ALPHABET[(datum >> 6) as usize & MASK]);
        vals.push(ALPHABET[datum as usize & MASK]);
    }

    match *remainder {
        [l, r] => {
            let datum: u16 = (l as u16) << 8 | r as u16;
            vals.push(ALPHABET[(datum >> 10) as usize & MASK]);
            vals.push(ALPHABET[(datum >> 4) as usize & MASK]);
            vals.push(ALPHABET[(datum << 2) as usize & MASK]);
            vals.push(PADDING);
        }
        [b] => {
            vals.push(ALPHABET[(b >> 2) as usize & MASK]);
            vals.push(ALPHABET[(b << 4) as usize & MASK]);
            vals.push(PADDING);
            vals.push(PADDING);
        }
        [] => { /* nothing to do */ }
        _ => unreachable!("remainder should be guaranteed shorter than chunk length"),
    }

    assert_eq!(vals.len() % 4, 0, "padding invariant violated");

    // SAFETY: all used bytes are ASCII -> valid UTF8
    unsafe { String::from_utf8_unchecked(vals) }
}

#[derive(Debug, PartialEq)]
pub enum DecodeError {
    /// The input length is not a multiple of 4.
    InvalidLength,
    /// The input contains a character not in the Base64 alphabet or padding.
    InvalidByte(usize, u8),
    /// The padding characters (`=`) are in an invalid position or quantity.
    InvalidPadding,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength => write!(f, "encoded text must be a multiple of 4"),
            Self::InvalidByte(pos, byte) => {
                write!(f, "invalid byte {:x} at position {}", byte, pos)
            }
            Self::InvalidPadding => write!(f, "invalid padding"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Reverse lookup table generated at compile time. `0xFF` indicates an invalid
/// character.
const DECODE_MAP: [u8; 256] = {
    let mut map = [0xFF; 256];
    let mut i = 0;
    while i < ALPHABET.len() {
        map[ALPHABET[i] as usize] = i as u8;
        i += 1;
    }
    map
};

pub fn decode(input: &str) -> Result<Vec<u8>, DecodeError> {
    let bytes = input.as_bytes();

    // Must strictly be a multiple of 4... for us anyway
    if !bytes.len().is_multiple_of(4) {
        return Err(DecodeError::InvalidLength);
    }

    // Rough estimate for capacity: 3 bytes for every 4 chars
    let mut output = Vec::with_capacity((input.len() / 4) * 3);

    let (chunks, remainder) = bytes.as_chunks::<4>();
    assert!(remainder.is_empty(), "should be empty after previous check");

    for (i, chunk) in chunks.iter().enumerate() {
        // Helper functino
        let get_byte = |offset: usize| -> Result<u8, DecodeError> {
            let b = chunk[offset];
            let val = DECODE_MAP[b as usize];
            if val == 0xFF {
                return Err(DecodeError::InvalidByte(i * 4 + offset, b));
            }
            Ok(val)
        };

        let mut datum: u32 = 0;

        // These can never be padding in a valid string
        datum |= (get_byte(0)? as u32) << 18;
        datum |= (get_byte(1)? as u32) << 12;

        if chunk[2] == PADDING {
            // We must be at the very end
            if i != chunks.len() - 1 {
                return Err(DecodeError::InvalidPadding);
            }
            if chunk[3] != PADDING {
                return Err(DecodeError::InvalidPadding);
            }
            output.push((datum >> 16) as u8);
            return Ok(output);
        }

        datum |= (get_byte(2)? as u32) << 6;

        if chunk[3] == PADDING {
            if i != chunks.len() - 1 {
                return Err(DecodeError::InvalidPadding);
            }
            output.push((datum >> 16) as u8);
            output.push((datum >> 8) as u8);
            return Ok(output);
        }

        datum |= get_byte(3)? as u32;

        output.push((datum >> 16) as u8);
        output.push((datum >> 8) as u8);
        output.push(datum as u8);
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    // cf. https://datatracker.ietf.org/doc/html/rfc4648#section-10
    #[test]
    fn test_encode_rfc4648_vectors() {
        let vectors = [
            (b"" as &[u8], ""),
            (b"f", "Zg=="),
            (b"fo", "Zm8="),
            (b"foo", "Zm9v"),
            (b"foob", "Zm9vYg=="),
            (b"fooba", "Zm9vYmE="),
            (b"foobar", "Zm9vYmFy"),
        ];

        for (input, expected) in vectors {
            assert_eq!(encode(input), expected, "failed on input: {:?}", input);
        }
    }

    #[test]
    fn test_encode_longer_sentence() {
        let input = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
        let expected = "TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQsIGNvbnNlY3RldHVyIGFkaXBpc2NpbmcgZWxpdCwgc2VkIGRvIGVpdXNtb2QgdGVtcG9yIGluY2lkaWR1bnQgdXQgbGFib3JlIGV0IGRvbG9yZSBtYWduYSBhbGlxdWEuIFV0IGVuaW0gYWQgbWluaW0gdmVuaWFtLCBxdWlzIG5vc3RydWQgZXhlcmNpdGF0aW9uIHVsbGFtY28gbGFib3JpcyBuaXNpIHV0IGFsaXF1aXAgZXggZWEgY29tbW9kbyBjb25zZXF1YXQuIER1aXMgYXV0ZSBpcnVyZSBkb2xvciBpbiByZXByZWhlbmRlcml0IGluIHZvbHVwdGF0ZSB2ZWxpdCBlc3NlIGNpbGx1bSBkb2xvcmUgZXUgZnVnaWF0IG51bGxhIHBhcmlhdHVyLiBFeGNlcHRldXIgc2ludCBvY2NhZWNhdCBjdXBpZGF0YXQgbm9uIHByb2lkZW50LCBzdW50IGluIGN1bHBhIHF1aSBvZmZpY2lhIGRlc2VydW50IG1vbGxpdCBhbmltIGlkIGVzdCBsYWJvcnVtLg==";

        assert_eq!(encode(input), expected);
    }

    #[test]
    fn test_encode_binary_data() {
        // Null byte, high byte, etc.
        let input = &[0u8, 255, 127, 63, 10];
        let expected = "AP9/Pwo=";
        assert_eq!(encode(input), expected);
    }

    #[test]
    fn test_decode_rfc4648_vectors() {
        let vectors = [
            ("", b"" as &[u8]),
            ("Zg==", b"f"),
            ("Zm8=", b"fo"),
            ("Zm9v", b"foo"),
            ("Zm9vYg==", b"foob"),
            ("Zm9vYmE=", b"fooba"),
            ("Zm9vYmFy", b"foobar"),
        ];

        for (input, expected) in vectors {
            let decoded = decode(input).expect("should decode valid input");
            assert_eq!(decoded, expected, "failed on input: {:?}", input);
        }
    }

    #[test]
    fn test_decode_binary_data() {
        let input = "AP9/Pwo=";
        let expected = &[0u8, 255, 127, 63, 10];
        let decoded = decode(input).expect("should decode binary data");
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_decode_invalid_length() {
        assert_eq!(decode("A"), Err(DecodeError::InvalidLength));
        assert_eq!(decode("AA"), Err(DecodeError::InvalidLength));
        assert_eq!(decode("AAA"), Err(DecodeError::InvalidLength));
        assert_eq!(decode("Zm9vY"), Err(DecodeError::InvalidLength));
    }

    #[test]
    fn test_decode_invalid_characters() {
        assert_eq!(decode("Zm$v"), Err(DecodeError::InvalidByte(2, b'$')));
    }

    #[test]
    fn test_decode_invalid_padding_placement() {
        assert_eq!(decode("Z=9v"), Err(DecodeError::InvalidByte(1, b'=')));
        assert_eq!(decode("Zm=v"), Err(DecodeError::InvalidPadding));
    }

    #[test]
    fn test_encode_decode_round_trip() {
        let input = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

        let encoded = super::encode(input);
        let decoded = decode(&encoded).expect("round trip failed");
        assert_eq!(input.as_slice(), decoded.as_slice());
    }
}
