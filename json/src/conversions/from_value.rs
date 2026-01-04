use std::num::{ParseFloatError, ParseIntError};

use crate::{Number, Value};

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

impl TryFrom<Value> for i64 {
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
