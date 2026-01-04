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

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}
