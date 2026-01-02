use std::{collections::HashMap, fmt::Debug};

use json::{Value as JSONValue, conversions::from_value::TryFromError};

pub struct Put {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Debug for Put {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Put")
            .field("key", &String::from_utf8_lossy(&self.key))
            .field("value", &String::from_utf8_lossy(&self.value))
            .finish()
    }
}

impl From<&Put> for JSONValue {
    fn from(value: &Put) -> Self {
        Self::Object(HashMap::from([
            ("key".to_string(), value.key.as_slice().into()),
            ("value".to_string(), value.value.as_slice().into()),
        ]))
    }
}

impl TryFrom<JSONValue> for Put {
    type Error = TryFromError;

    fn try_from(value: JSONValue) -> Result<Self, Self::Error> {
        if let JSONValue::Object(mut map) = value {
            match (map.remove("key"), map.remove("value")) {
                (Some(key), Some(value)) => Ok(Self {
                    key: key.try_into()?,
                    value: value.try_into()?,
                }),
                _ => Err(TryFromError {
                    value: JSONValue::Object(map),
                    reason: None,
                }),
            }
        } else {
            Err(TryFromError {
                value,
                reason: None,
            })
        }
    }
}
