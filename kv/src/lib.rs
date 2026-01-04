use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use json::Value as JSONValue;
use json::conversions::from_value::TryFromError;

/// Infrastructure kerfuffle (not domain-specific).
pub mod infra;

#[derive(Debug)]
pub struct Store<K: Hash + Eq, V: PartialEq> {
    inner: HashMap<K, V>,
}

#[derive(Debug)]
pub enum CASError<'v, V> {
    NoSuchKey,
    ValueMismatch { requested: V, found: &'v V },
}

impl<K: Hash + Eq, V: PartialEq> Store<K, V> {
    #[expect(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn read(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    pub fn write(&mut self, key: K, value: V) {
        self.inner.insert(key, value);
    }

    pub fn compare_and_swap(&mut self, key: &K, from: V, to: V) -> Result<(), CASError<'_, V>> {
        match self.inner.get_mut(key) {
            Some(v) if *v == from => {
                *v = to;
                Ok(())
            }
            Some(v) => Err(CASError::ValueMismatch {
                requested: from,
                found: v,
            }),
            None => Err(CASError::NoSuchKey),
        }
    }
}

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
