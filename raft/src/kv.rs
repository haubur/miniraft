use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use json::Value as JSONValue;
use json::conversions::from_value::TryFromError;
use json::serde::{Deserialize, Serialize};

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

#[derive(Default, Clone)]
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

impl Serialize for Put {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        Ok(JSONValue::Object(HashMap::from([
            ("key".to_string(), self.key.as_slice().into()),
            ("value".to_string(), self.value.as_slice().into()),
        ])))
    }
}

impl Deserialize for Put {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        if let JSONValue::Object(mut map) = value {
            match (map.remove("key"), map.remove("value")) {
                (Some(key), Some(value)) => Ok(Self {
                    key: key.try_into()?,
                    value: value.try_into()?,
                }),
                _ => Err(json::serde::DeserializeError::InvalidValue(
                    JSONValue::Object(map),
                )),
            }
        } else {
            Err(json::serde::DeserializeError::InvalidValue(value))
        }
    }
}
