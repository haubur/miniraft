//! Ser/de implementations for domain types.

use std::collections::HashMap;
use std::num::NonZero;

use json::Value as JSONValue;
use json::serde::{Deserialize, Serialize};

use crate::state::{Log, LogEntry, LogIndex, Term};

impl Serialize for LogIndex {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        self.0.serialize()
    }
}

impl Deserialize for LogIndex {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        let v: NonZero<u64> = Deserialize::deserialize(value)?;
        Ok(Self(v))
    }
}

impl Serialize for Term {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        self.0.serialize()
    }
}

impl Deserialize for Term {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        let v: u64 = Deserialize::deserialize(value)?;
        Ok(Self(v))
    }
}

impl<C: Serialize> Serialize for Log<C> {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        self.inner.serialize()
    }
}

impl<C: Deserialize> Deserialize for Log<C> {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        let inner = Deserialize::deserialize(value)?;
        Ok(Self { inner })
    }
}

impl<C: Serialize> Serialize for LogEntry<C> {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        Ok(JSONValue::Object(HashMap::from([
            ("c".to_string(), self.cmd.serialize()?),
            ("t".to_string(), self.term.serialize()?),
        ])))
    }
}

impl<C: Deserialize> Deserialize for LogEntry<C> {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        if let JSONValue::Object(mut map) = value {
            match (map.remove("c"), map.remove("t")) {
                (Some(cmd), Some(term)) => Ok(Self {
                    cmd: Deserialize::deserialize(cmd)?,
                    term: Deserialize::deserialize(term)?,
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
