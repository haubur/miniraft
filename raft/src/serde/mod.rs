//! Ser/de implementations for domain types.

use std::collections::HashMap;

use json::Value as JSONValue;
use json::serde::{Deserialize, Serialize};

use crate::state::{Log, LogEntry, Persistent, Term};

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
            ("cmd".to_string(), self.cmd.serialize()?),
            ("term".to_string(), self.term.serialize()?),
        ])))
    }
}

impl<C: Deserialize> Deserialize for LogEntry<C> {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        if let JSONValue::Object(mut map) = value {
            match (map.remove("cmd"), map.remove("term")) {
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

impl<C: Serialize> Serialize for Persistent<C> {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        Ok(JSONValue::Object(HashMap::from([
            ("current_term".to_string(), self.current_term.serialize()?),
            ("voted_for".to_string(), self.voted_for.serialize()?),
            ("log".to_string(), self.log.serialize()?),
        ])))
    }
}

impl<C: Deserialize> Deserialize for Persistent<C> {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        if let JSONValue::Object(mut map) = value {
            match (
                map.remove("current_term"),
                map.remove("voted_for"),
                map.remove("log"),
            ) {
                (Some(current_term), Some(voted_for), Some(log)) => Ok(Self {
                    current_term: Deserialize::deserialize(current_term)?,
                    voted_for: Deserialize::deserialize(voted_for)?,
                    log: Deserialize::deserialize(log)?,
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
