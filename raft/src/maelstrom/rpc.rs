use std::collections::HashMap;

use json::Value as JSONValue;
use json::serde::{Deserialize, Serialize};

use super::NodeId;
use crate::rpc::MessageId;

/// <https://github.com/jepsen-io/maelstrom/blob/cb7f07239012d85d2c0595fd942ddb4613205905/doc/protocol.md#message-bodies>
#[derive(Debug)]
pub struct MessageEnvelope<B> {
    pub source: NodeId,
    pub destination: NodeId,
    pub body: B,
}

impl<B> Serialize for MessageEnvelope<B>
where
    B: Serialize,
{
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        let mut map: HashMap<String, JSONValue> = HashMap::new();
        map.insert("src".into(), self.source.as_str().into());
        map.insert("dest".into(), self.destination.as_str().into());
        map.insert("body".into(), self.body.serialize()?);

        Ok(JSONValue::Object(map))
    }
}

impl<B> Deserialize for MessageEnvelope<B>
where
    B: Deserialize,
{
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        if let JSONValue::Object(mut msg) = value {
            match (msg.remove("src"), msg.remove("dest"), msg.remove("body")) {
                (Some(src), Some(dest), Some(body)) => Ok(Self {
                    source: src.try_into()?,
                    destination: dest.try_into()?,
                    body: Deserialize::deserialize(body)?,
                }),
                _ => Err(json::serde::DeserializeError::InvalidValue(
                    JSONValue::Object(msg),
                )),
            }
        } else {
            Err(json::serde::DeserializeError::InvalidValue(value))
        }
    }
}

/// The [init
/// message](https://github.com/jepsen-io/maelstrom/blob/cb7f07239012d85d2c0595fd942ddb4613205905/doc/protocol.md#initialization).
#[derive(Debug)]
pub(crate) struct InitRequest {
    pub(crate) message_id: MessageId,
    pub(crate) node_id: NodeId,
    pub(crate) node_ids: Vec<NodeId>,
}

#[derive(Debug)]
pub(crate) struct InitResponse {
    pub(crate) in_reply_to: MessageId,
}

impl Deserialize for InitRequest {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        if let JSONValue::Object(mut body) = value {
            let Some(JSONValue::String(typ)) = body.remove("type") else {
                return Err(json::serde::DeserializeError::InvalidValue(
                    JSONValue::Object(body),
                ));
            };

            match (
                typ.as_str(),
                body.remove("msg_id"),
                body.remove("node_id"),
                body.remove("node_ids"),
            ) {
                ("init", Some(msg_id), Some(node_id), Some(node_ids)) => Ok(Self {
                    message_id: msg_id.try_into()?,
                    node_id: node_id.try_into()?,
                    node_ids: Deserialize::deserialize(node_ids)?,
                }),
                _ => Err(json::serde::DeserializeError::InvalidValue(
                    JSONValue::Object(body),
                )),
            }
        } else {
            Err(json::serde::DeserializeError::InvalidValue(value))
        }
    }
}

impl Serialize for InitResponse {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        let mut map: HashMap<String, JSONValue> = HashMap::new();
        map.insert("type".into(), "init_ok".into());
        map.insert("in_reply_to".into(), (self.in_reply_to).into());

        Ok(JSONValue::Object(map))
    }
}

/// <https://github.com/jepsen-io/maelstrom/blob/cb7f07239012d85d2c0595fd942ddb4613205905/doc/protocol.md#errors>
///
/// 0-999 are [reserved by
/// Maelstrom](https://github.com/jepsen-io/maelstrom/blob/cb7f07239012d85d2c0595fd942ddb4613205905/doc/protocol.md#errors).
#[derive(Debug, Copy, Clone)]
pub enum ReservedErrorCode {
    Timeout = 0,
    NodeNotFound = 1,
    NotSupported = 10,
    TemporarilyUnavailable = 11,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    KeyAlreadyExists = 21,
    PreconditionFailed = 22,
    TxnConflict = 30,
}

impl From<ReservedErrorCode> for u64 {
    fn from(value: ReservedErrorCode) -> Self {
        value as u64
    }
}
