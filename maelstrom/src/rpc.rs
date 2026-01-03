use std::collections::HashMap;

use json::{
    Value as JSONValue,
    serde::{Deserialize, Serialize},
};

use crate::{NodeId, NodeMessageId};

type MessageId = u64;

/// <https://github.com/jepsen-io/maelstrom/blob/cb7f07239012d85d2c0595fd942ddb4613205905/doc/protocol.md#message-bodies>
#[derive(Debug)]
pub struct Message<B> {
    pub source: NodeId,
    pub destination: NodeId,
    pub body: B,
}

impl<B> Serialize for Message<B>
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

impl<B> Deserialize for Message<B>
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

#[derive(Debug)]
pub enum Request<K, V> {
    /// The [init
    /// message](https://github.com/jepsen-io/maelstrom/blob/cb7f07239012d85d2c0595fd942ddb4613205905/doc/protocol.md#initialization).
    Init {
        message_id: MessageId,
        node_id: NodeId,
        node_ids: Vec<NodeId>,
    },

    /// <https://github.com/jepsen-io/maelstrom/blob/cb7f07239012d85d2c0595fd942ddb4613205905/doc/workloads.md#workload-lin-kv>
    KVRead { key: K, message_id: MessageId },
    KVWrite {
        key: K,
        value: V,
        message_id: MessageId,
    },
    KVCAS {
        key: K,
        from: V,
        to: V,
        message_id: MessageId,
    },
}

impl<K: Deserialize, V: Deserialize> Deserialize for Request<K, V> {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        if let JSONValue::Object(mut body) = value {
            let Some(JSONValue::String(typ)) = body.remove("type") else {
                return Err(json::serde::DeserializeError::InvalidValue(
                    JSONValue::Object(body),
                ));
            };

            match typ.as_str() {
                "init" => match (
                    body.remove("msg_id"),
                    body.remove("node_id"),
                    body.remove("node_ids"),
                ) {
                    (Some(msg_id), Some(node_id), Some(node_ids)) => Ok(Self::Init {
                        message_id: msg_id.try_into()?,
                        node_id: node_id.try_into()?,
                        node_ids: Deserialize::deserialize(node_ids)?,
                    }),
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(body),
                    )),
                },
                "read" => match (body.remove("msg_id"), body.remove("key")) {
                    (Some(msg_id), Some(key)) => Ok(Self::KVRead {
                        message_id: msg_id.try_into()?,
                        key: Deserialize::deserialize(key)?,
                    }),
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(body),
                    )),
                },
                "write" => match (
                    body.remove("msg_id"),
                    body.remove("key"),
                    body.remove("value"),
                ) {
                    (Some(msg_id), Some(key), Some(value)) => Ok(Self::KVWrite {
                        message_id: msg_id.try_into()?,
                        key: Deserialize::deserialize(key)?,
                        value: Deserialize::deserialize(value)?,
                    }),
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(body),
                    )),
                },
                "cas" => match (
                    body.remove("msg_id"),
                    body.remove("key"),
                    body.remove("from"),
                    body.remove("to"),
                ) {
                    (Some(msg_id), Some(key), Some(from), Some(to)) => Ok(Self::KVCAS {
                        message_id: msg_id.try_into()?,
                        key: Deserialize::deserialize(key)?,
                        from: Deserialize::deserialize(from)?,
                        to: Deserialize::deserialize(to)?,
                    }),
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(body),
                    )),
                },
                _ => Err(json::serde::DeserializeError::InvalidValue(
                    JSONValue::Object(body),
                )),
            }
        } else {
            Err(json::serde::DeserializeError::InvalidValue(value))
        }
    }
}

/// A response to Maelstrom client requests.
///
/// A default `V` type is provided such that variants which do not use `V` can be
/// inferred without type annotations (as `V` does not matter in those cases). A default
/// `EC` is provided but left generic for clients to fill in their own error codes.
#[derive(Debug)]
pub enum Response<'v, V = (), EC = ReservedErrorCode> {
    InitOk {
        in_reply_to: MessageId,
    },
    Error {
        in_reply_to: MessageId,
        code: EC,
        text: String,
    },

    /// <https://github.com/jepsen-io/maelstrom/blob/cb7f07239012d85d2c0595fd942ddb4613205905/doc/workloads.md#workload-lin-kv>
    KVReadOK {
        in_reply_to: MessageId,
        /// From the store, we will only ever get a reference to look at.
        value: &'v V,
        message_id: NodeMessageId,
    },
    KVWriteOK {
        in_reply_to: MessageId,
        message_id: NodeMessageId,
    },
    KVCASOK {
        in_reply_to: MessageId,
        message_id: NodeMessageId,
    },
}

impl<'v, V, EC> Serialize for Response<'v, V, EC>
where
    V: Serialize,
    for<'a> u64: From<&'a EC>,
{
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        let mut map: HashMap<String, JSONValue> = HashMap::new();

        match self {
            Response::InitOk { in_reply_to } => {
                map.insert("type".into(), "init_ok".into());
                map.insert("in_reply_to".into(), (*in_reply_to).into());
            }
            Response::Error {
                in_reply_to,
                code,
                text,
            } => {
                let code: u64 = code.into();
                map.insert("type".into(), "error".into());
                map.insert("in_reply_to".into(), (*in_reply_to).into());
                map.insert("code".into(), code.into());
                map.insert("text".into(), text.as_str().into());
            }
            Response::KVReadOK {
                in_reply_to,
                value,
                message_id,
            } => {
                map.insert("type".into(), "read_ok".into());
                map.insert("value".into(), value.serialize()?);
                map.insert("in_reply_to".into(), (*in_reply_to).into());
                map.insert("msg_id".into(), message_id.get().into());
            }
            Response::KVWriteOK {
                in_reply_to,
                message_id,
            } => {
                map.insert("type".into(), "write_ok".into());
                map.insert("in_reply_to".into(), (*in_reply_to).into());
                map.insert("msg_id".into(), message_id.get().into());
            }
            Response::KVCASOK {
                in_reply_to,
                message_id,
            } => {
                map.insert("type".into(), "cas_ok".into());
                map.insert("in_reply_to".into(), (*in_reply_to).into());
                map.insert("msg_id".into(), message_id.get().into());
            }
        }

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

impl From<&ReservedErrorCode> for u64 {
    fn from(value: &ReservedErrorCode) -> Self {
        *value as u64
    }
}
