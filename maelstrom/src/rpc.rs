use std::collections::HashMap;

use json::{Value as JSONValue, conversions::from_value::TryFromError};

type MessageId = u64;
type NodeId = String;

/// <https://github.com/jepsen-io/maelstrom/blob/cb7f07239012d85d2c0595fd942ddb4613205905/doc/protocol.md#message-bodies>
#[derive(Debug)]
pub struct MessageEnvelope<B> {
    pub source: NodeId,
    pub destination: NodeId,
    pub body: B,
}

impl<B> From<&MessageEnvelope<B>> for JSONValue
where
    for<'a> JSONValue: From<&'a B>,
{
    fn from(value: &MessageEnvelope<B>) -> Self {
        let mut map: HashMap<String, JSONValue> = HashMap::new();
        map.insert("src".into(), value.source.as_str().into());
        map.insert("dest".into(), value.destination.as_str().into());
        map.insert("body".into(), (&value.body).into());

        JSONValue::Object(map)
    }
}

impl<B: TryFrom<JSONValue, Error = TryFromError>> TryFrom<JSONValue> for MessageEnvelope<B> {
    type Error = TryFromError;

    fn try_from(value: JSONValue) -> Result<Self, Self::Error> {
        if let JSONValue::Object(mut req) = value {
            match (req.remove("src"), req.remove("dest"), req.remove("body")) {
                (Some(src), Some(dest), Some(body)) => Ok(Self {
                    source: src.try_into()?,
                    destination: dest.try_into()?,
                    body: body.try_into()?,
                }),
                _ => Err(TryFromError {
                    value: JSONValue::Object(req),
                    reason: Some("required keys missing".into()),
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

#[derive(Debug)]
pub enum Request {
    /// <https://github.com/jepsen-io/maelstrom/blob/cb7f07239012d85d2c0595fd942ddb4613205905/doc/protocol.md#initialization>
    Init {
        message_id: MessageId,
        node_id: NodeId,
        node_ids: Vec<NodeId>,
    },
}

impl TryFrom<JSONValue> for Request {
    type Error = TryFromError;

    fn try_from(value: JSONValue) -> Result<Self, Self::Error> {
        if let JSONValue::Object(mut body) = value {
            let Some(JSONValue::String(typ)) = body.remove("type") else {
                return Err(TryFromError {
                    value: JSONValue::Object(body),
                    reason: Some("no request 'type' annotation".into()),
                });
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
                        node_ids: node_ids.try_into()?,
                    }),
                    _ => Err(TryFromError {
                        value: JSONValue::Object(body),
                        reason: Some("required keys missing".into()),
                    }),
                },
                s => Err(TryFromError {
                    value: JSONValue::Object(body),
                    reason: Some(format!("unknown 'type' annotation: {}", s).into()),
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

#[derive(Debug)]
pub enum Response {
    InitOk { in_reply_to: MessageId },
}

impl From<&Response> for JSONValue {
    fn from(value: &Response) -> Self {
        let mut map: HashMap<String, JSONValue> = HashMap::new();

        match value {
            &Response::InitOk { in_reply_to } => {
                map.insert("type".into(), "init_ok".into());
                map.insert("in_reply_to".into(), in_reply_to.into());
            }
        }

        JSONValue::Object(map)
    }
}
