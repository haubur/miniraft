//! See also p. 4 of <https://raft.github.io/raft.pdf>.

use std::collections::HashMap;

use json::Value as JSONValue;
use json::serde::{Deserialize, Serialize};

use crate::state::{Log, Term};

pub(crate) type MessageId = u64;

#[derive(Debug, Clone)]
pub enum Message<K, V, C> {
    Raft(RaftMessage<C>),
    Client(ClientMessage<K, V>),
}

impl<K: Serialize, V: Serialize, C: Serialize> Serialize for Message<K, V, C> {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        match self {
            Message::Raft(r) => r.serialize(),
            Message::Client(kv) => kv.serialize(),
        }
    }
}

impl<K: Deserialize, V: Deserialize, C: Deserialize> Deserialize for Message<K, V, C> {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        // A bit inefficient... but there's no enum tag on this level.
        if let Ok(msg) = Deserialize::deserialize(value.clone()) {
            return Ok(Self::Raft(msg));
        }

        if let Ok(msg) = Deserialize::deserialize(value.clone()) {
            return Ok(Self::Client(msg));
        }

        Err(json::serde::DeserializeError::InvalidValue(value))
    }
}

#[derive(Debug, Clone)]
pub enum RaftMessage<C> {
    AppendEntries {
        /// Leader's term
        leader_term: Term,

        /// Index of log entry immediately preceding new ones.
        prev_log_index: u64,
        /// Term of previous log index entry.
        prev_log_term: Term,

        /// Log entries to store (empty for heartbeat). May send more than one for
        /// efficiency.
        entries: Log<C>,

        /// Leader's commit index.
        leader_commit: u64,
    },
    AppendEntriesResponse {
        /// Current term, for leader to update itself.
        current_term: Term,

        /// True if follower contained entry matching
        /// [`AppendEntriesRequest::prev_log_index`] and
        /// [`AppendEntriesRequest::prev_log_term`].
        success: bool,
    },
    RequestVote {
        /// The requesting candidate's term.
        candidate_term: Term,

        /// Index of candidate's last log entry.
        last_log_index: u64,
        /// Term of candidate's last log entry.
        last_log_term: Term,
    },
    RequestVoteResponse {
        /// Node's term, for candidate to update itself.
        term: Term,
        /// If true, candidate received vote.
        vote_granted: bool,
    },
}

impl<C> RaftMessage<C> {
    /// All message envelopes contain a term. Extract it.
    pub(crate) fn term(&self) -> Term {
        match self {
            RaftMessage::AppendEntries {
                leader_term: term, ..
            }
            | RaftMessage::AppendEntriesResponse {
                current_term: term, ..
            }
            | RaftMessage::RequestVote {
                candidate_term: term,
                ..
            }
            | RaftMessage::RequestVoteResponse { term, .. } => *term,
        }
    }
}

/// See also
/// <https://github.com/jepsen-io/maelstrom/blob/cb7f07239012d85d2c0595fd942ddb4613205905/doc/workloads.md#workload-lin-kv>.
#[derive(Debug, Clone)]
pub enum ClientMessage<K, V> {
    ReadRequest {
        key: K,
        message_id: MessageId,
    },
    WriteRequest {
        key: K,
        value: V,
        message_id: MessageId,
    },
    CASRequest {
        key: K,
        from: V,
        to: V,
        message_id: MessageId,
    },
    ReadResponse {
        in_reply_to: MessageId,
        value: V,
        message_id: MessageId,
    },
    WriteResponse {
        in_reply_to: MessageId,
        message_id: MessageId,
    },
    CASResponse {
        in_reply_to: MessageId,
        message_id: MessageId,
    },
    ErrorResponse {
        in_reply_to: MessageId,
        message_id: MessageId,
        code: u64,
        text: String,
    },
}

impl<C: Serialize> Serialize for RaftMessage<C> {
    fn serialize(&self) -> Result<json::Value, json::serde::SerializeError> {
        let mut map: HashMap<String, JSONValue> = HashMap::new();

        match self {
            RaftMessage::AppendEntries {
                leader_term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                map.insert("type".into(), "append_entries".serialize()?);
                map.insert("leader_term".into(), leader_term.serialize()?);
                map.insert("prev_log_index".into(), prev_log_index.serialize()?);
                map.insert("prev_log_term".into(), prev_log_term.serialize()?);
                map.insert("entries".into(), entries.serialize()?);
                map.insert("leader_commit".into(), leader_commit.serialize()?);
            }
            RaftMessage::AppendEntriesResponse {
                current_term,
                success,
            } => {
                map.insert("type".into(), "append_entries_response".serialize()?);
                map.insert("current_term".into(), current_term.serialize()?);
                map.insert("success".into(), success.serialize()?);
            }
            RaftMessage::RequestVote {
                candidate_term,
                last_log_index,
                last_log_term,
            } => {
                map.insert("type".into(), "request_vote".serialize()?);
                map.insert("candidate_term".into(), candidate_term.serialize()?);
                map.insert("last_log_index".into(), last_log_index.serialize()?);
                map.insert("last_log_term".into(), last_log_term.serialize()?);
            }
            RaftMessage::RequestVoteResponse { term, vote_granted } => {
                map.insert("type".into(), "request_vote_response".serialize()?);
                map.insert("term".into(), term.serialize()?);
                map.insert("vote_granted".into(), vote_granted.serialize()?);
            }
        }

        Ok(JSONValue::Object(map))
    }
}

impl<C: Deserialize> Deserialize for RaftMessage<C> {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        if let JSONValue::Object(mut body) = value {
            let Some(JSONValue::String(typ)) = body.remove("type") else {
                return Err(json::serde::DeserializeError::InvalidValue(
                    JSONValue::Object(body),
                ));
            };

            match typ.as_str() {
                "append_entries" => match (
                    body.remove("leader_term"),
                    body.remove("prev_log_index"),
                    body.remove("prev_log_term"),
                    body.remove("entries"),
                    body.remove("leader_commit"),
                ) {
                    (
                        Some(leader_term),
                        Some(prev_log_index),
                        Some(prev_log_term),
                        Some(entries),
                        Some(leader_commit),
                    ) => Ok(Self::AppendEntries {
                        leader_term: Deserialize::deserialize(leader_term)?,
                        prev_log_index: Deserialize::deserialize(prev_log_index)?,
                        prev_log_term: Deserialize::deserialize(prev_log_term)?,
                        entries: Deserialize::deserialize(entries)?,
                        leader_commit: Deserialize::deserialize(leader_commit)?,
                    }),
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(body),
                    )),
                },
                "append_entries_response" => {
                    match (body.remove("current_term"), body.remove("success")) {
                        (Some(current_term), Some(success)) => Ok(Self::AppendEntriesResponse {
                            current_term: Deserialize::deserialize(current_term)?,
                            success: Deserialize::deserialize(success)?,
                        }),
                        _ => Err(json::serde::DeserializeError::InvalidValue(
                            JSONValue::Object(body),
                        )),
                    }
                }
                "request_vote" => match (
                    body.remove("candidate_term"),
                    body.remove("last_log_index"),
                    body.remove("last_log_term"),
                ) {
                    (Some(candidate_term), Some(last_log_index), Some(last_log_term)) => {
                        Ok(Self::RequestVote {
                            candidate_term: Deserialize::deserialize(candidate_term)?,
                            last_log_index: Deserialize::deserialize(last_log_index)?,
                            last_log_term: Deserialize::deserialize(last_log_term)?,
                        })
                    }
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(body),
                    )),
                },
                "request_vote_response" => match (body.remove("term"), body.remove("vote_granted"))
                {
                    (Some(term), Some(vote_granted)) => Ok(Self::RequestVoteResponse {
                        term: Deserialize::deserialize(term)?,
                        vote_granted: Deserialize::deserialize(vote_granted)?,
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

impl<K: Serialize, V: Serialize> Serialize for ClientMessage<K, V> {
    fn serialize(&self) -> Result<json::Value, json::serde::SerializeError> {
        let mut map: HashMap<String, JSONValue> = HashMap::new();

        match self {
            Self::ReadRequest { key, message_id } => {
                map.insert("type".into(), "read".serialize()?);
                map.insert("key".into(), key.serialize()?);
                map.insert("msg_id".into(), message_id.serialize()?);
            }
            Self::WriteRequest {
                key,
                value,
                message_id,
            } => {
                map.insert("type".into(), "write".serialize()?);
                map.insert("key".into(), key.serialize()?);
                map.insert("value".into(), value.serialize()?);
                map.insert("msg_id".into(), message_id.serialize()?);
            }
            Self::CASRequest {
                key,
                from,
                to,
                message_id,
            } => {
                map.insert("type".into(), "cas".serialize()?);
                map.insert("key".into(), key.serialize()?);
                map.insert("from".into(), from.serialize()?);
                map.insert("to".into(), to.serialize()?);
                map.insert("msg_id".into(), message_id.serialize()?);
            }
            Self::ReadResponse {
                in_reply_to,
                value,
                message_id,
            } => {
                map.insert("type".into(), "read_ok".serialize()?);
                map.insert("in_reply_to".into(), in_reply_to.serialize()?);
                map.insert("value".into(), value.serialize()?);
                map.insert("msg_id".into(), message_id.serialize()?);
            }
            Self::WriteResponse {
                in_reply_to,
                message_id,
            } => {
                map.insert("type".into(), "write_ok".serialize()?);
                map.insert("in_reply_to".into(), in_reply_to.serialize()?);
                map.insert("msg_id".into(), message_id.serialize()?);
            }
            Self::CASResponse {
                in_reply_to,
                message_id,
            } => {
                map.insert("type".into(), "cas_ok".serialize()?);
                map.insert("in_reply_to".into(), in_reply_to.serialize()?);
                map.insert("msg_id".into(), message_id.serialize()?);
            }
            Self::ErrorResponse {
                in_reply_to,
                message_id,
                code,
                text,
            } => {
                map.insert("type".into(), "error".serialize()?);
                map.insert("in_reply_to".into(), in_reply_to.serialize()?);
                map.insert("msg_id".into(), message_id.serialize()?);
                map.insert("code".into(), code.serialize()?);
                map.insert("text".into(), text.serialize()?);
            }
        }

        Ok(JSONValue::Object(map))
    }
}

impl<K: Deserialize, V: Deserialize> Deserialize for ClientMessage<K, V> {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        if let JSONValue::Object(mut body) = value {
            let Some(JSONValue::String(typ)) = body.remove("type") else {
                return Err(json::serde::DeserializeError::InvalidValue(
                    JSONValue::Object(body),
                ));
            };

            match typ.as_str() {
                "read" => match (body.remove("key"), body.remove("msg_id")) {
                    (Some(key), Some(message_id)) => Ok(Self::ReadRequest {
                        key: Deserialize::deserialize(key)?,
                        message_id: Deserialize::deserialize(message_id)?,
                    }),
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(body),
                    )),
                },
                "write" => match (
                    body.remove("key"),
                    body.remove("value"),
                    body.remove("msg_id"),
                ) {
                    (Some(key), Some(value), Some(message_id)) => Ok(Self::WriteRequest {
                        key: Deserialize::deserialize(key)?,
                        value: Deserialize::deserialize(value)?,
                        message_id: Deserialize::deserialize(message_id)?,
                    }),
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(body),
                    )),
                },
                "cas" => match (
                    body.remove("key"),
                    body.remove("from"),
                    body.remove("to"),
                    body.remove("msg_id"),
                ) {
                    (Some(key), Some(from), Some(to), Some(message_id)) => Ok(Self::CASRequest {
                        key: Deserialize::deserialize(key)?,
                        from: Deserialize::deserialize(from)?,
                        to: Deserialize::deserialize(to)?,
                        message_id: Deserialize::deserialize(message_id)?,
                    }),
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(body),
                    )),
                },
                "read_ok" => match (
                    body.remove("in_reply_to"),
                    body.remove("value"),
                    body.remove("msg_id"),
                ) {
                    (Some(in_reply_to), Some(value), Some(message_id)) => Ok(Self::ReadResponse {
                        in_reply_to: Deserialize::deserialize(in_reply_to)?,
                        value: Deserialize::deserialize(value)?,
                        message_id: Deserialize::deserialize(message_id)?,
                    }),
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(body),
                    )),
                },
                "write_ok" => match (body.remove("in_reply_to"), body.remove("msg_id")) {
                    (Some(in_reply_to), Some(message_id)) => Ok(Self::WriteResponse {
                        in_reply_to: Deserialize::deserialize(in_reply_to)?,
                        message_id: Deserialize::deserialize(message_id)?,
                    }),
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(body),
                    )),
                },
                "cas_ok" => match (body.remove("in_reply_to"), body.remove("msg_id")) {
                    (Some(in_reply_to), Some(message_id)) => Ok(Self::CASResponse {
                        in_reply_to: Deserialize::deserialize(in_reply_to)?,
                        message_id: Deserialize::deserialize(message_id)?,
                    }),
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(body),
                    )),
                },
                "error" => match (
                    body.remove("in_reply_to"),
                    body.remove("msg_id"),
                    body.remove("code"),
                    body.remove("text"),
                ) {
                    (Some(in_reply_to), Some(message_id), Some(code), Some(text)) => {
                        Ok(Self::ErrorResponse {
                            in_reply_to: Deserialize::deserialize(in_reply_to)?,
                            message_id: Deserialize::deserialize(message_id)?,
                            code: Deserialize::deserialize(code)?,
                            text: Deserialize::deserialize(text)?,
                        })
                    }
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
