use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Read;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use json::Value as JSONValue;
use json::serde::{Deserialize, Serialize};

use crate::maelstrom::NodeMessageIdGenerator;
use crate::maelstrom::rpc::ReservedErrorCode;
use crate::rpc::{ClientMessage, MessageId};
use crate::state::{PersistenceError, Persistent, State, StateMachine};

pub mod maelstrom;
pub mod rpc;
pub mod serde;
pub mod state;

/// Identifier for nodes in the cluster.
type NodeID = String;

/// Timeout for elections.
///
/// If we do not receive communications within this period, assume there is no leader.
const ELECTION_TIMEOUT: Duration = Duration::from_secs(2);

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const MIN_REPLICATION_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone)]
pub struct Engine<S: StateMachine> {
    /// Underlying raft state.
    state: Arc<Mutex<State<S>>>,
}

impl<S> Engine<S>
where
    S::Command: Deserialize + Debug,
    S::Command: Send + Sync + 'static,
    S: StateMachine + 'static,
{
    /// Create a new Raft engine from a reader, from which persisted state will be
    /// restored.
    pub fn new_from_src(
        id: NodeID,
        cluster_ids: Vec<NodeID>,
        persistence_source: &mut impl Read,
    ) -> Result<Self, PersistenceError> {
        let state = Persistent::<S::Command>::restore(persistence_source)?;
        Ok(Self::new(id, cluster_ids, state))
    }

    /// Create a new Raft engine.
    ///
    /// Does not do anything by itself; call [`Self::start`] afterwards.
    pub fn new(id: NodeID, cluster_ids: Vec<NodeID>, state: Persistent<S::Command>) -> Self {
        assert!(
            !cluster_ids.contains(&id),
            "cluster IDs should not contain self"
        );

        let state = Arc::new(Mutex::new(State::new(id, cluster_ids, state)));
        Self { state }
    }

    /// Launch the Raft engine.
    pub fn start<K, V>(
        &self,
        raft_rx: Receiver<(NodeID, rpc::RaftMessage<S::Command>)>,
        raft_tx: Sender<(NodeID, rpc::RaftMessage<S::Command>)>,
        client_rx: Receiver<(NodeID, rpc::ClientMessage<K, V>)>,
        client_tx: Sender<(NodeID, rpc::ClientMessage<K, V>)>,
        mut msg_id_gen: NodeMessageIdGenerator,
    ) where
        K: Send + 'static + Debug,
        V: Send + 'static + Debug,
        S::Command: Clone,
        S::Command: From<(
            NodeID,
            rpc::ClientMessage<K, V>,
            Sender<(String, ClientMessage<K, V>)>,
        )>,
    {
        // Periodic election launch
        thread::Builder::new()
            .name("raft-election-loop".into())
            .spawn({
                let state = Arc::clone(&self.state);
                let raft_tx = raft_tx.clone();

                move || {
                    // "if many followers become candidates at the same time, votes
                    // could be split so that no candidate obtains a majority. When this
                    // happens, each candidate will time out and start a new election by
                    // incrementing its term and initiating another round"
                    loop {
                        state
                            .lock()
                            .expect("no poison")
                            .maybe_begin_election(raft_tx.clone());

                        // Poll as frequently as feasible. Note, the election deadline
                        // this monitors can be bumped forward *at any time*, so we
                        // cannot just sleep once and wake up. So while we don't have
                        // async niceties and to avoid callback hell, just poll. Jitter
                        // for good measure (break out of simultaneous startup more
                        // efficiently).
                        let d = Duration::from_millis(100);
                        let jitter = Duration::from_secs_f64(d.as_secs_f64() * 0.2 * rand::rand());
                        thread::sleep(d + jitter);
                    }
                }
            })
            .expect("thread creation should always succeed");

        // Periodic log replication
        thread::Builder::new()
            .name("raft-log-replication-loop".into())
            .spawn({
                let state = Arc::clone(&self.state);
                let raft_tx = raft_tx.clone();

                move || {
                    loop {
                        // > If followers crash or run slowly, or if network packets are
                        // > lost, the leader retries AppendEntries RPCs indefinitely
                        // > (even after it has responded to the client) until all
                        // > followers eventually store all log entries.
                        state
                            .lock()
                            .expect("no poison")
                            .replicate_log(raft_tx.clone());

                        thread::sleep(HEARTBEAT_INTERVAL);
                    }
                }
            })
            .expect("thread creation should always succeed");

        // Handle incoming Raft messages
        thread::Builder::new()
            .name("raft-handle-incoming-raft-msgs".into())
            .spawn({
                let state = Arc::clone(&self.state);
                let raft_tx = raft_tx.clone();

                move || {
                    for (peer, msg) in raft_rx.iter() {
                        // Check if another node has a more advanced logical clock.
                        let remote_term = msg.term();
                        state
                            .lock()
                            .expect("no poison")
                            .maybe_step_down(remote_term);

                        match msg {
                            rpc::RaftMessage::RequestVote {
                                candidate_term,
                                last_log_index,
                                last_log_term,
                            } => {
                                let resp = state.lock().expect("no poison").handle_vote_request(
                                    peer.clone(),
                                    candidate_term,
                                    last_log_index,
                                    last_log_term,
                                );
                                raft_tx
                                    .send((peer, resp))
                                    .expect("raft message receiver should never hang up");
                            }
                            rpc::RaftMessage::RequestVoteResponse { term, vote_granted } => state
                                .lock()
                                .expect("no poison")
                                .handle_vote_response(peer, term, vote_granted),
                            rpc::RaftMessage::AppendEntries {
                                leader_term: _,
                                prev_log_index: _,
                                prev_log_term: _,
                                entries: _,
                                leader_commit: _,
                            } => {
                                // TODO: business logic.
                                eprintln!("handling append entries from {}", peer);

                                state.lock().expect("no poison").become_follower();
                            }
                            rpc::RaftMessage::AppendEntriesResponse {
                                current_term: _,
                                success: _,
                            } => unimplemented!("append entries response"),
                        };
                    }

                    panic!("requests sender should never hang up");
                }
            })
            .expect("creation should succeed");

        // Handle incoming client messages
        thread::Builder::new()
            .name("raft-handle-incoming-client-msgs".into())
            .spawn({
                let state = Arc::clone(&self.state);
                let client_tx = client_tx.clone();
                let raft_tx = raft_tx.clone();

                move || {
                    for (client, msg) in client_rx.iter() {
                        // This check is pessimistic; it's safe to reject requests if
                        // we're not the leader. It is NOT safe to reply if just this
                        // check passes, without also confirming reads from quorum.
                        //
                        // TODO: proxy to leader.
                        if !state.lock().expect("no poison").is_leader() {
                            let resp = rpc::ClientMessage::ErrorResponse {
                                in_reply_to: msg.id(),
                                id: msg_id_gen
                                    .next()
                                    .expect("should never run out of IDs")
                                    .get(),
                                code: ReservedErrorCode::TemporarilyUnavailable.into(),
                                text: "not a leader".into(),
                            };

                            client_tx
                                .send((client, resp))
                                .expect("client message receiver should never hang up");

                            continue;
                        };

                        match msg {
                            rpc::ClientMessage::ReadRequest { key: _, id, .. } => client_tx
                                .send((
                                    client,
                                    rpc::ClientMessage::ErrorResponse {
                                        in_reply_to: id,
                                        id: msg_id_gen
                                            .next()
                                            .expect("should never run out of IDs")
                                            .get(),
                                        code: ReservedErrorCode::KeyDoesNotExist.into(),
                                        text: "no such key".into(),
                                    },
                                ))
                                .expect("client receiver should never hang up"),
                            msg @ rpc::ClientMessage::WriteRequest { id: _, .. } => {
                                let cmd: S::Command = (client, msg, client_tx.clone()).into();
                                state.lock().expect("no poison").append(cmd);

                                // Opportunistically replicate immediately, in addition
                                // to the replication loop.
                                state
                                    .lock()
                                    .expect("no poison")
                                    .replicate_log(raft_tx.clone());

                                // ClientMessage::ErrorResponse {
                                //     in_reply_to: id,
                                //     id: msg_id_gen
                                //         .next()
                                //         .expect("should never run out of IDs")
                                //         .get(),
                                //     code: ReservedErrorCode::NotSupported.into(),
                                //     text: "writes not supported yet".into(),
                                // }
                            }
                            rpc::ClientMessage::CASRequest { id, .. } => {
                                client_tx
                                    .send((
                                        client,
                                        ClientMessage::ErrorResponse {
                                            in_reply_to: id,
                                            id: msg_id_gen
                                                .next()
                                                .expect("should never run out of IDs")
                                                .get(),
                                            code: ReservedErrorCode::NotSupported.into(),
                                            text: "cas not supported yet".into(),
                                        },
                                    ))
                                    .expect("client receiver should never hang up");
                            }
                            rpc::ClientMessage::ReadResponse { .. }
                            | rpc::ClientMessage::WriteResponse { .. }
                            | rpc::ClientMessage::CASResponse { .. }
                            | rpc::ClientMessage::ErrorResponse { .. } => unimplemented!(
                                "client responses should never be routed to nodes, only clients"
                            ),
                        };
                    }

                    panic!("requests sender should never hang up");
                }
            })
            .expect("creation should succeed");
    }
}

impl<K, V> StateMachine for HashMap<K, V>
where
    K: Eq + std::hash::Hash,
    K: Send + Debug,
    V: Send + Debug + Clone + PartialEq,
{
    type Command = Command<K, V>;

    fn apply(&mut self, cmd: Self::Command) {
        let client = cmd
            .client
            .expect("should never try to apply invalidated log entry");
        let chan = cmd
            .respond
            .expect("should never try to apply invalidated log entry");
        let in_reply_to = cmd
            .in_reply_to
            .expect("should never try to apply invalidated log entry");

        let id = NodeMessageIdGenerator
            .next()
            .expect("should never run out of IDs")
            .get();

        let resp = match cmd.op {
            Operation::Read { key } => {
                if let Some(v) = self.get(&key) {
                    rpc::ClientMessage::ReadResponse {
                        in_reply_to,
                        value: v.clone(),
                        id,
                    }
                } else {
                    rpc::ClientMessage::ErrorResponse {
                        in_reply_to,
                        id,
                        code: ReservedErrorCode::KeyDoesNotExist.into(),
                        text: "no such key".into(),
                    }
                }
            }
            Operation::Write { key, value } => {
                self.insert(key, value);
                rpc::ClientMessage::WriteResponse { in_reply_to, id }
            }
            Operation::CAS { key, from, to } => match self.get_mut(&key) {
                Some(v) if *v == from => {
                    *v = to;

                    rpc::ClientMessage::CASResponse { in_reply_to, id }
                }
                Some(v) => rpc::ClientMessage::ErrorResponse {
                    in_reply_to,
                    id,
                    code: ReservedErrorCode::PreconditionFailed.into(),
                    text: format!("values for key differ (found {:?})", v),
                },
                None => rpc::ClientMessage::ErrorResponse {
                    in_reply_to,
                    id,
                    code: ReservedErrorCode::KeyDoesNotExist.into(),
                    text: "no such key".into(),
                },
            },
        };

        chan.send((client, resp))
            .expect("client response receiver should never hang up");
    }
}

#[derive(Debug, Clone)]
pub struct Command<K, V> {
    op: Operation<K, V>,

    /// Below are optional, as they do not exist on the wire and when deserializing
    /// state (which contains the [`state::Log`] and thus commands) from disk. Restoring
    /// from disk implies a node booting, thus crashed, thus there's no way to respond
    /// to clients anymore anyway! The below are pure in-memory concepts.
    in_reply_to: Option<MessageId>,
    client: Option<NodeID>,
    respond: Option<Sender<(String, rpc::ClientMessage<K, V>)>>,
}

impl<K, V>
    From<(
        NodeID,
        rpc::ClientMessage<K, V>,
        Sender<(String, rpc::ClientMessage<K, V>)>,
    )> for Command<K, V>
{
    fn from(
        (client, msg, chan): (
            NodeID,
            rpc::ClientMessage<K, V>,
            Sender<(String, rpc::ClientMessage<K, V>)>,
        ),
    ) -> Self {
        match msg {
            ClientMessage::ReadRequest { key, id } => Self {
                op: Operation::Read { key },
                in_reply_to: Some(id),
                client: Some(client),
                respond: Some(chan),
            },
            ClientMessage::WriteRequest { key, value, id } => Self {
                op: Operation::Write { key, value },
                in_reply_to: Some(id),
                client: Some(client),
                respond: Some(chan),
            },
            ClientMessage::CASRequest { key, from, to, id } => Self {
                op: Operation::CAS { key, from, to },
                in_reply_to: Some(id),
                client: Some(client),
                respond: Some(chan),
            },
            ClientMessage::ReadResponse { .. }
            | ClientMessage::WriteResponse { .. }
            | ClientMessage::CASResponse { .. }
            | ClientMessage::ErrorResponse { .. } => {
                unreachable!("client responses cannot be turned in Raft log commands")
            }
        }
    }
}

impl<K: Serialize, V: Serialize> Serialize for Command<K, V> {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        self.op.serialize()
    }
}

impl<K: Deserialize, V: Deserialize> Deserialize for Command<K, V> {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        let op = Deserialize::deserialize(value)?;
        Ok(Self {
            op,
            in_reply_to: None,
            client: None,
            respond: None,
        })
    }
}

#[derive(Debug, Clone)]
pub enum Operation<K, V> {
    Read { key: K },
    Write { key: K, value: V },
    CAS { key: K, from: V, to: V },
}

impl<K: Serialize, V: Serialize> Serialize for Operation<K, V> {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        let mut map = HashMap::new();

        match self {
            Operation::Read { key } => {
                map.insert("t".into(), "r".serialize()?);
                map.insert("k".into(), key.serialize()?);
            }
            Operation::Write { key, value } => {
                map.insert("t".into(), "w".serialize()?);
                map.insert("k".into(), key.serialize()?);
                map.insert("v".into(), value.serialize()?);
            }
            Operation::CAS { key, from, to } => {
                map.insert("t".into(), "c".serialize()?);
                map.insert("k".into(), key.serialize()?);
                map.insert("f".into(), from.serialize()?);
                map.insert("t".into(), to.serialize()?);
            }
        }

        Ok(JSONValue::Object(map))
    }
}

impl<K: Deserialize, V: Deserialize> Deserialize for Operation<K, V> {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        if let JSONValue::Object(mut map) = value {
            let Some(JSONValue::String(typ)) = map.remove("t") else {
                return Err(json::serde::DeserializeError::InvalidValue(
                    JSONValue::Object(map),
                ));
            };

            match typ.as_str() {
                "r" => match (map.remove("k"),) {
                    (Some(key),) => Ok(Self::Read {
                        key: Deserialize::deserialize(key)?,
                    }),
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(map),
                    )),
                },
                "w" => match (map.remove("k"), map.remove("v")) {
                    (Some(key), Some(value)) => Ok(Self::Write {
                        key: Deserialize::deserialize(key)?,
                        value: Deserialize::deserialize(value)?,
                    }),
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(map),
                    )),
                },
                "c" => match (map.remove("k"), map.remove("f"), map.remove("t")) {
                    (Some(key), Some(from), Some(to)) => Ok(Self::CAS {
                        key: Deserialize::deserialize(key)?,
                        from: Deserialize::deserialize(from)?,
                        to: Deserialize::deserialize(to)?,
                    }),
                    _ => Err(json::serde::DeserializeError::InvalidValue(
                        JSONValue::Object(map),
                    )),
                },
                _ => Err(json::serde::DeserializeError::InvalidValue(
                    JSONValue::Object(map),
                )),
            }
        } else {
            Err(json::serde::DeserializeError::InvalidValue(value))
        }
    }
}
