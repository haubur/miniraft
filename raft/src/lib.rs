use std::collections::HashMap;
use std::io::Read;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use json::Value as JSONValue;
use json::serde::{Deserialize, Serialize};

use crate::maelstrom::NodeMessageIdGenerator;
use crate::maelstrom::rpc::ReservedErrorCode;
use crate::rpc::ClientMessage;
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
pub struct Engine<C, S: StateMachine> {
    /// Underlying raft state.
    state: Arc<Mutex<State<C, S>>>,
}

impl<C, S> Engine<C, S>
where
    C: Deserialize + std::fmt::Debug,
    C: Send + Sync + 'static,
    S: StateMachine + 'static,
{
    /// Create a new Raft engine from a reader, from which persisted state will be
    /// restored.
    pub fn new_from_src(
        id: NodeID,
        cluster_ids: Vec<NodeID>,
        persistence_source: &mut impl Read,
    ) -> Result<Self, PersistenceError> {
        let state = Persistent::<C>::restore(persistence_source)?;
        Ok(Self::new(id, cluster_ids, state))
    }

    /// Create a new Raft engine.
    ///
    /// Does not do anything by itself; call [`Self::start`] afterwards.
    pub fn new(id: NodeID, cluster_ids: Vec<NodeID>, state: Persistent<C>) -> Self {
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
        raft_rx: Receiver<(NodeID, rpc::RaftMessage<C>)>,
        raft_tx: Sender<(NodeID, rpc::RaftMessage<C>)>,
        client_rx: Receiver<(NodeID, rpc::ClientMessage<K, V>)>,
        client_tx: Sender<(NodeID, rpc::ClientMessage<K, V>)>,
        mut msg_id_gen: NodeMessageIdGenerator,
    ) where
        K: Send + 'static + std::fmt::Debug,
        V: Send + 'static + std::fmt::Debug,
        C: From<(K, V)>,
        C: Clone,
        <S as StateMachine>::Request: From<K>,
        <S as StateMachine>::Response: Into<V>,
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

                move || {
                    for (client, msg) in client_rx.iter() {
                        // TODO: this check is pessimistic; it's safe to reject requests
                        // if we're not the leader. It is NOT safe to reply if just this
                        // check passes, without also confirming reads from quorum.
                        let is_leader = state.lock().expect("no poison").is_leader();

                        let resp = match (is_leader, msg) {
                            (
                                true,
                                rpc::ClientMessage::ReadRequest {
                                    key, message_id, ..
                                },
                            ) => {
                                let req = key.into();
                                match state.lock().expect("no poison").respond(req) {
                                    Some(resp) => {
                                        let value = resp.into();
                                        rpc::ClientMessage::ReadResponse {
                                            message_id: msg_id_gen
                                                .next()
                                                .expect("should never run out of IDs")
                                                .get(),
                                            value,
                                            in_reply_to: message_id,
                                        }
                                    }
                                    None => rpc::ClientMessage::ErrorResponse {
                                        in_reply_to: message_id,
                                        message_id: msg_id_gen
                                            .next()
                                            .expect("should never run out of IDs")
                                            .get(),
                                        code: ReservedErrorCode::KeyDoesNotExist.into(),
                                        text: "no such key".into(),
                                    },
                                }
                            }
                            (
                                true,
                                rpc::ClientMessage::WriteRequest {
                                    key,
                                    value,
                                    message_id,
                                },
                            ) => {
                                let _cmd: C = (key, value).into();
                                // state.lock().expect("no poison").append(cmd);

                                // TODO: replicate + wait

                                ClientMessage::ErrorResponse {
                                    in_reply_to: message_id,
                                    message_id: msg_id_gen
                                        .next()
                                        .expect("should never run out of IDs")
                                        .get(),
                                    code: ReservedErrorCode::NotSupported.into(),
                                    text: "writes not supported yet".into(),
                                }
                            }
                            (true, rpc::ClientMessage::CASRequest { message_id, .. }) => {
                                ClientMessage::ErrorResponse {
                                    in_reply_to: message_id,
                                    message_id: msg_id_gen
                                        .next()
                                        .expect("should never run out of IDs")
                                        .get(),
                                    code: ReservedErrorCode::NotSupported.into(),
                                    text: "cas not supported yet".into(),
                                }
                            }
                            (
                                false,
                                rpc::ClientMessage::ReadRequest { message_id, .. }
                                | rpc::ClientMessage::WriteRequest { message_id, .. }
                                | rpc::ClientMessage::CASRequest { message_id, .. },
                            ) => ClientMessage::ErrorResponse {
                                in_reply_to: message_id,
                                message_id: msg_id_gen
                                    .next()
                                    .expect("should never run out of IDs")
                                    .get(),
                                code: ReservedErrorCode::TemporarilyUnavailable.into(),
                                text: "not a leader".into(),
                            },
                            (
                                _,
                                rpc::ClientMessage::ReadResponse { .. }
                                | rpc::ClientMessage::WriteResponse { .. }
                                | rpc::ClientMessage::CASResponse { .. }
                                | rpc::ClientMessage::ErrorResponse { .. },
                            ) => unimplemented!(
                                "client responses should never be routed to nodes, only clients"
                            ),
                        };

                        client_tx
                            .send((client, resp))
                            .expect("client message receiver should never hang up");
                    }

                    panic!("requests sender should never hang up");
                }
            })
            .expect("creation should succeed");
    }
}

impl<K, V> StateMachine for HashMap<K, V>
where
    K: Eq + std::hash::Hash + Send,
    V: Send + Clone,
{
    type Command = (K, V);
    type Request = K;
    type Response = V;

    fn apply(&mut self, (k, v): (K, V)) {
        self.insert(k, v);
    }

    fn respond(&self, key: K) -> Option<V> {
        self.get(&key).cloned()
    }
}

#[derive(Debug, Clone, Default)]
pub struct Set<K, V> {
    key: K,
    value: V,
}

impl<K, V> From<(K, V)> for Set<K, V> {
    fn from((key, value): (K, V)) -> Self {
        Self { key, value }
    }
}

impl<K: Serialize, V: Serialize> Serialize for Set<K, V> {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        Ok(JSONValue::Object(HashMap::from([
            ("key".into(), self.key.serialize()?),
            ("value".into(), self.value.serialize()?),
        ])))
    }
}

impl<K: Deserialize, V: Deserialize> Deserialize for Set<K, V> {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        if let JSONValue::Object(mut map) = value {
            match (map.remove("key"), map.remove("value")) {
                (Some(key), Some(value)) => Ok(Self {
                    key: Deserialize::deserialize(key)?,
                    value: Deserialize::deserialize(value)?,
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
