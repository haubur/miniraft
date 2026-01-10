use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Read;
use std::num::NonZero;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use json::Value as JSONValue;
use json::serde::{Deserialize, Serialize};

use crate::maelstrom::NodeMessageID;
use crate::maelstrom::rpc::ReservedErrorCode;
use crate::rpc::{ClientMessage, MessageID};
use crate::state::{PersistenceError, Persistent, State, StateMachine};

pub mod maelstrom;
pub mod rpc;
pub mod serde;
pub mod state;

/// Identifier for nodes in the cluster.
type NodeID = String;

/// Index of log entries. Raft is 1-indexed.
pub(crate) type LogIndex = NonZero<u64>;

/// Get the smallest permissible log index allowed in the 1-indexed Raft model.
///
/// (Helper function as we cannot `impl` on a foreign type.)
pub(crate) fn min_log_index() -> LogIndex {
    LogIndex::try_from(1).expect("1 > 0")
}

/// A pairing of node ID (client or Raft peer) and some message.
type PeerMessage<M> = (NodeID, M);
type PeerReceiver<M> = Receiver<PeerMessage<M>>;
type PeerSender<M> = Sender<PeerMessage<M>>;

/// Timeout for elections.
///
/// If we do not receive communications within this period, assume there is no leader.
const ELECTION_TIMEOUT: Duration = Duration::from_secs(2);

/// Interval at which heartbeats (empty AppendEntries RPCs) are emitted, if no regular
/// AppendEntries are emitted.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// Minimum time between log replications emitted from leaders.
///
/// Client requests and leadership promotion can trigger opportunistic, immediate
/// replication events; this interval rate limits those events, allowing batching for
/// better network utilization (1 RPC with N messages over N RPCs with 1 message each).
const MIN_REPLICATION_INTERVAL: Duration = Duration::from_millis(10);

/// The engine driving core Raft state, and holding application state in the Raft state
/// machine.
///
/// Responsibilities are:
///
/// - receive client and Raft peer requests and route to Raft core
/// - translate between client domain (key-value store) and Raft core (generic),
///   including for state machine driving
/// - route Raft responses back to peers and clients
#[derive(Debug, Clone)]
pub struct Engine<S: StateMachine> {
    /// Underlying raft state.
    state: Arc<Mutex<State<S>>>,
    /// State machine holding current application state.
    state_machine: S,
}

impl<S> Engine<S>
where
    S: StateMachine + 'static,
    S::Command: Debug,
    S::Command: Deserialize,           // for reading wire messages
    S::Command: Send + Sync + 'static, // for threading
{
    /// Create a new Raft engine from a reader, from which persisted state will be
    /// restored.
    pub fn new_from_src(
        id: NodeID,
        node_ids: Vec<NodeID>,
        persistence_source: &mut impl Read,
    ) -> Result<Self, PersistenceError> {
        let state = Persistent::<S::Command>::restore(persistence_source)?;
        Ok(Self::new(id, node_ids, state))
    }

    /// Create a new Raft engine.
    ///
    /// Does not do anything by itself; call [`Self::start`] afterwards.
    pub fn new(id: NodeID, node_ids: Vec<NodeID>, state: Persistent<S::Command>) -> Self {
        assert!(
            !node_ids.contains(&id),
            "cluster IDs should not contain self"
        );

        let state = Arc::new(Mutex::new(State::new(id, node_ids, state)));
        Self {
            state,
            // State is built up from log on each log; start with a fresh machine.
            state_machine: Default::default(),
        }
    }

    /// Launch the Raft engine.
    ///
    /// It specifically accepts client RPC calls for key-value store operations.
    ///
    /// Takes ownership, as a given engine can only be started once.
    ///
    /// While running, it accepts client- and Raft-specific messages, and will respond
    /// on the corresponding outgoing channels. It will also put newly created,
    /// non-response messages on the outgoing channels any time it needs.
    pub fn start<K, V, I>(
        self,
        raft_rx: PeerReceiver<rpc::RaftMessage<S::Command>>,
        raft_tx: PeerSender<rpc::RaftMessage<S::Command>>,
        client_rx: PeerReceiver<rpc::ClientMessage<K, V>>,
        client_tx: PeerSender<rpc::ClientMessage<K, V>>,
        message_ids: I,
    ) where
        K: Send + 'static + Debug, // for threading
        V: Send + 'static + Debug, // for threading
        S::Command: Clone,         // for applying from log to state machine
        S::Command: From<(
            // for creating state machine commands from incoming messages + a callback
            // channel for responses
            NodeID,
            NodeMessageID,
            rpc::ClientMessage<K, V>,
            PeerSender<ClientMessage<K, V>>,
        )>,
        I: Iterator<Item = NodeMessageID> + Send + 'static,
    {
        launch_named_background_task("raft-election-loop", {
            let state = Arc::clone(&self.state);
            let raft_tx = raft_tx.clone();
            move || Self::election_loop(raft_tx, state)
        });

        launch_named_background_task("raft-log-replication-loop", {
            let state = Arc::clone(&self.state);
            let raft_tx = raft_tx.clone();
            move || Self::log_replication_loop(raft_tx, state)
        });

        launch_named_background_task("raft-handle-incoming-raft-messages", {
            let state = Arc::clone(&self.state);
            let raft_tx = raft_tx.clone();
            // This task is also responsible for driving the state machine forward.
            let mut machine = self.state_machine; // move out + make mutable
            move || Self::incoming_raft_rpcs_loop(raft_rx, raft_tx, state, &mut machine)
        });

        launch_named_background_task("raft-handle-incoming-client-messages", {
            let state = Arc::clone(&self.state);
            let client_tx = client_tx.clone();
            move || Self::incoming_client_rpcs_loop(client_rx, client_tx, state, message_ids)
        });
    }

    /// Run the election loop, sleeping randomly between checks if a new election term
    /// should be launched.
    ///
    /// Random sleep breaks candidacy live-locks:
    ///
    /// > if many followers become candidates at the same time, votes could be split so
    /// > that no candidate obtains a majority. When this happens, each candidate will
    /// > time out and start a new election by incrementing its term and initiating
    /// > another round
    fn election_loop(
        raft_tx: PeerSender<rpc::RaftMessage<<S as StateMachine>::Command>>,
        state: Arc<Mutex<State<S>>>,
    ) -> ! {
        loop {
            state
                .lock()
                .expect("no poison")
                .maybe_begin_election(raft_tx.clone());

            // Poll as frequently as feasible. Note, the election deadline this monitors
            // can be bumped forward *at any time*, so we cannot just sleep once and
            // wake up. So while we don't have async niceties and to avoid callback
            // hell, just poll. Jitter for good measure (break out of simultaneous
            // startup more efficiently).
            let d = Duration::from_millis(100);
            let jitter = Duration::from_secs_f64(d.as_secs_f64() * 0.2 * rand::rand());
            thread::sleep(d + jitter);
        }
    }

    /// Run the log replication loop.
    ///
    /// While logs are replicating opportunistically whenever possible in response to
    /// client requests, a background loop is still necessary for idle periods of no
    /// clients request. In those, (empty) heartbeats need to be sent to Raft peers to
    /// retain leadership.
    ///
    /// Raft also specifies we retry RPCs like AppendEntries; a background loop covers
    /// this as well.
    ///
    /// > If followers crash or run slowly, or if network packets are lost, the leader
    /// > retries AppendEntries RPCs indefinitely (even after it has responded to the
    /// > client) until all followers eventually store all log entries.
    fn log_replication_loop(
        raft_tx: PeerSender<rpc::RaftMessage<<S as StateMachine>::Command>>,
        state: Arc<Mutex<State<S>>>,
    ) {
        loop {
            state
                .lock()
                .expect("no poison")
                .replicate_log(raft_tx.clone());

            thread::sleep(HEARTBEAT_INTERVAL);
        }
    }

    /// Handles all Raft messages incoming on the receiver channel, forwarding them to
    /// the core Raft engine, responding on the out channel.
    ///
    /// ## Panics
    ///
    /// - if any channel's other end closes
    fn incoming_raft_rpcs_loop(
        raft_rx: PeerReceiver<rpc::RaftMessage<S::Command>>,
        raft_tx: PeerSender<rpc::RaftMessage<S::Command>>,
        state: Arc<Mutex<State<S>>>,
        machine: &mut S,
    ) {
        for (peer, msg) in raft_rx.iter() {
            // Check if another node has a more advanced logical clock.
            let remote_term = msg.term();
            state
                .lock()
                .expect("no poison")
                .maybe_step_down(remote_term);

            // Even if we stepped down to follower, implying we were in the logical
            // past, replying is still useful (e.g. to vote for an eligible candidate
            // peer).

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
                    .handle_vote_response(peer, term, vote_granted, raft_tx.clone()),
                rpc::RaftMessage::AppendEntries {
                    term,
                    commit_index,
                    prev_log_index,
                    prev_log_term,
                    entries,
                } => {
                    let resp = state.lock().expect("no poison").handle_append_entries(
                        peer.clone(),
                        term,
                        commit_index,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        machine,
                    );
                    raft_tx
                        .send((peer, resp))
                        .expect("raft message receiver should never hang up");
                }
                rpc::RaftMessage::AppendEntriesResponse { index, success, .. } => {
                    state
                        .lock()
                        .expect("no poison")
                        .handle_append_entries_response(
                            peer,
                            success,
                            index,
                            raft_tx.clone(),
                            machine,
                        );
                }
            };
        }

        // If we get here we're non-functional due to programming error, blow up and
        // start over.
        panic!("raft request sender should never hang up");
    }

    /// Handles all client messages incoming on the receiver channel, forwarding them to
    /// the core Raft engine, responding on the out channel **once replication
    /// occurred**.
    ///
    /// ## Panics
    ///
    /// - if any channel's other end closes
    fn incoming_client_rpcs_loop<K, V>(
        client_rx: PeerReceiver<rpc::ClientMessage<K, V>>,
        client_tx: PeerSender<rpc::ClientMessage<K, V>>,
        state: Arc<Mutex<State<S>>>,
        mut message_ids: impl Iterator<Item = NodeMessageID>,
    ) where
        S::Command: From<(
            // for creating state machine commands from incoming messages + a callback
            // channel for responses
            NodeID,
            NodeMessageID,
            rpc::ClientMessage<K, V>,
            PeerSender<ClientMessage<K, V>>,
        )>,
    {
        for (client, msg) in client_rx.iter() {
            let response_id = message_ids.next().expect("should never run out of IDs");

            let is_leader = state.lock().expect("no poison").is_leader();
            if !is_leader {
                let resp = if let Some(leader) = state.lock().expect("no poison").current_leader() {
                    // TODO: proxy to leader.
                    rpc::ClientMessage::ErrorResponse {
                        in_reply_to: msg.id(),
                        id: response_id.get(),
                        code: ReservedErrorCode::TemporarilyUnavailable.into(),
                        text: format!("not a leader, current leader {leader}"),
                    }
                } else {
                    rpc::ClientMessage::ErrorResponse {
                        in_reply_to: msg.id(),
                        id: response_id.get(),
                        code: ReservedErrorCode::TemporarilyUnavailable.into(),
                        text: "not a leader and current leader unknown".into(),
                    }
                };

                client_tx
                    .send((client, resp))
                    .expect("client message receiver should never hang up");

                continue;
            };

            match msg {
                rpc::ClientMessage::WriteRequest { .. }
                | ClientMessage::ReadRequest { .. }
                | ClientMessage::CASRequest { .. } => {
                    // Client request goes into log (in a more efficient format than raw
                    // RPC requests), carrying a callback channel on which the client
                    // response is sent once the state machine applies the command,
                    // implying replication is reached and responding is safe.
                    let cmd: S::Command = (client, response_id, msg, client_tx.clone()).into();
                    state.lock().expect("no poison").append(cmd);
                }
                rpc::ClientMessage::ReadResponse { .. }
                | rpc::ClientMessage::WriteResponse { .. }
                | rpc::ClientMessage::CASResponse { .. }
                | rpc::ClientMessage::ErrorResponse { .. } => {
                    unimplemented!("client responses should never be routed to nodes, only clients")
                }
            };
        }

        // If we get here we're non-functional due to programming error, blow up and
        // start over.
        panic!("client requests sender should never hang up");
    }
}

fn launch_named_background_task<F>(name: &str, f: F)
where
    F: FnOnce() + Send + 'static,
{
    thread::Builder::new()
        .name(name.into())
        .spawn(f)
        .expect("(named) thread creation should always succeed");
}

impl<K, V> StateMachine for HashMap<K, V>
where
    K: Eq + std::hash::Hash,
    K: Send + Debug + Clone,
    V: Send + Debug + Clone + PartialEq,
{
    type Command = Command<K, V>;

    fn apply(&mut self, cmd: Self::Command) {
        // If not set, use a bogus default: we will not end up sending this on the wire
        // anyway.
        let id = cmd.response_id.unwrap_or_default();
        let in_reply_to = cmd.in_reply_to.unwrap_or_default();

        let resp = match cmd.inner {
            WireCommand::Read { key } => {
                if let Some(v) = self.get(&key) {
                    eprintln!("applying to state machine: read {key:?}");

                    rpc::ClientMessage::ReadResponse {
                        in_reply_to,
                        value: v.clone(),
                        id,
                    }
                } else {
                    eprintln!("applying to state machine: read {key:?}: no such key");

                    rpc::ClientMessage::ErrorResponse {
                        in_reply_to,
                        id,
                        code: ReservedErrorCode::KeyDoesNotExist.into(),
                        text: "no such key".into(),
                    }
                }
            }
            WireCommand::Write { key, value } => {
                eprintln!("applying to state machine: write {key:?} <- {value:?}");
                self.insert(key, value);
                rpc::ClientMessage::WriteResponse { in_reply_to, id }
            }
            WireCommand::CAS { key, from, to } => match self.get_mut(&key) {
                Some(v) if *v == from => {
                    eprintln!("applying to state machine: CAS {key:?}: {from:?} -> {to:?}");
                    *v = to;

                    rpc::ClientMessage::CASResponse { in_reply_to, id }
                }
                Some(v) => {
                    eprintln!("applying to state machine: CAS {key:?}: {from:?}: conflict");

                    rpc::ClientMessage::ErrorResponse {
                        in_reply_to,
                        id,
                        code: ReservedErrorCode::PreconditionFailed.into(),
                        text: format!("values for key differ (found {:?})", v),
                    }
                }
                None => {
                    eprintln!("applying to state machine: CAS {key:?}: no such key");

                    rpc::ClientMessage::ErrorResponse {
                        in_reply_to,
                        id,
                        code: ReservedErrorCode::KeyDoesNotExist.into(),
                        text: "no such key".into(),
                    }
                }
            },
        };

        if let (Some(client), Some(chan)) = (cmd.client, cmd.respond) {
            chan.send((client, resp))
                .expect("client response receiver should never hang up");
        } else {
            // This can happen when deserializing the log fresh from disk, at which
            // point neither does an in-memory concept like "channel" exist, nor do we
            // need one: there is no client listening anymore (or if it is it will have
            // to retry -- we just crashed and rebooted!).
            eprintln!("applying to state machine: no response channel set")
        }
    }
}

#[derive(Debug, Clone)]
pub struct Command<K, V> {
    inner: WireCommand<K, V>,

    /// Below are Options as they do not exist on the wire and when deserializing state
    /// (which contains the [`state::Log`] and thus commands) from disk.
    ///
    /// Restoring from disk implies a node booting, thus crashed, thus there's no way to
    /// respond to clients anymore anyway. Clients need to retry.
    ///
    /// The below are pure in-memory concepts.
    response_id: Option<MessageID>,
    in_reply_to: Option<MessageID>,
    client: Option<NodeID>,
    respond: Option<PeerSender<rpc::ClientMessage<K, V>>>,
}

impl<K, V>
    From<(
        NodeID,
        NodeMessageID,
        rpc::ClientMessage<K, V>,
        PeerSender<rpc::ClientMessage<K, V>>,
    )> for Command<K, V>
{
    fn from(
        (client, response_id, msg, chan): (
            NodeID,
            NodeMessageID,
            rpc::ClientMessage<K, V>,
            PeerSender<rpc::ClientMessage<K, V>>,
        ),
    ) -> Self {
        match msg {
            ClientMessage::ReadRequest { key, id } => Self {
                inner: WireCommand::Read { key },
                response_id: Some(response_id.get()),
                in_reply_to: Some(id),
                client: Some(client),
                respond: Some(chan),
            },
            ClientMessage::WriteRequest { key, value, id } => Self {
                inner: WireCommand::Write { key, value },
                response_id: Some(response_id.get()),
                in_reply_to: Some(id),
                client: Some(client),
                respond: Some(chan),
            },
            ClientMessage::CASRequest { key, from, to, id } => Self {
                inner: WireCommand::CAS { key, from, to },
                response_id: Some(response_id.get()),
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
        self.inner.serialize()
    }
}

impl<K: Deserialize, V: Deserialize> Deserialize for Command<K, V> {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        let op = Deserialize::deserialize(value)?;
        Ok(Self {
            inner: op,
            response_id: None,
            in_reply_to: None,
            client: None,
            respond: None,
        })
    }
}

#[derive(Debug, Clone)]
pub enum WireCommand<K, V> {
    Read { key: K },
    Write { key: K, value: V },
    CAS { key: K, from: V, to: V },
}

impl<K: Serialize, V: Serialize> Serialize for WireCommand<K, V> {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        let mut map = HashMap::new();

        // Keep key names short for efficiency (or more like simulating what that could
        // look like... we're using JSON so it's not like we care about it. In any case
        // this justifies why a dedicated wire type exists; imagine this could also be a
        // binary format).
        match self {
            WireCommand::Read { key } => {
                map.insert("T".into(), "r".serialize()?);
                map.insert("k".into(), key.serialize()?);
            }
            WireCommand::Write { key, value } => {
                map.insert("T".into(), "w".serialize()?);
                map.insert("k".into(), key.serialize()?);
                map.insert("v".into(), value.serialize()?);
            }
            WireCommand::CAS { key, from, to } => {
                map.insert("T".into(), "c".serialize()?);
                map.insert("k".into(), key.serialize()?);
                map.insert("f".into(), from.serialize()?);
                map.insert("t".into(), to.serialize()?);
            }
        }

        Ok(JSONValue::Object(map))
    }
}

impl<K: Deserialize, V: Deserialize> Deserialize for WireCommand<K, V> {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        if let JSONValue::Object(mut map) = value {
            let Some(JSONValue::String(typ)) = map.remove("T") else {
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
