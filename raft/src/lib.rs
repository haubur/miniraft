use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::io::{Seek, Write};
use std::net::ToSocketAddrs;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use json::Value as JSONValue;
use json::serde::{Deserialize, Serialize};

use crate::maelstrom::NodeMessageID;
use crate::maelstrom::rpc::ReservedErrorCode;
use crate::metrics::{Gauge, InflightProxyRequestsLabels};
use crate::persistence::{PersistenceError, Persistent};
use crate::rpc::{ClientMessage, MessageID};
use crate::state::{State, StateMachine};

pub mod maelstrom;
mod metrics;
pub mod persistence;
pub mod rpc;
mod serde;
mod state;

/// Identifier for nodes in the cluster.
type NodeID = String;

/// A pairing of node ID (client or Raft peer) and some message.
type PeerMessage<M> = (NodeID, M);
type PeerReceiver<M> = Receiver<PeerMessage<M>>;
type PeerSender<M> = Sender<PeerMessage<M>>;

/// Timeout for elections.
///
/// If we do not receive communications within this period, assume there is no leader.
///
/// Should be a few orders of magnitude **less** than the expected Mean Time Between
/// Failures (= node crashes) and one order of magnitude **greater** than broadcast time
/// ("average time it takes a server to send RPCs in parallel to every server in the
/// cluster and receive their responses").
///
/// Note, next to network latency, broadcast time also includes disk I/O, as nodes must
/// persist state to stable storage before responding to Raft RPCs.
const ELECTION_TIMEOUT: Duration = Duration::from_millis(100);

/// Interval at which heartbeats (empty AppendEntries RPCs) are emitted, if no regular
/// AppendEntries were emitted in the meantime.
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

/// Minimum time between log replications emitted from leaders.
///
/// Client requests and leadership promotion can trigger opportunistic, immediate
/// replication events; this interval rate limits those events, allowing batching for
/// better network utilization (1 RPC with N messages over N RPCs with 1 message each).
const MIN_REPLICATION_INTERVAL: Duration = Duration::from_millis(5);

/// Limit inflight concurrency to this number of most recent proxied requests.
///
/// Proxied queries occur when a client contacts a non-leader node, which on behalf of
/// the client forwards the request to the (suspected) current leader for a response
/// (transparently to the client).
///
/// Note: not specific to core Raft. An optimization to allow any client to contact any
/// node for a response (Raft is transparent to clients).s
const MAX_INFLIGHT_PROXIES: usize = 1_024;

/// The engine driving core Raft state, and holding application state in the Raft state
/// machine.
///
/// Responsibilities are:
///
/// - receive client and Raft peer requests and route to Raft core
/// - translate between client domain (key-value store) and Raft core (generic),
///   including for state machine driving
/// - route Raft responses back to peers and clients
/// - persist state to durable storage before responding to RPCs
#[derive(Debug, Clone)]
pub struct Engine<S: StateMachine> {
    /// Underlying raft state.
    state: Arc<Mutex<State<S>>>,
    /// State machine holding current application state.
    state_machine: S,
    /// This node's ID.
    id: NodeID,
}

impl<S> Engine<S>
where
    S: StateMachine + 'static,
    S::Command: Debug,
    S::Command: Deserialize,           // for reading wire messages
    S::Command: Send + Sync + 'static, // for threading
    S::Command: Serialize,             // for persisting state regularly
{
    /// Create a new Raft engine.
    ///
    /// Does not do anything by itself; call [`Self::start`] afterwards.
    pub fn new(id: NodeID, node_ids: Vec<NodeID>, state: Persistent<S::Command>) -> Self {
        assert!(
            !node_ids.contains(&id),
            "cluster IDs should not contain self"
        );

        let state = Arc::new(Mutex::new(State::new(id.clone(), node_ids, state)));
        Self {
            state,
            // State is built up from log on each boot; start with a fresh machine.
            state_machine: Default::default(),
            id,
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
    #[expect(clippy::too_many_arguments)]
    pub fn start<K, V>(
        self,
        raft_rx: PeerReceiver<rpc::RaftMessage<S::Command>>,
        raft_tx: PeerSender<rpc::RaftMessage<S::Command>>,
        client_rx: PeerReceiver<rpc::ClientMessage<K, V>>,
        client_tx: PeerSender<rpc::ClientMessage<K, V>>,
        persist: impl Write + Seek + Send + 'static,
        message_ids: impl Iterator<Item = NodeMessageID> + Send + 'static,
        metrics_addr: impl ToSocketAddrs + Send + 'static,
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
            move || Self::incoming_raft_rpcs_loop(raft_rx, raft_tx, state, &mut machine, persist)
        });

        launch_named_background_task("raft-handle-incoming-client-messages", {
            let state = Arc::clone(&self.state);
            let client_tx = client_tx.clone();
            let raft_tx = raft_tx.clone();
            move || {
                Self::incoming_client_rpcs_loop(
                    self.id.clone(),
                    client_rx,
                    client_tx,
                    raft_tx,
                    state,
                    message_ids,
                )
            }
        });

        launch_named_background_task("raft-metrics-server", || metrics::http::serve(metrics_addr));
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
    ) {
        loop {
            state
                .lock()
                .expect("no poison")
                .maybe_begin_election()
                .into_iter()
                .try_for_each(|msg| raft_tx.send(msg))
                .expect("raft receiver should never hang up");

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
                .replicate_log()
                .into_iter()
                .try_for_each(|msg| raft_tx.send(msg))
                .expect("raft receiver should never hang up");

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
        mut persist: impl Write + Seek,
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

            let responses = match msg {
                rpc::RaftMessage::RequestVote {
                    candidate_term,
                    last_log_index,
                    last_log_term,
                } => vec![(
                    peer.clone(), // single receiver: the candidate
                    state.lock().expect("no poison").handle_vote_request(
                        peer,
                        candidate_term,
                        last_log_index,
                        last_log_term,
                    ),
                )],
                rpc::RaftMessage::RequestVoteResponse { term, vote_granted } => state
                    .lock()
                    .expect("no poison")
                    .handle_vote_response(peer, term, vote_granted),
                rpc::RaftMessage::AppendEntries {
                    term,
                    commit_index,
                    prev_log_index,
                    prev_log_term,
                    entries,
                } => vec![(
                    peer.clone(), // single receiver: the leader
                    state.lock().expect("no poison").handle_append_entries(
                        peer,
                        term,
                        commit_index,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        machine,
                    ),
                )],
                rpc::RaftMessage::AppendEntriesResponse { index, success, .. } => state
                    .lock()
                    .expect("no poison")
                    .handle_append_entries_response(peer, success, index, machine),
            };

            // Fetch persistable version of state and write out. Note, we hold ownership
            // over the persistence destination and access it sequentially only: no risk
            // of concurrent writers conflicting, guaranteed by the type system
            // (assuming, if persistence destination is a file, no other file handles
            // are open to it, outside our little universe here, but we don't control
            // that).
            let p: Persistent<S::Command> = (&*state.lock().expect("no poison")).into();
            if let Err(e) = persist
                .rewind() // NB: not incremental, redo all
                .map_err(PersistenceError::IoError)
                .and_then(|()| p.persist(&mut persist))
                .and_then(|()| persist.flush().map_err(PersistenceError::IoError))
            {
                eprintln!("error persisting state, refusing RPC response: {e}");
                continue;
            }

            responses
                .into_iter()
                .try_for_each(|msg| raft_tx.send(msg))
                .expect("raft receiver should never hang up");
        }

        // If we get here we're non-functional due to programming error, blow up and
        // start over.
        panic!("raft request sender should never hang up");
    }

    /// Handles all client messages incoming on the receiver channel, forwarding them to
    /// the core Raft engine, responding on the out channel **once replication
    /// occurred**.
    ///
    /// Client requests arriving while this node is not a leader are **proxied** to the
    /// leader, if possible. The leader response is then sent back to the client.
    ///
    /// ## Panics
    ///
    /// - if any channel's other end closes
    fn incoming_client_rpcs_loop<K, V>(
        node_id: NodeID,
        client_rx: PeerReceiver<rpc::ClientMessage<K, V>>,
        client_tx: PeerSender<rpc::ClientMessage<K, V>>,
        raft_tx: PeerSender<rpc::RaftMessage<S::Command>>,
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
        // Track outstanding proxy requests this Raft node sends on behalf of clients to
        // current leaders.
        let mut proxies = BTreeMap::new();

        for (client, msg) in client_rx.iter() {
            let response_id = message_ids.next().expect("should never run out of IDs");

            metrics::InflightProxyRequests::set(
                proxies.len(),
                InflightProxyRequestsLabels {
                    node: node_id.clone(),
                },
            );

            // Copy bool out of lock, drop lock again ASAP.
            let is_leader = state.lock().expect("no poison").is_leader();

            match (is_leader, msg) {
                (
                    true, // only leaders (should) add client requests to their log
                    req @ (rpc::ClientMessage::WriteRequest { .. }
                    | ClientMessage::ReadRequest { .. }
                    | ClientMessage::CASRequest { .. }),
                ) => {
                    // Client request goes into leader log (in a more efficient format
                    // than raw RPC requests), carrying a callback channel on which the
                    // client response is sent once the state machine applies the
                    // command, implying replication is reached and responding is safe.
                    //
                    // After successfully checking for leadership above, we might have
                    // lost it by now. That's safe: we will (eventually) fail to commit
                    // this new entry, a new leader will wipe it off our log, and we
                    // will never end up replying to this client.
                    let cmd: S::Command = (client, response_id, req, client_tx.clone()).into();
                    state.lock().expect("no poison").append(cmd);

                    // Opportunistically replicate right away for minimum latency. This
                    // is internally rate-limited so safe to call frequently.
                    state
                        .lock()
                        .expect("no poison")
                        .replicate_log()
                        .into_iter()
                        .try_for_each(|msg| raft_tx.send(msg))
                        .expect("raft receiver should never hang up");
                }
                (
                    false, // as non-leader, we might still perform helpful work
                    mut req @ (rpc::ClientMessage::WriteRequest { .. }
                    | ClientMessage::ReadRequest { .. }
                    | ClientMessage::CASRequest { .. }),
                ) => {
                    let leader = state.lock().expect("no poison").current_leader().cloned();

                    // Proxy to a leader if known.
                    let (node, msg) = if let Some(leader) = leader {
                        if proxies.len() > MAX_INFLIGHT_PROXIES {
                            // Full. Make room for this most recent request. Map is
                            // keyed by monotonically increasing counter, so this drops
                            // the oldest dangling request; ~least likely to still have
                            // an interested client waiting.
                            let (key, _) = proxies.pop_first().expect("just checked");
                            eprintln!("proxy: full: dropped {key}");
                        }

                        // We might have become a leader by now while working! That's
                        // OK: our proxy will never receive a response and just time
                        // out. To make it safe, ensure we're not routing to ourselves.
                        assert!(
                            leader != state.lock().expect("no poison").c.id,
                            "leader should always be someone else"
                        );

                        // Swap out message ID. This node's generated message IDs are
                        // unique, so we use it to tell async responses apart later.
                        // Note, if we crash, restart, generate the same node ID again,
                        // client retries, _then_ we receive a _late_ response on the
                        // _first_ proxy request, we will use a new proxy map entry for
                        // an old response; if clients do not reuse IDs that should be
                        // fine (= same ID -> same operation). Our response might differ
                        // but linearizability for the client is retained.
                        let original_id = req.id();
                        let forward_id = response_id; // reuse but rename
                        eprintln!(
                            "proxy: to known leader {leader}: {}({}) -> {}",
                            client,
                            original_id,
                            forward_id.get()
                        );

                        req.set_id(forward_id.get());

                        // Note, inserting original ID (u64) first, client name (string)
                        // second should give better linear scan performance.
                        let res = proxies.insert(forward_id.get(), (original_id, client));
                        assert!(res.is_none(), "node IDs are unique per process");

                        (leader, /* proxy original unchanged */ req)
                    } else {
                        // No (known) leader to proxy to, short-circuit to client
                        // directly.
                        (
                            client,
                            rpc::ClientMessage::ErrorResponse {
                                in_reply_to: req.id(),
                                id: response_id.get(),
                                code: ReservedErrorCode::TemporarilyUnavailable.into(),
                                text: "not a leader and current leader unknown".into(),
                            },
                        )
                    };

                    client_tx
                        .send((node, msg))
                        .expect("client message receiver should never hang up");
                }
                (
                    // Since proxying initially, we might have changed to any other role
                    // before a response arrived. We can always reply to the client
                    // still, as if the response made it to here, it came from a
                    // legitimate then-leader, so is authoritative (= committed), even
                    // if outdated.
                    _,
                    mut resp @ (rpc::ClientMessage::ReadResponse { in_reply_to, .. }
                    | rpc::ClientMessage::WriteResponse { in_reply_to, .. }
                    | rpc::ClientMessage::CASResponse { in_reply_to, .. }
                    | rpc::ClientMessage::ErrorResponse { in_reply_to, .. }),
                ) => {
                    // Check if there's a client awaiting this response.
                    if let Some((original_id, original_client)) = proxies.remove(&in_reply_to) {
                        eprintln!(
                            "proxy: responding for {in_reply_to} -> {original_client}({original_id})"
                        );
                        // Swap back for our own "in reply to"
                        resp.set_id(original_id);

                        client_tx
                            .send((original_client, resp))
                            .expect("client message receiver should never hang up");
                    } else {
                        eprintln!(
                            "proxy: received client response without registered interest in reply to {}: dropping",
                            in_reply_to
                        );
                    };
                }
            };
        }

        // If we get here we're non-functional due to programming error, blow up and
        // start over.
        panic!("client requests sender should never hang up");
    }
}

fn launch_named_background_task<F, T>(name: &str, f: F)
where
    F: FnOnce() -> T,
    F: Send + 'static,
    T: Send + 'static,
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

/// A command for the Raft log, specific to the key-value store use case.
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

/// An efficient, minimal representation of Raft commands for wire communication and
/// disk I/O.
#[derive(Debug, Clone)]
pub enum WireCommand<K, V> {
    /// Note, reading does not alter state, so adding it to the log is not strictly
    /// necessary and wastes space (state machine does not need Reads to reach full
    /// valid state).
    ///
    /// However, reads also require consensus, which we get natively without any special
    /// code paths and complexity by including it here.
    Read {
        key: K,
    },
    Write {
        key: K,
        value: V,
    },
    CAS {
        key: K,
        from: V,
        to: V,
    },
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
