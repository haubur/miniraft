use std::cmp::{self, Ordering};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{self, Debug, Display};
use std::io::{self, Read, Write};
use std::mem;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use json::error::Error as JSONError;
use json::serde::{Deserialize, DeserializeError, Serialize, SerializeError};

use crate::{
    ELECTION_TIMEOUT, HEARTBEAT_INTERVAL, LogIndex, MIN_REPLICATION_INTERVAL, NodeID, rpc,
};

/// Abstraction for a state machine, to which Raft applies commands from its log
/// entries.
pub trait StateMachine: Default + Send + std::fmt::Debug {
    type Command: Debug + Clone; // Need to clone to pull out of log.

    fn apply(&mut self, cmd: Self::Command);
}

/// An election term.
#[derive(Debug, Clone, Copy, Default, PartialEq, PartialOrd, Eq, Ord)]
pub struct Term(pub(super) u64);

impl Term {
    fn set(&mut self, to: Term) {
        assert!(to > *self, "need to advance forward");

        *self = to;
    }

    fn step(&mut self) {
        self.set(Self(self.0 + 1));
    }
}

impl Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// An entry in the Raft log.
#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct LogEntry<Cmd> {
    /// Command to apply to state machine once this log entry is considered committed.
    pub(crate) cmd: Cmd,
    /// Term in which this entry was received by leader.
    pub(crate) term: Term,
}

/// The Raft log. 1-indexed.
#[derive(Debug, PartialEq, Clone)]
pub struct Log<Cmd> {
    pub(crate) inner: Vec<LogEntry<Cmd>>,
}

impl<Cmd: Default> Default for Log<Cmd> {
    fn default() -> Self {
        Self {
            inner: vec![LogEntry {
                cmd: Cmd::default(),
                term: Term::default(),
            }],
        }
    }
}

impl<Cmd> Log<Cmd> {
    /// Get entry at given index, if any.
    fn get(&self, index: LogIndex) -> Option<&LogEntry<Cmd>> {
        let index: usize = (index.get() - 1)
            .try_into()
            .expect("arch should be compatible");
        self.inner.get(index)
    }

    /// Get all entries starting from the given entry, if any.
    fn get_from(&self, index: LogIndex) -> Option<&[LogEntry<Cmd>]> {
        let index: usize = (index.get() - 1)
            .try_into()
            .expect("arch should be compatible");
        self.inner.get(index..)
    }

    /// Replace all entries starting at given index with new ones.
    fn replace_from(&mut self, index: LogIndex, with: Box<[LogEntry<Cmd>]>) {
        let index: usize = (index.get() - 1)
            .try_into()
            .expect("arch should be compatible");
        self.inner.truncate(index);
        self.inner.extend(with);
    }

    fn last(&self) -> &LogEntry<Cmd> {
        self.inner
            .last()
            .expect("should always have at least one entry")
    }

    fn highest_index(&self) -> LogIndex {
        LogIndex::try_from(self.inner.len() as u64)
            .expect("should always have at least 1 log entry")
    }

    // take ownership but make it read-only
    fn append(&mut self, entries: Box<[LogEntry<Cmd>]>) {
        self.inner.extend(entries);
    }
}

/// Persistent node state.
///
/// Needs to be persisted to stable storage before responding to requests, and restored
/// from it on node boot.
#[derive(Debug, PartialEq)]
pub struct Persistent<Cmd> {
    pub(crate) current_term: Term,
    pub(crate) voted_for: Option<NodeID>,
    pub(crate) log: Log<Cmd>,
}

/// Error raised when persisting or restoring from stable storage.
#[derive(Debug)]
pub enum PersistenceError {
    Serialize(SerializeError),
    Parse(JSONError),
    Deserialize(DeserializeError),
    IoError(io::Error),
}

impl Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PersistenceError::Serialize(e) => {
                write!(f, "serialization error while persisting: {e}")
            }
            PersistenceError::Parse(e) => {
                write!(f, "JSON parsing error while restoring: {e}")
            }
            PersistenceError::Deserialize(e) => {
                write!(f, "deserialization error while restoring: {e}")
            }
            PersistenceError::IoError(e) => write!(f, "i/o error while persisting: {e}"),
        }
    }
}

impl Error for PersistenceError {}

impl From<SerializeError> for PersistenceError {
    fn from(value: SerializeError) -> Self {
        Self::Serialize(value)
    }
}

impl From<DeserializeError> for PersistenceError {
    fn from(value: DeserializeError) -> Self {
        Self::Deserialize(value)
    }
}

impl From<JSONError> for PersistenceError {
    fn from(value: JSONError) -> Self {
        Self::Parse(value)
    }
}

impl From<io::Error> for PersistenceError {
    fn from(value: io::Error) -> Self {
        Self::IoError(value)
    }
}

impl<Cmd: Serialize> Persistent<Cmd> {
    /// Serializes out to a writer.
    pub fn persist(&self, dest: &mut impl Write) -> Result<(), PersistenceError> {
        let s = self.serialize()?.to_string();
        dest.write_all(s.as_bytes())?;

        Ok(())
    }
}

impl<Cmd: Deserialize> Persistent<Cmd> {
    /// Restores from a reader. The reader is read until EOF.
    pub fn restore(src: &mut impl Read) -> Result<Self, PersistenceError> {
        let mut buf = Vec::with_capacity(1_024);
        src.read_to_end(&mut buf)?;
        let jval = json::parse(&buf)?;

        Ok(Self::deserialize(jval)?)
    }
}

/// Volatile, i.e. non-persistent node state. This state will be built up from
/// scratch on reboot.
#[derive(Debug)]
pub(crate) struct Volatile {
    /// Commit index in the log.
    commit_index: Option<LogIndex>,
    /// Log index of the last applied command.
    last_applied: Option<LogIndex>,

    /// This node's own ID.
    id: NodeID,
    /// IDs of all other nodes in the cluster.
    node_ids: Vec<NodeID>,
    /// Deadline at which we will step up as a candidate for a new term. Continuously
    /// looming, but pushed forward on events indicating we should wait before stepping
    /// up: for example, when we receive messages from a recognized leader.
    election_deadline: Instant,
}

impl Volatile {
    fn new(id: NodeID, node_ids: Vec<NodeID>) -> Self {
        assert!(!node_ids.is_empty(), "cannot operate without peer nodes");
        Self {
            commit_index: Default::default(),
            last_applied: Default::default(),
            id,
            node_ids,
            election_deadline: Instant::now(),
        }
    }
}

impl Volatile {
    /// Extends election deadline, with randomness to break candidacy live-locks.
    fn extend_election_deadline(&mut self) {
        let add = Duration::from_secs_f64(ELECTION_TIMEOUT.as_secs_f64() * (rand::rand() + 1.0));
        self.election_deadline = Instant::now() + add;

        eprintln!("extended election deadline by {add:?}");
    }
}

/// State specific to and only usable from a leadership position.
#[derive(Debug, Default)]
pub(crate) struct Leader {
    /// Node IDs mapped to the next log index to replicate to them.
    ///
    /// Does not include the leader node itself (we do not send logs to ourselves).
    next_indexes: HashMap<NodeID, LogIndex>,
    /// Node IDs mapped to the highest log entry known to be replicated to that node.
    ///
    /// Does not include the leader node itself.
    match_indexes: HashMap<NodeID, Option<LogIndex>>,
    /// Timestamp of last replication to *any* peer.
    last_replication: Option<Instant>,
}

#[derive(Debug)]
pub(crate) enum State<S: StateMachine> {
    Follower {
        p: Persistent<S::Command>,
        v: Volatile,
        /// Who we _believe_ the current leader is -- this can never be conclusive and
        /// can only ever be used for e.g. performance improvements (i.e. nothing
        /// related to correctness).
        leader: Option<NodeID>,
    },
    Candidate {
        p: Persistent<S::Command>,
        v: Volatile,
        /// The votes for us, for this term's candidacy.
        votes: HashSet<NodeID>,
    },
    Leader {
        p: Persistent<S::Command>,
        v: Volatile,
        l: Leader,
    },

    /// For internal ownership handling.
    ///
    /// We need ownership of e.g. leader state to potentially drop. Else we'd leak, if
    /// we transitioned away. We can't take ownership as that would make for awkward
    /// APIs; we generally do `&mut self`. As such, this variant enables a pattern like
    /// [`Option::take`].
    ///
    /// TODO: revisit if we can refactor enum into product type, such that `p` and `v`
    /// are common to all variants (which they are, cf. [`State::p`] etc.)
    Transitioning,
}

/// Basic implementations.
impl<S: StateMachine> State<S> {
    pub(super) fn new(id: NodeID, node_ids: Vec<NodeID>, p: Persistent<S::Command>) -> Self {
        Self::Follower {
            p,
            v: Volatile::new(id, node_ids),
            leader: None,
        }
    }

    fn v(&self) -> &Volatile {
        match self {
            State::Follower { v, .. } | State::Candidate { v, .. } | State::Leader { v, .. } => v,
            State::Transitioning => unreachable!("in transition"),
        }
    }

    fn v_mut(&mut self) -> &mut Volatile {
        match self {
            State::Follower { v, .. } | State::Candidate { v, .. } | State::Leader { v, .. } => v,
            State::Transitioning => unreachable!("in transition"),
        }
    }

    fn p(&self) -> &Persistent<S::Command> {
        match self {
            State::Follower { p, .. } | State::Candidate { p, .. } | State::Leader { p, .. } => p,
            State::Transitioning => unreachable!("in transition"),
        }
    }

    fn p_mut(&mut self) -> &mut Persistent<S::Command> {
        match self {
            State::Follower { p, .. } | State::Candidate { p, .. } | State::Leader { p, .. } => p,
            State::Transitioning => unreachable!("in transition"),
        }
    }
}

/// Implementations for transitions, see Figure 4 of <https://raft.github.io/raft.pdf>.
impl<S: StateMachine> State<S> {
    /// If the remote term is ahead, step down as we're behind.
    ///
    /// Use to fulfill:
    ///
    /// > If RPC request or response contains term T > currentTerm: set currentTerm = T,
    /// > convert to follower (§5.1)
    ///
    /// Note this applies to _any_ RPC request.
    pub(super) fn maybe_step_down(&mut self, remote: Term) {
        let p = self.p_mut();
        if remote > p.current_term {
            eprintln!("stepping down: {} < remote {}", p.current_term, remote);
            p.current_term.set(remote);
            p.voted_for = None; // In this new term, not voted for anyone yet
            self.become_follower();
        } else {
            eprintln!("not stepping down: {} >= remote {}", p.current_term, remote);
        }
    }

    /// Begin a new election if the election deadline has passed.
    ///
    /// > If a follower receives no communication over a period of time called the
    /// > election timeout, then it assumes there is no viable leader and begins an
    /// > election to choose a new leader.
    pub(super) fn maybe_begin_election(
        &mut self,
        outgoing: Sender<(NodeID, rpc::RaftMessage<S::Command>)>,
    ) {
        eprintln!("maybe beginning election");

        if !self.election_timeout_passed() {
            eprintln!("election deadline in the future, doing nothing");
            return;
        }

        if let State::Leader { v, .. } = self {
            v.extend_election_deadline()
        } else {
            self.become_candidate(outgoing)
        }
    }

    fn election_timeout_passed(&self) -> bool {
        Instant::now() > self.v().election_deadline
    }

    /// Become a candidate and start an election.
    fn become_candidate(&mut self, outgoing: Sender<(NodeID, rpc::RaftMessage<S::Command>)>) {
        assert!(self.election_timeout_passed());
        eprintln!("becoming candidate and beginning election");

        *self = if let Self::Follower { mut p, mut v, .. } | Self::Candidate { mut p, mut v, .. } =
            mem::replace(self, Self::Transitioning)
        {
            // Vote for ourselves. We're selfish like that.
            let this = v.id.clone();
            let votes = HashSet::from([this.clone()]);
            p.voted_for = Some(this);

            // Every new election is a new term.
            p.current_term.step();

            // Give time for election to proceed. Otherwise, will become candidate again
            // right away (ending this term prematurely).
            v.extend_election_deadline();

            Self::Candidate { p, v, votes }
        } else {
            unreachable!("invalid transition from {:?}", self)
        };

        eprintln!("became candidate for term {}", self.p().current_term);
        self.request_votes(outgoing);
    }

    pub(super) fn become_follower(&mut self) {
        // Allow a new candidate or leader to emerge before trying again for candidacy
        // ourselves right away.
        self.v_mut().extend_election_deadline();

        *self = match mem::replace(self, Self::Transitioning) {
            Self::Candidate { p, v, .. } | Self::Leader { p, v, l: _ } => {
                eprintln!("becoming follower for term {}", p.current_term);
                Self::Follower { p, v, leader: None }
            }
            Self::Follower { p, v, .. } => {
                eprintln!("remaining follower for term {}", p.current_term);
                Self::Follower { p, v, leader: None }
            }
            Self::Transitioning => unreachable!("in transition"),
        };
    }

    fn become_leader(&mut self, outgoing: Sender<(NodeID, rpc::RaftMessage<S::Command>)>) {
        *self = if let Self::Candidate { p, v, .. } = mem::replace(self, Self::Transitioning) {
            Self::Leader {
                l: Leader {
                    // > When a leader first comes to power, it initializes all
                    // > nextIndex values to the index just after the last one in its
                    // > log
                    next_indexes: v
                        .node_ids
                        .iter()
                        .map(|id| {
                            assert_ne!(id, &v.id);
                            (
                                id.clone(),
                                LogIndex::try_from(p.log.highest_index().get() + 1)
                                    .expect("always at least 1"),
                            )
                        })
                        .collect(),
                    match_indexes: v.node_ids.iter().map(|id| (id.clone(), None)).collect(),
                    last_replication: None,
                },
                p,
                v,
            }
        } else {
            unreachable!("need to be candidate to become leader: {:?}", self)
        };

        eprintln!("became leader for term {}", self.p().current_term);

        // > Upon election: send initial empty AppendEntries RPCs (heartbeat) to each
        // > server; repeat during idle periods to prevent election timeouts (§5.2)
        //
        // This helps voters get feedback ASAP.
        self.replicate_log(outgoing);
    }

    /// Request votes from *all* other nodes.
    fn request_votes(&self, outgoing: Sender<(NodeID, rpc::RaftMessage<S::Command>)>) {
        eprintln!("requesting votes for term {}", self.p().current_term);

        for node in &self.v().node_ids {
            outgoing
                .send((
                    node.clone(),
                    rpc::RaftMessage::RequestVote {
                        candidate_term: self.p().current_term,
                        last_log_index: LogIndex::try_from(self.p().log.highest_index())
                            .expect("should always have at least 1 log entry"),
                        last_log_term: self.p().log.last().term,
                    },
                ))
                .expect("receiver should never hang up");
        }
    }
}

/// Implementations for handling requests. Note how client requests were turned into
/// plain log commands in a higher application layer, so at this stage all we see is
/// pure log commands.
impl<S: StateMachine> State<S> {
    /// Handle a request for a leadership vote from some remote candidate.
    #[must_use]
    pub(super) fn handle_vote_request(
        &mut self,
        candidate_id: NodeID,
        candidate_term: Term,
        candidate_last_log_index: LogIndex,
        candidate_last_log_term: Term,
    ) -> rpc::RaftMessage<S::Command> {
        eprintln!(
            "handling vote request for {} on term {}",
            candidate_id, candidate_term
        );
        assert_ne!(
            self.v().id,
            candidate_id,
            "should never be requested to vote for self"
        );

        let Self::Follower { p, v, .. } = self else {
            eprintln!("rejecting vote: am candidate or leader already");
            return rpc::RaftMessage::RequestVoteResponse {
                term: self.p().current_term,
                vote_granted: false,
            };
        };

        let vote_granted = {
            if candidate_term < p.current_term {
                // Candidate is in the logical past.
                false
            } else {
                match &p.voted_for {
                    Some(voted_for) if candidate_id != *voted_for => {
                        // We already voted for a different candidate in this
                        // term.
                        false
                    }
                    Some(_) | None => {
                        // We have not voted yet in this term, or voted for this
                        // candidate already. We will gladly cast our vote again
                        // (to support that candidate retrying), under certain
                        // conditions.

                        let t = p.log.last().term;
                        match candidate_last_log_term.cmp(&t) {
                            Ordering::Less => {
                                eprintln!(
                                    "got candidate logs from past term {} < {}",
                                    candidate_last_log_term, t
                                );
                                false
                            }
                            Ordering::Equal => {
                                let longer = candidate_last_log_index >= p.log.highest_index();
                                eprintln!(
                                    "got candidate logs from same term {}, candidate log is >=? {}",
                                    t, longer
                                );

                                longer
                            }
                            Ordering::Greater => true, // they are ahead
                        }
                    }
                }
            }
        };

        if vote_granted {
            // Register new state *before* replying
            p.voted_for = Some(candidate_id.clone());
            // Allow time for voting session to conclude; without us running for
            // candidacy prematurely (starting a new term, invalidating our own
            // vote in this term).
            v.extend_election_deadline();
        }

        eprintln!("granting vote to {}: {}", candidate_id, vote_granted);
        rpc::RaftMessage::RequestVoteResponse {
            term: p.current_term,
            vote_granted,
        }
    }

    /// Handle a response to a previous vote request we sent.
    ///
    /// Note these responses can be arbitrarily delayed and correspondingly malformed.
    pub(super) fn handle_vote_response(
        &mut self,
        peer: NodeID,
        remote_term: Term,
        vote_granted: bool,
        outgoing: Sender<(NodeID, rpc::RaftMessage<S::Command>)>,
    ) {
        eprintln!(
            "handling vote response from {} on term {}, vote granted: {}",
            peer, remote_term, vote_granted
        );

        if !vote_granted {
            return; // Too bad
        }

        if let Self::Candidate { p, v, votes } = self {
            assert!(vote_granted);
            assert!(votes.contains(&v.id), "should always vote for ourselves");

            match p.current_term.cmp(&remote_term) {
                Ordering::Less => {
                    eprintln!("ignoring vote: term {} < {}", remote_term, p.current_term);
                }
                Ordering::Equal => {
                    eprintln!("accepting favorable vote from {}", peer);
                    votes.insert(peer);
                    eprintln!("have {} votes: {:?}", votes.len(), votes);

                    let votes_received = votes.len();
                    if self.won_election(votes_received) {
                        self.become_leader(outgoing);
                    }
                }
                Ordering::Greater => {
                    unreachable!("should have stepped down from candidacy beforehand")
                }
            }
        } else {
            assert!(!matches!(self, Self::Transitioning));
            // For example, because we _just became_ a leader by majority vote, and
            // further peers' favorable votes arrived after we already stepped up.
            eprintln!("ignoring vote: not currently a candidate (anymore)");
        };
    }

    #[must_use]
    fn won_election(&self, votes_received: usize) -> bool {
        let cluster_size = self.v().node_ids.len() + 1; // Cluster + ourselves
        let n_majority = cluster_size / 2 + 1;
        votes_received >= n_majority
    }

    /// Handle a request to append entries to our log, sent by leaders.
    ///
    /// If all checks pass, the request is accepted and this node's state machine
    /// progressed to the next indicated commit index.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn handle_append_entries(
        &mut self,
        peer: NodeID,
        leader_term: Term,
        leader_commit_index: Option<LogIndex>,
        leader_prev_log_index: LogIndex,
        leader_prev_log_term: Term,
        entries: Log<S::Command>,
        machine: &mut S,
    ) -> rpc::RaftMessage<S::Command> {
        eprintln!(
            "handling append entries for leader term {}, commit {:?}, p. log idx {}, p. log term {}",
            leader_term, leader_commit_index, leader_prev_log_index, leader_prev_log_term
        );

        let current_term = self.p().current_term;

        let failure_msg = rpc::RaftMessage::AppendEntriesResponse {
            current_term,
            index: self.p().log.highest_index(),
            success: false,
        };

        if leader_term < current_term {
            // Leader is in the logical past, let it know.
            return failure_msg;
        }

        // This is a valid leader. Don't compete with it.
        self.v_mut().extend_election_deadline();

        if let Self::Candidate { .. } = self {
            // Receiving AppendEntries _in a term we are currently candidating for_ must
            // mean another peer established themselves as leader. Cancel our candidacy
            // and get in line.
            self.become_follower();
        }

        assert_eq!(
            leader_term, current_term,
            "should have stepped down already if remote were greater"
        );
        if let Self::Follower { v, leader, .. } = self {
            // Invariant, as we might use this to proxy client requests to the leader;
            // if it's ourselves that's an infinite loop.
            assert_ne!(v.id, peer, "current leader can never be self");

            *leader = Some(peer); // Recognize this leader
        } else {
            // Raft invariant: if we were a candidate we just stepped down. We also
            // confirmed same term as our peer. As there can only be at most one leader
            // per term and only leaders send AppendEntries, receiving one means we
            // can't possibly be a leader (= 2 leaders in same term => bug in election
            // implementation).
            unreachable!("in same term, if no longer candidate, can only be follower");
        };

        match self.p().log.get(leader_prev_log_index) {
            Some(entry) if entry.term != leader_prev_log_term => {
                // We have an entry at the _index_ but its _term_ does not match.
                return failure_msg;
            }
            None => {
                // We have no entry at all at that index; terms can never match.
                return failure_msg;
            }
            Some(entry) => assert_eq!(entry.term, leader_prev_log_term),
        };

        // Consistency check with induction step OK.

        self.p_mut().log.replace_from(
            leader_prev_log_index
                .checked_add(1)
                .expect("log length should never exceed range"),
            entries.inner.into_boxed_slice(),
        );

        let new_index = self.p().log.highest_index();
        let v = self.v_mut();
        if let Some(leader_commit_index) = leader_commit_index
            && leader_commit_index > v.commit_index.unwrap_or(LogIndex::new(1).expect("1 > 0"))
        {
            // Leader sent a commit index and ours is lower: advance.
            v.commit_index = Some(cmp::min(leader_commit_index, new_index));
            self.advance_state_machine(machine);
        }

        rpc::RaftMessage::AppendEntriesResponse {
            current_term,
            index: new_index,
            success: true,
        }
    }

    /// Handle a response to a previous AppendEntries request.
    ///
    /// If not successful, log replication is immediately retried via the outgoing
    /// channel. If successful, we advance the state machine.
    pub(super) fn handle_append_entries_response(
        &mut self,
        peer: NodeID,
        success: bool,
        index: LogIndex,
        outgoing: Sender<(NodeID, rpc::RaftMessage<S::Command>)>,
        machine: &mut S,
    ) {
        let Self::Leader {
            l:
                Leader {
                    next_indexes,
                    match_indexes,
                    ..
                },
            ..
        } = self
        else {
            eprintln!("ignoring append entries response from {peer}: no longer a leader");
            return;
        };

        let Some(next_index) = next_indexes.get_mut(&peer) else {
            eprintln!("ignoring append entries response from {peer}: no tracking next index entry");
            return;
        };

        if !success {
            eprintln!("append entries: unsuccessful, decrementing {next_index} of {peer}");

            // Decrement, floor it to minimum permissible index.
            *next_index = LogIndex::try_from(next_index.get() - 1)
                .unwrap_or(LogIndex::new(1).expect("1 > 0"));

            // Retry right away with new decremented value. This can be called anytime,
            // it internally ensures we don't replicate too often.
            self.replicate_log(outgoing);

            return;
        }

        let Some(match_index) = match_indexes.get_mut(&peer) else {
            eprintln!(
                "ignoring append entries response from {peer}: no tracking match index entry"
            );
            return;
        };

        eprintln!("append entries: successful, indexes to {index} for {peer}");
        // Peers send their new _current_ index. Note, this means indexes can go
        // _backwards_ if an outdated response arrives late. That is safe, we will
        // re-replicate from an earlier state.
        *next_index = index.checked_add(1).expect("should never exceed log size");
        *match_index = Some(index);
        self.advance_commit_index(machine);
    }
}

/// Implementations relevant to log handling (e.g. replication), mostly relevant while
/// holding leadership.
impl<S: StateMachine> State<S> {
    /// During leadership, replicate local log to all relevant peers, potentially
    /// sending an empty replication request for heartbeat.
    pub(super) fn replicate_log(
        &mut self,
        outgoing: Sender<(NodeID, rpc::RaftMessage<S::Command>)>,
    ) {
        let State::Leader {
            p,
            v,
            l:
                Leader {
                    next_indexes: next_index,
                    last_replication,
                    ..
                },
        } = self
        else {
            eprintln!("replicate log: not a leader");
            return;
        };

        let heartbeat_needed = if let Some(lr) = last_replication {
            let elapsed = Instant::now() - *lr;
            if elapsed < MIN_REPLICATION_INTERVAL {
                eprintln!("replicate log: last replication too recently, skipping");
                return;
            }

            let v = elapsed > HEARTBEAT_INTERVAL;
            eprintln!("replicate log: regular heartbeat needed: {}", v);
            v
        } else {
            eprintln!("replicate log: initial heartbeat needed");
            true
        };

        for node_id in &v.node_ids {
            let next_index_for_node = next_index
                .get(node_id)
                .expect("should have entry for all known cluster nodes");
            assert!(
                next_index_for_node.get() > 1,
                "nodes all have the same entry for index 1 already"
            );

            let prev_log_index =
                LogIndex::try_from(next_index_for_node.get() - 1).expect("checked above");
            let prev_log_term = p
                .log
                .get(prev_log_index)
                .expect("should always have a preceding entry")
                .term;

            let entries = if let Some(entries) = p.log.get_from(*next_index_for_node) {
                entries.to_vec() // Note, might be empty
            } else {
                vec![]
            };
            let n_entries = entries.len();

            let msg = rpc::RaftMessage::AppendEntries {
                term: p.current_term,
                prev_log_index,
                prev_log_term,
                entries: Log {
                    inner: { entries.clone() },
                },
                commit_index: v.commit_index,
            };

            if n_entries == 0 && !heartbeat_needed {
                eprintln!("replicate log: no entries and no heartbeat needed: skipping");
                continue;
            }

            eprintln!(
                "replicate log: local size {}, sending {} entries to {}: {:?}",
                p.log.highest_index(),
                n_entries,
                node_id,
                entries
            );

            outgoing
                .send((node_id.clone(), msg))
                .expect("raft receiver should never hang up");

            *last_replication = Some(Instant::now());
        }
    }

    pub(super) fn append(&mut self, cmd: S::Command) {
        let term = self.p().current_term;
        self.p_mut()
            .log
            .append(vec![LogEntry { cmd, term }].into_boxed_slice());
    }

    /// During leadership, advance the commit index if to the highest possible watermark
    /// among all peers.
    ///
    /// > If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥
    /// > N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)
    fn advance_commit_index(&mut self, machine: &mut S) {
        let Self::Leader { v, p, l } = self else {
            unreachable!("need to be leader to advance commit index");
        };

        // We might not have match indexes for _all_ peers yet. If at least one is
        // missing, abort.
        let match_index: Option<Vec<_>> = l.match_indexes.values().cloned().collect();
        let Some(mut match_index) = match_index else {
            eprintln!("advance commit index: skipping: some peers without match index");
            return;
        };

        let n = *lower_median(&mut match_index)
            .expect("all peers have match index recorded, and there is at least one peer");

        let Some(entry) = p.log.get(n) else {
            eprintln!("advance commit index: skipping: no log entry at {n}");
            return;
        };

        // TODO: remove hack with 0-term
        if entry.term != p.current_term && p.current_term > Term(0) {
            // TODO: explain why
            eprintln!("advance commit index: log entry {n} not in current term");
            return;
        }

        let commit_index = v.commit_index;
        v.commit_index = cmp::max(commit_index, Some(n));
        eprintln!("advance commit index: now {:?}", v.commit_index);

        self.advance_state_machine(machine);
    }

    /// Applies all outstanding log entries below the commit index to the state machine.
    fn advance_state_machine(&mut self, machine: &mut S) {
        let (Self::Candidate { p, v, .. }
        | Self::Follower { p, v, .. }
        | Self::Leader { p, v, .. }) = self
        else {
            unreachable!("invalid: in transition");
        };

        // Also excludes Some(last_applied) while commit index is None, which would be
        // an invariant violation. See also
        // https://doc.rust-lang.org/std/option/enum.Option.html#impl-Ord-for-Option%3CT%3E.
        assert!(v.last_applied <= v.commit_index, "can never overtake");

        let mut n = 1;
        while v.last_applied < v.commit_index {
            // Increment
            v.last_applied = Some(
                v.last_applied
                    // On first application
                    .unwrap_or(LogIndex::new(1).expect("1 > 0"))
                    .checked_add(1)
                    .expect("commit index is valid and greater, this must be valid"),
            );

            let log_entry = p
                .log
                .get(v.last_applied.expect("always Some at this point"))
                .expect("entries below commit index must exist (replicated)");

            machine.apply(log_entry.cmd.clone());
            n += 1;
        }

        eprintln!("advanced state machine {n} steps: {:?}", machine);
    }

    #[must_use]
    pub(super) fn is_leader(&self) -> bool {
        matches!(self, Self::Leader { .. })
    }

    #[must_use]
    pub(super) fn current_leader(&self) -> Option<&NodeID> {
        if let Self::Follower { leader, .. } = self {
            leader.as_ref()
        } else {
            None
        }
    }
}

/// Compute median of slice, picking the lower/left value sort even-sized inputs.
fn lower_median<T: Ord>(items: &mut [T]) -> Option<&T> {
    if items.is_empty() {
        return None;
    }

    let mid = items.len().saturating_sub(1) / 2;
    let (_, median, _) = items.select_nth_unstable(mid);
    Some(median)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use json::Value as JSONValue;
    use json::serde::Deserialize;

    use super::*;

    #[derive(Debug, PartialEq)]
    struct TestCommand(String);

    impl Serialize for TestCommand {
        fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
            self.0.serialize()
        }
    }

    impl Deserialize for TestCommand {
        fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
            let s: String = Deserialize::deserialize(value)?;
            Ok(Self(s))
        }
    }

    type TestResult<T> = Result<T, Box<dyn Error>>;

    #[test]
    fn test_persistent_serialize_deserialize_roundtrip() -> TestResult<()> {
        let there = Persistent {
            current_term: Term(3),
            voted_for: Some("foo".into()),
            log: Log {
                inner: vec![
                    LogEntry {
                        cmd: TestCommand("foo".into()),
                        term: Term(1),
                    },
                    LogEntry {
                        cmd: TestCommand("bar".into()),
                        term: Term(7),
                    },
                ],
            },
        };

        // In-memory values only
        {
            let jval = there.serialize()?;
            let back: Persistent<TestCommand> = Deserialize::deserialize(jval)?;

            assert_eq!(there, back);
        }

        // Actual serialization to a "file"
        {
            let mut file = Vec::new(); // in-memory "file"
            there.persist(&mut file)?;

            let back = Persistent::<TestCommand>::restore(&mut Cursor::new(file.clone()))?;

            assert_eq!(
                there,
                back,
                "could not read back file with contents: {}",
                String::from_utf8_lossy(&file)
            );
        }

        Ok(())
    }

    #[test]
    fn test_median() {
        for (mut items, expected) in [
            (vec![], None),
            (vec![1], Some(&1)),
            (vec![1, 2], Some(&1)),
            (vec![1, 2, 3], Some(&2)),
            (vec![1, 2, 3, 4], Some(&2)),
            (vec![1, 2, 3, 4, 5], Some(&3)),
        ] {
            assert_eq!(lower_median(&mut items), expected);
        }
    }
}
