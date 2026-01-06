use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{self, Display};
use std::io::{self, Read, Write};
use std::mem;
use std::num::NonZero;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use json::error::Error as JSONError;
use json::serde::{Deserialize, DeserializeError, Serialize, SerializeError};

use crate::{ELECTION_TIMEOUT, HEARTBEAT_INTERVAL, MIN_REPLICATION_INTERVAL, NodeID, rpc};

#[derive(Debug, Clone, Copy, Default, PartialEq, PartialOrd, Eq, Ord)]
pub struct Term(pub(super) u64);

impl Term {
    fn advance(&mut self, to: Term) {
        assert!(to > *self, "need to advance forward");

        *self = to;
    }

    fn step(&mut self) {
        self.advance(Self(self.0 + 1));
    }
}

impl Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub trait StateMachine: Default + Send {
    type Command;
    type Request;
    type Response;

    fn apply(&mut self, cmd: Self::Command);
    fn respond(&self, req: Self::Request) -> Option<Self::Response>;
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct LogEntry<C> {
    pub(crate) cmd: C,
    pub(crate) term: Term,
}

impl<C: Display> Display for LogEntry<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.cmd.to_string().escape_default())?;
        write!(f, "{}", self.term)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Log<C> {
    pub(crate) inner: Vec<LogEntry<C>>,
}

impl<C: Default> Default for Log<C> {
    fn default() -> Self {
        Self {
            inner: vec![LogEntry {
                cmd: C::default(),
                term: Term::default(),
            }],
        }
    }
}

impl<C> Log<C> {
    fn get(&self, index: NonZero<usize>) -> Option<&LogEntry<C>> {
        self.inner.get(index.get() - 1)
    }

    fn get_from(&self, index: NonZero<usize>) -> Option<&[LogEntry<C>]> {
        self.inner.get(index.get()..)
    }

    fn last(&self) -> &LogEntry<C> {
        self.inner
            .last()
            .expect("should always have at least one entry")
    }

    fn size(&self) -> usize {
        self.inner.len()
    }

    // take ownership but make it read-only
    fn append(&mut self, entries: Box<[LogEntry<C>]>) {
        self.inner.extend(entries);
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct Persistent<C> {
    pub(crate) current_term: Term,
    pub(crate) voted_for: Option<NodeID>,
    pub(crate) log: Log<C>,
}

impl<C: Display> Display for Persistent<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.current_term)?;
        if let Some(ref c) = self.voted_for {
            write!(f, "{c}")?;
        }
        writeln!(f)?;

        Ok(())
    }
}

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

impl<C: Serialize> Persistent<C> {
    /// Serializes out to a writer.
    pub fn persist(&self, dest: &mut impl Write) -> Result<(), PersistenceError> {
        let s = self.serialize()?.to_string();
        dest.write_all(s.as_bytes())?;

        Ok(())
    }
}

impl<C: Deserialize> Persistent<C> {
    /// Restores from a reader. The reader is read until EOF.
    pub fn restore(src: &mut impl Read) -> Result<Self, PersistenceError> {
        let mut buf = Vec::with_capacity(32);
        src.read_to_end(&mut buf)?;
        let jval = json::parse(&buf)?;

        Ok(Self::deserialize(jval)?)
    }
}

#[derive(Debug)]
pub(crate) struct Volatile<S: StateMachine> {
    commit_index: usize,
    #[expect(unused)]
    last_applied: usize,

    /// This node's own ID.
    id: NodeID,
    /// IDs of all other nodes in the cluster.
    cluster_ids: Vec<NodeID>,
    state_machine: S,
    election_deadline: Instant,
}

impl<S: StateMachine> Volatile<S> {
    fn new(id: NodeID, cluster_ids: Vec<NodeID>) -> Self {
        Self {
            commit_index: Default::default(),
            last_applied: Default::default(),
            id,
            cluster_ids,
            state_machine: Default::default(),
            election_deadline: Instant::now(),
        }
    }
}

impl<S: StateMachine> Volatile<S> {
    #[expect(unused)]
    fn reset(&mut self) {
        // This way we go through existing constructor and cannot forget any fields.
        *self = Self::new(mem::take(&mut self.id), mem::take(&mut self.cluster_ids))
    }

    fn extend_election_deadline(&mut self) {
        let add = Duration::from_secs_f64(ELECTION_TIMEOUT.as_secs_f64() * (rand::rand() + 1.0));
        self.election_deadline = Instant::now() + add;

        eprintln!("extended election deadline by {add:?}");
    }
}

#[derive(Debug, Default)]
pub(crate) struct Leader {
    pub(crate) next_index: HashMap<NodeID, NonZero<usize>>,
    #[expect(unused)] // TODO
    pub(crate) match_index: HashMap<NodeID, Option<NonZero<usize>>>,
    pub(crate) last_replication: Option<Instant>,
}

#[derive(Debug)]
pub(crate) enum State<C, S: StateMachine> {
    Follower {
        p: Persistent<C>,
        v: Volatile<S>,
    },
    Candidate {
        p: Persistent<C>,
        v: Volatile<S>,
        votes: HashSet<NodeID>,
    },
    Leader {
        p: Persistent<C>,
        v: Volatile<S>,
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

/// For transitions, see Figure 4 of <https://raft.github.io/raft.pdf>.
impl<C, S: StateMachine> State<C, S> {
    pub(super) fn new(id: NodeID, cluster_ids: Vec<NodeID>, p: Persistent<C>) -> Self {
        Self::Follower {
            p,
            v: Volatile::new(id, cluster_ids),
        }
    }

    /// Begin a new election if the election deadline has passed.
    ///
    /// > If a follower receives no communication over a period of time called the
    /// > election timeout, then it assumes there is no viable leader and begins an
    /// > election to choose a new leader.
    pub(super) fn maybe_begin_election(&mut self, outgoing: Sender<(NodeID, rpc::RaftMessage<C>)>) {
        eprintln!("maybe beginning election");

        if !self.election_timeout_passed() {
            eprintln!("election deadline in the future, doing nothing");
            return;
        }

        match self {
            State::Leader { v, .. } => v.extend_election_deadline(),
            State::Follower { .. } | State::Candidate { .. } => self.become_candidate(outgoing),
            State::Transitioning => unreachable!("in transition"),
        }
    }

    fn election_timeout_passed(&self) -> bool {
        Instant::now() > self.v().election_deadline
    }

    /// Become a candidate and start an election.
    fn become_candidate(&mut self, outgoing: Sender<(NodeID, rpc::RaftMessage<C>)>) {
        assert!(self.election_timeout_passed());
        eprintln!("becoming candidate and beginning election");

        match mem::replace(self, Self::Transitioning) {
            // Note, candidates also start new elections if election timeout passed.
            Self::Follower { p, v, .. } | Self::Candidate { p, v, .. } => {
                // Vote for ourselves. We're selfish like that.
                let this = v.id.clone();
                let votes = HashSet::from([this.clone()]);
                *self = Self::Candidate { p, v, votes };

                self.p_mut().current_term.step();
                self.p_mut().voted_for = Some(this);

                // Give time for election to proceed.
                self.v_mut().extend_election_deadline();

                self.request_votes(outgoing);

                eprintln!("became candidate for term {}", self.p().current_term);
            }
            State::Leader { .. } => {
                unreachable!("invalid transition (leader steps down to follower first)")
            }
            Self::Transitioning => unreachable!("in transition"),
        };
    }

    pub(crate) fn become_follower(&mut self) {
        self.v_mut().extend_election_deadline();
        *self = match mem::replace(self, Self::Transitioning) {
            Self::Candidate { p, v, .. } | Self::Leader { p, v, l: _ } => {
                eprintln!("becoming follower for term {}", p.current_term);
                Self::Follower { p, v }
            }
            Self::Follower { p, v, .. } => {
                eprintln!("remaining follower for term {}", p.current_term);
                Self::Follower { p, v }
            }
            Self::Transitioning => unreachable!("in transition"),
        };
    }

    pub(crate) fn become_leader(&mut self) {
        match mem::replace(self, Self::Transitioning) {
            Self::Candidate { p, v, .. } => {
                *self = Self::Leader {
                    l: Leader {
                        // "When a leader first comes to power, it initializes all
                        // nextIndex values to the index just after the last one in its
                        // log"
                        next_index: v
                            .cluster_ids
                            .iter()
                            .map(|id| {
                                (
                                    id.clone(),
                                    NonZero::try_from(p.log.size() + 1).expect("always at least 1"),
                                )
                            })
                            .collect(),
                        match_index: v.cluster_ids.iter().map(|id| (id.clone(), None)).collect(),
                        last_replication: None,
                    },
                    p,
                    v,
                };

                eprintln!("became leader for term {}", self.p().current_term);
            }
            Self::Follower { .. } | Self::Leader { .. } => {
                unreachable!("need to be candidate to become leader")
            }
            Self::Transitioning => unreachable!("in transition"),
        };
    }

    /// Request votes from *all* other nodes.
    pub(crate) fn request_votes(&self, outgoing: Sender<(NodeID, rpc::RaftMessage<C>)>) {
        eprintln!("requesting votes for term {}", self.p().current_term);
        for node in &self.v().cluster_ids {
            outgoing
                .send((
                    node.clone(),
                    rpc::RaftMessage::RequestVote {
                        // candidate_id: self.v().id.clone(),
                        candidate_term: self.p().current_term,
                        last_log_index: self
                            .p()
                            .log
                            .size()
                            .try_into()
                            .expect("should never exceed sendable log size"),
                        last_log_term: self.p().log.last().term,
                    },
                ))
                .expect("receiver should never hang up");
        }
    }

    /// Handle a request for a leadership vote from some remote candidate.
    pub(crate) fn handle_vote_request(
        &mut self,
        candidate_id: NodeID,
        candidate_term: Term,
        candidate_last_log_index: u64,
        candidate_last_log_term: Term,
    ) -> rpc::RaftMessage<C> {
        eprintln!(
            "handling vote request for {} on term {} ({} / {})",
            candidate_id, candidate_term, candidate_last_log_index, candidate_last_log_term
        );
        assert_ne!(
            self.v().id,
            candidate_id,
            "should never be requested to vote for self"
        );

        match self {
            Self::Follower { p, v, .. } => {
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
                                        let longer =
                                            candidate_last_log_index >= p.log.size() as u64;
                                        eprintln!(
                                            "got candidate logs from same term {}, candidate log is longer: {}",
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
                    v.extend_election_deadline();
                }

                eprintln!("granting vote to {}: {}", candidate_id, vote_granted);
                rpc::RaftMessage::RequestVoteResponse {
                    // remote_id: candidate_id.clone(),
                    term: p.current_term,
                    vote_granted,
                }
            }
            Self::Candidate { p, .. } | Self::Leader { p, .. } => {
                eprintln!("rejecting vote: am candidate or leader already");
                rpc::RaftMessage::RequestVoteResponse {
                    // remote_id: candidate_id,
                    term: p.current_term,
                    vote_granted: false,
                }
            }
            Self::Transitioning => unreachable!("in transition"),
        }
    }

    /// Handle a response to a previous vote request we sent.
    ///
    /// Note these responses can be arbitrarily delayed and correspondingly malformed.
    pub(crate) fn handle_vote_response(
        &mut self,
        peer: NodeID,
        remote_term: Term,
        vote_granted: bool,
    ) {
        eprintln!(
            "handling vote response from {} on term {}, vote granted: {}",
            peer, remote_term, vote_granted
        );

        if !vote_granted {
            // Too bad
            return;
        }

        match self {
            Self::Candidate { p, v, votes } => {
                assert!(vote_granted);
                assert!(votes.contains(&v.id), "should always voted for ourselves");

                if remote_term < p.current_term {
                    eprintln!("ignoring vote: term {} < {}", remote_term, p.current_term);
                    return;
                }

                votes.insert(peer);
                eprintln!("have {} votes: {:?}", votes.len(), votes);

                let votes_received = votes.len();
                if self.won_election(votes_received) {
                    self.become_leader();
                }
            }
            _ => {
                // For example, because we _just became_ a leader.
                eprintln!("ignoring vote: not currently a candidate (anymore)");
            }
        };
    }

    fn won_election(&self, votes_received: usize) -> bool {
        let cluster_size = self.v().cluster_ids.len() + 1; // Cluster + ourselves
        let n_majority = cluster_size / 2 + 1;
        votes_received >= n_majority
    }

    /// If the remote term is ahead, step down as we're behind.
    pub(crate) fn maybe_step_down(&mut self, remote: Term) {
        let p = self.p_mut();
        if p.current_term < remote {
            eprintln!("stepping down: {} < remote {}", p.current_term, remote);
            p.current_term.advance(remote);
            p.voted_for = None; // In this new term, not voted for anyone yet
            self.become_follower();
        } else {
            eprintln!("not stepping down: {} >= remote {}", p.current_term, remote);
        }
    }

    pub(crate) fn replicate_log(&mut self, outgoing: Sender<(NodeID, rpc::RaftMessage<C>)>)
    where
        C: Clone,
    {
        let State::Leader {
            p,
            v,
            l:
                Leader {
                    next_index,
                    match_index: _,
                    last_replication,
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

        let mut replicated = false;
        for id in &v.cluster_ids {
            let next_index = next_index
                .get(id)
                .expect("should have entry for all known cluster nodes");
            assert!(next_index.get() >= 2, "nodes always have index 1 already");

            let prev_log_index = (next_index.get() - 1) as u64;
            let prev_log_term = p
                .log
                .get(NonZero::try_from(next_index.get() - 1).expect("at least 1"))
                .expect("should always have a preceding entry")
                .term;

            let entries = if let Some(entries) = p.log.get_from(*next_index) {
                entries.to_vec() // Note, might be empty
            } else {
                vec![]
            };
            let n_entries = entries.len();

            let msg = rpc::RaftMessage::AppendEntries {
                leader_term: p.current_term,
                // leader_id: v.id.clone(),
                prev_log_index,
                prev_log_term,
                entries: Log { inner: { entries } },
                leader_commit: v.commit_index as u64,
            };

            if n_entries == 0 && !heartbeat_needed {
                eprintln!("replicate log: no entries and no heartbeat needed: skipping");
                continue;
            }

            eprintln!("replicate log: sending {} entries to {}", n_entries, id);
            outgoing
                .send((id.clone(), msg))
                .expect("raft receiver should never hang up");
            replicated = true;
        }

        if replicated {
            *last_replication = Some(Instant::now());
        }
    }

    #[expect(unused)]
    pub(crate) fn append(&mut self, cmd: C) {
        let term = self.p().current_term;
        self.p_mut()
            .log
            .append(vec![LogEntry { cmd, term }].into_boxed_slice());
    }

    pub(crate) fn is_leader(&self) -> bool {
        matches!(self, Self::Leader { .. })
    }

    pub(crate) fn respond(
        &self,
        req: <S as StateMachine>::Request,
    ) -> Option<<S as StateMachine>::Response> {
        self.v().state_machine.respond(req)
    }

    pub(crate) fn v(&self) -> &Volatile<S> {
        match self {
            State::Follower { v, .. } | State::Candidate { v, .. } | State::Leader { v, .. } => v,
            State::Transitioning => unreachable!("in transition"),
        }
    }

    pub(crate) fn v_mut(&mut self) -> &mut Volatile<S> {
        match self {
            State::Follower { v, .. } | State::Candidate { v, .. } | State::Leader { v, .. } => v,
            State::Transitioning => unreachable!("in transition"),
        }
    }

    pub(crate) fn p(&self) -> &Persistent<C> {
        match self {
            State::Follower { p, .. } | State::Candidate { p, .. } | State::Leader { p, .. } => p,
            State::Transitioning => unreachable!("in transition"),
        }
    }

    pub(crate) fn p_mut(&mut self) -> &mut Persistent<C> {
        match self {
            State::Follower { p, .. } | State::Candidate { p, .. } | State::Leader { p, .. } => p,
            State::Transitioning => unreachable!("in transition"),
        }
    }
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
}
