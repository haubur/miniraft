use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{self, Display};
use std::io::{self, Read, Write};
use std::mem;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use json::error::Error as JSONError;
use json::serde::{Deserialize, DeserializeError, Serialize, SerializeError};

use crate::{ELECTION_TIMEOUT, NodeID, rpc};

#[derive(Debug, Default, PartialEq)]
pub struct CandidateID(pub u64);

impl Display for CandidateID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, PartialOrd)]
pub struct Term(pub(crate) u64);

impl Term {
    pub fn advance(&mut self, remote: Term) {
        if *self > remote {
            unreachable!("should check before advancing to incompatible term")
        }

        *self = remote;
    }

    pub fn step(&mut self) {
        self.0 += 1;
    }
}

impl Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct LogEntry<C> {
    pub cmd: C,
    pub term: Term,
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
    pub fn get(&self, index: usize) -> Option<&LogEntry<C>> {
        // Log is 1-indexed
        if index == 0 {
            None
        } else {
            self.inner.get(index - 1)
        }
    }

    pub fn last(&self) -> &LogEntry<C> {
        self.inner
            .last()
            .expect("should always have at least one entry")
    }

    pub fn size(&self) -> usize {
        self.inner.len()
    }

    // take ownership but make it read-only
    pub fn append(&mut self, entries: Box<[LogEntry<C>]>) {
        self.inner.extend(entries);
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct Persistent<C> {
    pub current_term: Term,
    pub voted_for: Option<CandidateID>,
    pub log: Log<C>,
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
pub(crate) struct VotingSession {
    start: Instant,
    term: Term,
    votes: HashSet<NodeID>,
}

#[derive(Debug)]
pub(crate) struct Volatile {
    #[expect(unused)]
    commit_index: usize,
    #[expect(unused)]
    last_applied: usize,

    // Internal bookkeeping:
    /// This node's own ID.
    id: NodeID,
    /// The current voting session, if any.
    election_deadline: Instant,
}

impl Volatile {
    fn new(id: NodeID) -> Self {
        Self {
            commit_index: Default::default(),
            last_applied: Default::default(),
            id,
            election_deadline: Instant::now(),
        }
    }
}

impl Volatile {
    #[expect(unused)]
    fn reset(&mut self) {
        // This way we go through existing constructor and cannot forget any fields.
        let prev = mem::take(&mut self.id);
        *self = Self::new(prev)
    }

    fn extend_election_deadline(&mut self) {
        let add = Duration::from_secs_f64(ELECTION_TIMEOUT.as_secs_f64() * (rand::rand() + 1.0));
        self.election_deadline = Instant::now() + add;

        eprintln!("extended election deadline by {add:?}");
    }
}

#[derive(Debug, Default)]
pub struct Leader {
    pub next_index: HashMap<NodeID, usize>,
    pub match_index: HashMap<NodeID, usize>,
}

#[derive(Debug)]
pub(crate) enum State<C> {
    Follower {
        p: Persistent<C>,
        v: Volatile,
    },
    Candidate {
        p: Persistent<C>,
        v: Volatile,
        s: VotingSession,
    },
    #[expect(unused)]
    Leader {
        p: Persistent<C>,
        v: Volatile,
        l: Leader,
    },
    // For internal ownership handling.
    Transitioning,
}

/// For transitions, see Figure 4 of <https://raft.github.io/raft.pdf>.
impl<C> State<C> {
    pub(crate) fn new(id: NodeID, p: Persistent<C>) -> Self {
        Self::Follower {
            p,
            v: Volatile::new(id),
        }
    }

    pub(crate) fn begin_election(&mut self, outgoing: Sender<rpc::RaftMessage<C>>) {
        eprintln!("beginning election");

        if self.v().election_deadline >= Instant::now() {
            eprintln!("election deadline in the future, doing nothing");
            return;
        }

        match self {
            State::Leader { v, .. } => v.extend_election_deadline(),
            State::Follower { .. } | State::Candidate { .. } => self.become_candidate(outgoing),
            State::Transitioning => unreachable!("in transition"),
        }
    }

    pub(crate) fn become_candidate(&mut self, outgoing: Sender<rpc::RaftMessage<C>>) {
        // Need ownership of leader state to potentially drop. Else we leak, if we
        // transition away from Leader state.
        let prev = mem::replace(self, Self::Transitioning);

        match prev {
            Self::Follower { p, v } => {
                let session = VotingSession {
                    start: Instant::now(),
                    term: p.current_term,
                    votes: HashSet::from([v.id.clone()]),
                };
                *self = Self::Candidate { p, v, s: session };

                self.p_mut().current_term.step();

                // Do not call back for a while
                self.v_mut().extend_election_deadline();
                self.request_votes(outgoing);

                eprintln!("became candidate for term {}", self.p().current_term);
            }
            c @ Self::Candidate { .. } => *self = c,
            _ => unreachable!("invalid transition"),
        };
    }

    pub(crate) fn become_follower(&mut self) {
        let prev = mem::replace(self, Self::Transitioning);

        match prev {
            Self::Candidate { p, v, .. } | Self::Leader { p, v, l: _ } => {
                *self = Self::Follower { p, v };

                self.v_mut().extend_election_deadline();

                eprintln!("became follower for term {}", self.p().current_term);
            }
            _ => unreachable!("invalid transition"),
        };
    }

    pub(crate) fn request_votes(&mut self, outgoing: Sender<rpc::RaftMessage<C>>) {
        outgoing
            .send(rpc::RaftMessage::RequestVote {
                candidate_id: self.v().id.clone(),
                candidate_term: self.p().current_term,
                last_log_index: self
                    .p()
                    .log
                    .size()
                    .try_into()
                    .expect("should never exceed sendable log size"),
                last_log_term: self.p().log.last().term,
            })
            .expect("receiver should never hang up");
    }

    pub(crate) fn handle_vote_response(
        &mut self,
        remote_id: NodeID,
        remote_term: Term,
        vote_granted: bool,
    ) {
        match self {
            Self::Candidate {
                p,
                s: VotingSession { start, term, votes },
                ..
            } if vote_granted && *term == p.current_term && remote_term == p.current_term => {
                votes.insert(remote_id);
                eprintln!("have votes: {:?}", votes);
            }
            _ => eprintln!("ignoring vote"),
        };
    }

    pub(crate) fn maybe_step_down(&mut self, remote: Term) {
        let p = self.p_mut();
        if p.current_term < remote {
            eprintln!("stepping down: {} < remote {}", p.current_term, remote);
            p.current_term.advance(remote);
            self.become_follower();
        } else {
            eprintln!("not stepping down: {} >= remote {}", p.current_term, remote);
        }
    }

    pub(crate) fn v(&self) -> &Volatile {
        match self {
            State::Follower { v, .. } | State::Candidate { v, .. } | State::Leader { v, .. } => v,
            State::Transitioning => unreachable!("in transition"),
        }
    }

    pub(crate) fn v_mut(&mut self) -> &mut Volatile {
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
            let s: String = value.try_into()?;
            Ok(Self(s))
        }
    }

    type TestResult<T> = Result<T, Box<dyn Error>>;

    #[test]
    fn test_persistent_serialize_deserialize_roundtrip() -> TestResult<()> {
        let there = Persistent {
            current_term: Term(3),
            voted_for: Some(CandidateID(5)),
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
