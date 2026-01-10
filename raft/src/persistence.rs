use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Debug, Display};
use std::io::{self, Read, Write};

use json::Value as JSONValue;
use json::error::Error as JSONError;
use json::serde::{Deserialize, DeserializeError, Serialize, SerializeError};

use crate::NodeID;
use crate::state::{Log, Role, State, StateMachine, Term};

/// Persistent node state.
///
/// Needs to be persisted to stable storage before responding to requests, and restored
/// from it on node boot.
///
/// See also fig. 4 in <https://raft.github.io/raft.pdf>.
#[derive(Debug, PartialEq)]
pub struct Persistent<Cmd> {
    pub(crate) current_term: Term,
    pub(crate) voted_for: Option<NodeID>,
    pub(crate) log: Log<Cmd>,
}

/// If no persisted state to restore from exists, allow to start from scratch.
impl<Cmd> Default for Persistent<Cmd> {
    fn default() -> Self {
        Self {
            current_term: Default::default(),
            voted_for: None,
            log: Log { inner: vec![] },
        }
    }
}

/// Convert the current node state to a version suitable for persisting.
///
/// NB: full clone, not efficient.
impl<S: StateMachine> From<&State<S>> for Persistent<S::Command> {
    fn from(state: &State<S>) -> Self {
        Self {
            current_term: state.c.current_term,
            voted_for: match &state.r {
                Role::Follower { voted_for, .. } => voted_for.clone(),
                Role::Candidate { votes } => {
                    assert!(
                        votes.contains(&state.c.id),
                        "candidates always vote for themselves"
                    );
                    Some(state.c.id.clone())
                }
                Role::Leader { .. } => {
                    // By implication, must have voted for self to become leader.
                    Some(state.c.id.clone())
                }
            },
            log: state.c.log.clone(), // Full clone!
        }
    }
}

impl<C: Serialize> Serialize for Persistent<C> {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        Ok(JSONValue::Object(HashMap::from([
            ("current_term".to_string(), self.current_term.serialize()?),
            ("voted_for".to_string(), self.voted_for.serialize()?),
            ("log".to_string(), self.log.serialize()?),
        ])))
    }
}

impl<C: Deserialize> Deserialize for Persistent<C> {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        if let JSONValue::Object(mut map) = value {
            match (
                map.remove("current_term"),
                map.remove("voted_for"),
                map.remove("log"),
            ) {
                (Some(current_term), Some(voted_for), Some(log)) => Ok(Self {
                    current_term: Deserialize::deserialize(current_term)?,
                    voted_for: Deserialize::deserialize(voted_for)?,
                    log: Deserialize::deserialize(log)?,
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::state::LogEntry;

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
