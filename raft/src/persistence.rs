use std::collections::HashMap;
use std::env::temp_dir;
use std::error::Error;
use std::fmt::{self, Debug, Display};
use std::fs::{self, File};
use std::io::{self, BufWriter, ErrorKind, Read, Seek, Write};
use std::path::PathBuf;

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
/// Note, full ownership of state data is attained through cloning. While expensive in
/// terms of memory, this allows persistence to proceed without taking a lock on the
/// state across I/O operations (which can take ~unbounded time -- we don't control).
/// That frees the node to keep processing in other threads.
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

/// A [`File`] with "Move on `Flush`" semantics (funky huh).
#[derive(Debug)]
pub struct FileMoF {
    /// The target to manage.
    ///
    /// Note how we do not ever open a file handle to it: all writing is in the inner
    /// type. "Writing" to the target is an atomic file rename.
    target: PathBuf,
    inner: (PathBuf, BufWriter<File>),
}

impl FileMoF {
    /// Creates a new instance of a path to manage.
    pub fn new(path: PathBuf) -> io::Result<Self> {
        Ok(Self {
            target: path,
            inner: Self::new_backing()?,
        })
    }

    /// Generates a new random file name -- a bit like a UUID.
    fn generate_name() -> String {
        let mut s = String::with_capacity(64);
        s.push_str("raft-persistence-");
        for _ in 0..32 {
            let n = rand::rand() * 16.0;
            #[allow(clippy::cast_possible_truncation)]
            s.push(char::from_digit(n as u32, 16).expect("is within bounds"))
        }
        s
    }

    /// Creates a new random backing file, with a buffered, open handle to it. Ready to
    /// accept new writes for a new atomic swap on flush.
    ///
    /// Grants tries before giving up in case of file name conflicts (highly
    /// unlikely...).
    fn new_backing() -> io::Result<(PathBuf, BufWriter<File>)> {
        (0..3)
            .into_iter()
            .find_map(|_| {
                let path = temp_dir().join(Self::generate_name());
                let handle = File::create_new(&path).ok()?;
                Some((path, BufWriter::new(handle)))
            })
            .inspect(|(path, _)| {
                eprintln!(
                    "file: generated new backing file at {}",
                    path.to_string_lossy()
                );
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidFilename,
                    "after 3 attempts, unable to create temporary file",
                )
            })
    }
}

impl Write for FileMoF {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.1.write(buf)
    }

    /// For this type, flushing to the `target` means renaming to it, which is atomic.
    fn flush(&mut self) -> io::Result<()> {
        self.inner.1.flush()?; // Flush out buffered writer
        self.inner.1.get_ref().sync_all()?; // fsync

        std::fs::rename(&self.inner.0, &self.target)?;

        if let Some(parent) = self.inner.0.parent() {
            let dir = File::open(parent)?;
            dir.sync_all()?; // fsync rename
        }

        eprintln!(
            "file: flushed out, moved {} -> {}",
            self.inner.0.to_string_lossy(),
            self.target.to_string_lossy()
        );

        Ok(())
    }
}

impl Seek for FileMoF {
    /// NB: not too useful. To be atomic, we do not allow arbitrary seeking.
    fn seek(&mut self, _: io::SeekFrom) -> io::Result<u64> {
        eprintln!("file: warning: any seeking causes rewinding");
        self.rewind().map(|()| 0)
    }

    /// Rewinding resets the inner state, creating a fresh temporary copy to work
    /// against for a new atomic operation later on.
    fn rewind(&mut self) -> io::Result<()> {
        // No use for previous file anymore.
        fs::remove_file(&self.inner.0).or_else(|err| {
            if err.kind() == ErrorKind::NotFound {
                Ok(())
            } else {
                Err(err)
            }
        })?;

        // Create a new temporary file.
        self.inner = Self::new_backing()?;

        Ok(())
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

    #[test]
    fn test_file_mof() -> TestResult<()> {
        let target = temp_dir().join("test_file_mof");
        let mut f = FileMoF::new(target.clone())?;

        f.write_all(b"some bytes")?;
        f.flush()?;

        f.rewind()?;

        f.write_all(b"some more bytes")?;
        f.flush()?;

        f.rewind()?;

        fs::remove_file(target)?;

        Ok(())
    }
}
