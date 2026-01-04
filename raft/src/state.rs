use std::collections::HashMap;
use std::fmt::{self, Display};

use json::Value as JSONValue;
use json::serde::{Deserialize, Serialize};

#[derive(Debug, Default, PartialEq)]
pub struct Candidate(pub u64);

impl Serialize for Candidate {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        Ok(self.0.into())
    }
}

impl Deserialize for Candidate {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        let v: u64 = value.try_into()?;
        Ok(Self(v))
    }
}

impl Display for Candidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct Term(pub u64);

impl Serialize for Term {
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        Ok(self.0.into())
    }
}

impl Deserialize for Term {
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        let v: u64 = value.try_into()?;
        Ok(Self(v))
    }
}

impl Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct LogEntry<C> {
    pub cmd: C,
    pub term: Term,
}

impl<C> Serialize for LogEntry<C>
where
    C: Serialize,
{
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        Ok(JSONValue::Object(HashMap::from([
            ("cmd".to_string(), self.cmd.serialize()?),
            ("term".to_string(), self.term.serialize()?),
        ])))
    }
}

impl<C> Deserialize for LogEntry<C>
where
    C: Deserialize,
{
    fn deserialize(value: JSONValue) -> Result<Self, json::serde::DeserializeError> {
        if let JSONValue::Object(mut map) = value {
            match (map.remove("cmd"), map.remove("term")) {
                (Some(cmd), Some(term)) => Ok(Self {
                    cmd: Deserialize::deserialize(cmd)?,
                    term: Deserialize::deserialize(term)?,
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

impl<C: Display> Display for LogEntry<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.cmd.to_string().escape_default())?;
        write!(f, "{}", self.term)
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct Persistent<C> {
    pub current_term: Term,
    pub voted_for: Option<Candidate>,
    pub log: Vec<LogEntry<C>>,
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

impl<C> Serialize for Persistent<C>
where
    C: Serialize,
{
    fn serialize(&self) -> Result<JSONValue, json::serde::SerializeError> {
        Ok(JSONValue::Object(HashMap::from([
            ("current_term".to_string(), self.current_term.serialize()?),
            ("voted_for".to_string(), self.voted_for.serialize()?),
            ("log".to_string(), self.log.serialize()?),
        ])))
    }
}

impl<C> Deserialize for Persistent<C>
where
    C: Deserialize,
{
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

#[derive(Debug, Default)]
pub struct Volatile {
    pub commit_index: usize,
    pub last_applied: usize,
}

#[derive(Debug, Default)]
pub struct Leader {
    pub next_index: Vec<usize>,
    pub match_index: Vec<usize>,
}

#[derive(Debug)]
pub enum State<C> {
    Follower {
        p: Persistent<C>,
        v: Volatile,
    },
    Candidate {
        p: Persistent<C>,
        v: Volatile,
    },
    Leader {
        p: Persistent<C>,
        v: Volatile,
        l: Leader,
    },
}

#[cfg(test)]
mod tests {
    use json::Value as JSONValue;

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

    #[test]
    fn test_persistent_serialize_deserialize_roundtrip() {
        let there = Persistent {
            current_term: Term(3),
            voted_for: Some(Candidate(5)),
            log: vec![
                LogEntry {
                    cmd: TestCommand("foo".into()),
                    term: Term(1),
                },
                LogEntry {
                    cmd: TestCommand("bar".into()),
                    term: Term(7),
                },
            ],
        };

        // In-memory values only
        let val = there.serialize().unwrap();
        let back = Persistent::<TestCommand>::deserialize(val).unwrap();

        assert_eq!(there, back);

        let json_string_there = there.serialize().unwrap().to_string();
        let back_parsed = Persistent::<TestCommand>::deserialize(
            json::parse(json_string_there.as_bytes()).unwrap(),
        )
        .unwrap();

        assert_eq!(there, back_parsed);
    }
}
