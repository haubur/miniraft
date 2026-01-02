use std::{
    collections::HashMap,
    fmt::{self, Display},
};

use json::{Value as JSONValue, conversions::from_value::TryFromError};

#[derive(Debug, Default, PartialEq)]
pub struct Candidate(pub u64);

impl From<&Candidate> for JSONValue {
    fn from(value: &Candidate) -> Self {
        value.0.into()
    }
}

impl TryFrom<JSONValue> for Candidate {
    type Error = TryFromError;

    fn try_from(value: JSONValue) -> Result<Self, Self::Error> {
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

impl From<&Term> for JSONValue {
    fn from(value: &Term) -> Self {
        value.0.into()
    }
}

impl TryFrom<JSONValue> for Term {
    type Error = TryFromError;

    fn try_from(value: JSONValue) -> Result<Self, Self::Error> {
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

impl<C> From<&LogEntry<C>> for JSONValue
where
    for<'a> JSONValue: From<&'a C>,
{
    fn from(value: &LogEntry<C>) -> Self {
        Self::Object(HashMap::from([
            ("cmd".to_string(), (&value.cmd).into()),
            ("term".to_string(), (&value.term).into()),
        ]))
    }
}

impl<C: TryFrom<JSONValue, Error = TryFromError>> TryFrom<JSONValue> for LogEntry<C> {
    type Error = TryFromError;

    fn try_from(value: JSONValue) -> Result<Self, Self::Error> {
        if let JSONValue::Object(mut map) = value {
            match (map.remove("cmd"), map.remove("term")) {
                (Some(cmd), Some(term)) => Ok(Self {
                    cmd: cmd.try_into()?,
                    term: term.try_into()?,
                }),
                _ => Err(TryFromError {
                    value: JSONValue::Object(map),
                    reason: None,
                }),
            }
        } else {
            Err(TryFromError {
                value,
                reason: None,
            })
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

impl<C> From<&Persistent<C>> for JSONValue
where
    for<'a> JSONValue: From<&'a C>,
{
    fn from(value: &Persistent<C>) -> Self {
        Self::Object(HashMap::from([
            ("current_term".to_string(), (&value.current_term).into()),
            ("voted_for".to_string(), (&value.voted_for).into()),
            ("log".to_string(), value.log.as_slice().into()),
        ]))
    }
}

impl<C: TryFrom<JSONValue, Error = TryFromError>> TryFrom<JSONValue> for Persistent<C> {
    type Error = TryFromError;

    fn try_from(value: JSONValue) -> Result<Self, Self::Error> {
        if let JSONValue::Object(mut map) = value {
            match (
                map.remove("current_term"),
                map.remove("voted_for"),
                map.remove("log"),
            ) {
                (Some(current_term), Some(voted_for), Some(log)) => Ok(Self {
                    current_term: current_term.try_into()?,
                    voted_for: voted_for.try_into()?,
                    log: log.try_into()?,
                }),
                _ => Err(TryFromError {
                    value: JSONValue::Object(map),
                    reason: None,
                }),
            }
        } else {
            Err(TryFromError {
                value,
                reason: None,
            })
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
    use super::*;
    use json::{
        Value as JSONValue,
        serde::{Deserialize, Serialize},
    };

    #[derive(Debug, PartialEq)]
    struct TestCommand(String);

    impl From<&TestCommand> for JSONValue {
        fn from(value: &TestCommand) -> Self {
            value.0.clone().into()
        }
    }

    impl TryFrom<JSONValue> for TestCommand {
        type Error = TryFromError;

        fn try_from(value: JSONValue) -> Result<Self, Self::Error> {
            if let JSONValue::String(s) = value {
                Ok(Self(s))
            } else {
                Err(TryFromError {
                    value,
                    reason: None,
                })
            }
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

        let json = there.serialize().unwrap();
        let back = Persistent::<TestCommand>::deserialize(json).unwrap();

        assert_eq!(there, back);
    }
}
