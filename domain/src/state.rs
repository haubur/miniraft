use std::fmt::{self, Display};

use serde::{Deserialize, DeserializeError, Serialize};

#[derive(Debug, Default, PartialEq)]
pub struct Candidate(pub u64);

impl Serialize for Candidate {
    fn serialize(&self, writer: &mut impl std::io::Write) -> Result<(), serde::SerializeError> {
        self.0.serialize(writer)
    }
}

impl Deserialize for Candidate {
    fn deserialize_into(&mut self, reader: &mut impl std::io::Read) -> Result<(), DeserializeError>
    where
        Self: Sized,
    {
        u64::deserialize_into(&mut self.0, reader)?;

        Ok(())
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
    fn serialize(&self, writer: &mut impl std::io::Write) -> Result<(), serde::SerializeError> {
        self.0.serialize(writer)
    }
}

impl Deserialize for Term {
    fn deserialize_into(&mut self, reader: &mut impl std::io::Read) -> Result<(), DeserializeError>
    where
        Self: Sized,
    {
        u64::deserialize_into(&mut self.0, reader)?;

        Ok(())
    }
}

impl Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// pub trait Command

#[derive(Debug, Default, PartialEq)]
pub struct LogEntry<C> {
    pub cmd: C,
    pub term: Term,
}

impl<C: Serialize> Serialize for LogEntry<C> {
    fn serialize(&self, writer: &mut impl std::io::Write) -> Result<(), serde::SerializeError> {
        self.cmd.serialize(writer)?;
        self.term.serialize(writer)
    }
}

impl<C: Deserialize> Deserialize for LogEntry<C> {
    fn deserialize_into(&mut self, reader: &mut impl std::io::Read) -> Result<(), DeserializeError>
    where
        Self: Sized,
    {
        C::deserialize_into(&mut self.cmd, reader)?;
        Term::deserialize_into(&mut self.term, reader)?;

        Ok(())
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

impl<C: Serialize> Serialize for Persistent<C> {
    fn serialize(&self, writer: &mut impl std::io::Write) -> Result<(), serde::SerializeError> {
        self.current_term.serialize(writer)?;
        self.voted_for.serialize(writer)?;
        self.log.as_slice().serialize(writer)?;

        Ok(())
    }
}

impl<C: Deserialize> Deserialize for Persistent<C> {
    fn deserialize_into(
        &mut self,
        reader: &mut impl std::io::Read,
    ) -> Result<(), DeserializeError> {
        Term::deserialize_into(&mut self.current_term, reader)?;
        Option::<Candidate>::deserialize_into(&mut self.voted_for, reader)?;
        Vec::<LogEntry<C>>::deserialize_into(&mut self.log, reader)?;

        Ok(())
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
