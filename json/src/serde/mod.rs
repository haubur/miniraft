//! A bit like [serde](https://serde.rs/) except much simpler (no derive macros, ...).
//! Conversions are therefore very verbose.

use std::{error::Error, fmt::Display};

use crate::{Value, conversions::from_value::TryFromError};

/// Serialize a type into a JSON representation.
pub trait Serialize {
    fn serialize(&self) -> Result<Value, SerializeError>;
}

/// Deserialize a type from a JSON representation.
pub trait Deserialize {
    fn deserialize(value: Value) -> Result<Self, DeserializeError>
    where
        Self: Sized;
}

#[derive(Debug)]
pub enum SerializeError {}

impl Display for SerializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "serialization error")
    }
}

impl Error for SerializeError {}

#[derive(Debug)]
pub enum DeserializeError {
    InvalidConversion(TryFromError),
    InvalidValue(Value),
}

impl From<TryFromError> for DeserializeError {
    fn from(value: TryFromError) -> Self {
        Self::InvalidConversion(value)
    }
}

impl Display for DeserializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConversion(v) => write!(f, "invalid conversion: {v:?}"),
            Self::InvalidValue(v) => write!(f, "invalid object: {v:?}"),
        }
    }
}

impl Error for DeserializeError {}

/// Blanket impl for any type of which we can build a [`Value`] from just a reference.
impl<T> Serialize for T
where
    for<'a> Value: From<&'a T>, // for any lifetime, doesn't matter
{
    fn serialize(&self) -> Result<Value, SerializeError> {
        let v: Value = self.into();
        Ok(v)
    }
}

/// Blanket impl to deserialize any type which has a [`TryFrom`], where its associated
/// error can be converted.
impl<T> Deserialize for T
where
    T: TryFrom<Value>,
    <T as TryFrom<Value>>::Error: Into<DeserializeError>,
{
    fn deserialize(value: Value) -> Result<Self, DeserializeError>
    where
        Self: Sized,
    {
        value.try_into().map_err(Into::into)
    }
}
