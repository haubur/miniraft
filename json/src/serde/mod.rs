//! A bit like [serde](https://serde.rs/) except much simpler (no derive macros, ...).
//! Conversions are therefore very verbose.

use std::error::Error;
use std::fmt::Display;

use crate::Value;
use crate::conversions::from_value::TryFromError;

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

/// Implementations for common and more complex types. Generally, implementing [`From`]
/// and/or [`TryFrom`] etc. on generic stdlib types is error-prone as it can lead to
/// infinite recursion. Being explicit with our own traits is much simpler and safer.
pub mod stddlib_impls {
    use super::{Deserialize, Serialize};
    use crate::Value;
    use crate::serde::{DeserializeError, SerializeError};

    // Primitives, non-generic

    impl Serialize for () {
        fn serialize(&self) -> Result<Value, SerializeError> {
            Ok(Value::Null)
        }
    }

    impl Serialize for String {
        fn serialize(&self) -> Result<Value, SerializeError> {
            Ok(self.as_str().into())
        }
    }

    impl Deserialize for String {
        fn deserialize(value: Value) -> Result<Self, super::DeserializeError> {
            let v = value.try_into()?;
            Ok(v)
        }
    }

    impl Serialize for u64 {
        fn serialize(&self) -> Result<Value, SerializeError> {
            Ok((*self).into())
        }
    }

    impl Deserialize for u64 {
        fn deserialize(value: Value) -> Result<Self, super::DeserializeError> {
            let v = value.try_into()?;
            Ok(v)
        }
    }

    impl Serialize for i64 {
        fn serialize(&self) -> Result<Value, SerializeError> {
            Ok((*self).into())
        }
    }

    impl Deserialize for i64 {
        fn deserialize(value: Value) -> Result<Self, super::DeserializeError> {
            let v = value.try_into()?;
            Ok(v)
        }
    }

    // Generics

    impl<T> Serialize for &T
    where
        T: Serialize,
    {
        fn serialize(&self) -> Result<Value, SerializeError> {
            // This is OK because `serialize` only needs a ref; we don't actually move
            // out here.
            (*self).serialize()
        }
    }

    impl<T> Serialize for [T]
    where
        T: Serialize,
    {
        fn serialize(&self) -> Result<Value, SerializeError> {
            let items: Result<Vec<Value>, _> = self.iter().map(|t| t.serialize()).collect();
            Ok(Value::Array(items?))
        }
    }

    impl<T> Serialize for Vec<T>
    where
        T: Serialize,
    {
        fn serialize(&self) -> Result<Value, SerializeError> {
            self.as_slice().serialize()
        }
    }

    impl<T> Deserialize for Vec<T>
    where
        T: Deserialize,
    {
        fn deserialize(value: Value) -> Result<Self, super::DeserializeError> {
            if let Value::Array(values) = value {
                let items: Result<Self, _> =
                    values.into_iter().map(|v| T::deserialize(v)).collect();
                Ok(items?)
            } else {
                Err(DeserializeError::InvalidValue(value))
            }
        }
    }

    impl<T> Serialize for Option<T>
    where
        T: Serialize,
    {
        fn serialize(&self) -> Result<Value, SerializeError> {
            match self {
                Some(v) => T::serialize(v),
                None => Ok(Value::Null),
            }
        }
    }

    impl<T> Deserialize for Option<T>
    where
        T: Deserialize,
    {
        fn deserialize(value: Value) -> Result<Self, super::DeserializeError> {
            if let Value::Null = value {
                Ok(None)
            } else {
                Ok(Some(T::deserialize(value)?))
            }
        }
    }
}
