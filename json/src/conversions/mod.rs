//! Convenience conversion implementations for [`crate::Value`] and common stdlib types.
//!
//! This muddles the waters a bit and mixes JSON and the higher-level concept of
//! "serialization" (independent of JSON) but it works. Serialization/conversion is
//! hard-coded to JSON.

pub mod from_value;
pub mod to_value;
