use std::fmt::Debug;

use serde::{Deserialize, Serialize};

#[derive(Default, PartialEq)]
pub struct Put {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Debug for Put {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Put")
            .field("key", &String::from_utf8_lossy(&self.key))
            .field("value", &String::from_utf8_lossy(&self.value))
            .finish()
    }
}

impl Serialize for Put {
    fn serialize(&self, writer: &mut impl std::io::Write) -> Result<(), serde::SerializeError> {
        self.key.as_slice().serialize(writer)?;
        self.value.as_slice().serialize(writer)
    }
}

impl Deserialize for Put {
    fn deserialize_into(
        &mut self,
        reader: &mut impl std::io::Read,
    ) -> Result<(), serde::DeserializeError>
    where
        Self: Sized,
    {
        Vec::<u8>::deserialize_into(&mut self.key, reader)?;
        Vec::<u8>::deserialize_into(&mut self.value, reader)?;

        Ok(())
    }
}
