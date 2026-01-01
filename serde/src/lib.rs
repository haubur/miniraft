use std::{
    error::Error,
    fmt::Display,
    io::{self, Read, Write},
};

#[derive(Debug)]
pub enum SerializeError {
    Io(io::Error),
}

impl From<io::Error> for SerializeError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl Display for SerializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SerializeError::Io(error) => write!(f, "i/o error: {error}"),
        }
    }
}

impl Error for SerializeError {}

#[derive(Debug)]
pub enum DeserializeError {
    Io(io::Error),
    InvalidTag { found: Vec<u8>, diag: String },
}

impl From<io::Error> for DeserializeError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl Display for DeserializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "i/o error: {error}"),
            Self::InvalidTag { found, diag } => {
                write!(f, "invalid tag '{:?}' for enum ({})", found, diag)
            }
        }
    }
}

impl Error for DeserializeError {}

pub trait Serialize {
    fn serialize(&self, writer: &mut impl Write) -> Result<(), SerializeError>;
}

pub trait Deserialize: Default {
    fn deserialize(reader: &mut impl Read) -> Result<Self, DeserializeError>
    where
        Self: Sized,
    {
        let mut s = Self::default();
        Self::deserialize_into(&mut s, reader)?;
        Ok(s)
    }

    fn deserialize_into(&mut self, reader: &mut impl Read) -> Result<(), DeserializeError>
    where
        Self: Sized;
}

impl Serialize for u64 {
    fn serialize(&self, writer: &mut impl Write) -> Result<(), SerializeError> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
}

impl Deserialize for u64 {
    fn deserialize_into(&mut self, reader: &mut impl Read) -> Result<(), DeserializeError>
    where
        Self: Sized,
    {
        let mut buf = [0; 8];
        reader.read_exact(&mut buf)?;
        *self = Self::from_le_bytes(buf);
        Ok(())
    }
}

impl Serialize for u8 {
    fn serialize(&self, writer: &mut impl Write) -> Result<(), SerializeError> {
        writer.write_all(&[*self])?;
        Ok(())
    }
}

impl Deserialize for u8 {
    fn deserialize_into(&mut self, reader: &mut impl Read) -> Result<(), DeserializeError>
    where
        Self: Sized,
    {
        let mut buf = [0; 1];
        reader.read_exact(&mut buf)?;
        *self = Self::from_le_bytes(buf);
        Ok(())
    }
}

impl<T: Serialize> Serialize for &[T] {
    fn serialize(&self, writer: &mut impl Write) -> Result<(), SerializeError> {
        (self.len() as u64).serialize(writer)?;
        for elem in self.iter() {
            elem.serialize(writer)?;
        }
        Ok(())
    }
}

impl<T: Deserialize> Deserialize for Vec<T> {
    fn deserialize_into(&mut self, reader: &mut impl Read) -> Result<(), DeserializeError>
    where
        Self: Sized,
    {
        let length = u64::deserialize(reader)?;
        self.reserve(length as usize);
        for _ in 0..length {
            self.push(T::deserialize(reader)?);
        }

        Ok(())
    }
}

impl<T: Serialize> Serialize for Option<T> {
    fn serialize(&self, writer: &mut impl Write) -> Result<(), SerializeError> {
        match self {
            Some(elem) => {
                writer.write_all(&[1])?;
                elem.serialize(writer)?;
            }
            None => writer.write_all(&[0])?,
        }

        Ok(())
    }
}

impl<T: Deserialize> Deserialize for Option<T> {
    fn deserialize_into(&mut self, reader: &mut impl Read) -> Result<(), DeserializeError>
    where
        Self: Sized,
    {
        let mut tag_buf = [0; 1];
        reader.read_exact(&mut tag_buf)?;
        match tag_buf {
            [0] => *self = None,
            [1] => {
                *self = Some(T::deserialize(reader)?);
            }
            v => {
                return Err(DeserializeError::InvalidTag {
                    found: v.into(),
                    diag: "Option only has tags: 0, 1".into(),
                });
            }
        }

        Ok(())
    }
}
