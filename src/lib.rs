use std::fmt;

mod buffer;
#[cfg(feature = "serde")]
pub mod serde;

use buffer::Buffer;
pub use buffer::DataType;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown data type `{0}`")]
    InvalidDataType(u8),
    #[error("wrong data type `{actual:?}`, expected {expected:?}")]
    WrongDataType {
        actual: DataType,
        expected: DataType,
    },
    #[error("invalid size")]
    InvalidSize,
    #[error("reached end of buffer")]
    Eof,
    #[error("{0}")]
    Custom(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// A buffer representing a single JSONB value.
#[derive(Debug)]
pub struct JSONB<'d> {
    buffer: Buffer<'d>,
}

/// A string encoded in any of the JSONB formats.
#[derive(Debug)]
pub struct JSONBString<'d> {
    data: &'d [u8],
    #[allow(unused)] // TODO
    ty: DataType,
}

/// A JSONB array containing binary data.
#[derive(Clone)]
pub struct JSONBArray<'d> {
    data: Buffer<'d>,
}

#[derive(Clone)]
pub struct JSONBObject<'d> {
    data: Buffer<'d>,
}

/// Iterator over a JSONB array.
pub struct ArrayIter<'d>(
    // Officially a JSONB struct only contains one value. We move the data pointer after each
    // iteration.
    JSONB<'d>,
);

/// Iterator over a JSONB object.
pub struct ObjectIter<'d>(
    // An object is an array of alternating key, value elements.
    ArrayIter<'d>,
);

impl TryFrom<u8> for DataType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        Ok(match value {
            0 => DataType::Null,
            1 => DataType::True,
            2 => DataType::False,
            3 => DataType::Int,
            4 => DataType::Int5,
            5 => DataType::Float,
            6 => DataType::Float5,
            7 => DataType::Text,
            8 => DataType::TextJ,
            9 => DataType::Text5,
            10 => DataType::TextRaw,
            11 => DataType::Array,
            12 => DataType::Object,
            n => return Err(Error::InvalidDataType(n)),
        })
    }
}

impl From<DataType> for u8 {
    fn from(data_type: DataType) -> Self {
        data_type as u8
    }
}

impl<'d> JSONBString<'d> {
    pub fn as_str(&self) -> Option<&'d str> {
        // TODO this should only be done for certain data types
        std::str::from_utf8(self.data).ok()
    }
}

impl<'d> fmt::Display for JSONBString<'d> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Unescape
        if let Some(s) = self.as_str() {
            f.write_str(s)?;
        } else {
            todo!()
        }
        Ok(())
    }
}

impl<'b> PartialEq<str> for JSONBString<'b> {
    fn eq(&self, other: &str) -> bool {
        self.as_str().is_some_and(|value| value == other)
    }
}

impl<'b> PartialEq<&str> for JSONBString<'b> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().is_some_and(|value| value == *other)
    }
}

impl<'d> ArrayIter<'d> {
    fn new(array: JSONBArray<'d>) -> Self {
        Self(JSONB { buffer: array.data })
    }
}

impl<'d> Iterator for ArrayIter<'d> {
    type Item = JSONB<'d>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.0.buffer.is_empty() {
            return None;
        }
        let buffer = self.0.buffer.next().ok()?;
        Some(JSONB { buffer })
    }
}

impl<'d> JSONBArray<'d> {
    pub fn iter(&self) -> ArrayIter<'d> {
        // Clone is a copy.
        ArrayIter::new(self.clone())
    }
}

impl<'a, 'd: 'a> IntoIterator for &'a JSONBArray<'d> {
    type Item = JSONB<'d>;
    type IntoIter = ArrayIter<'d>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'d> ObjectIter<'d> {
    fn new(object: JSONBObject<'d>) -> Self {
        Self(JSONBArray { data: object.data }.iter())
    }
}

impl<'d> Iterator for ObjectIter<'d> {
    type Item = (JSONBString<'d>, JSONB<'d>);
    fn next(&mut self) -> Option<Self::Item> {
        let key = self.0.next()?;
        let value = self.0.next()?;
        Some((key.as_string()?, value))
    }
}

impl<'d> JSONBObject<'d> {
    fn to_array(&self) -> JSONBArray<'d> {
        JSONBArray { data: self.data }
    }

    pub fn iter(&self) -> ObjectIter<'d> {
        // Clone is memcpy
        ObjectIter::new(self.clone())
    }

    pub fn keys(&self) -> impl Iterator<Item = JSONBString<'d>> {
        ArrayIter::new(self.to_array())
            .step_by(2)
            // TODO error handling
            .filter_map(|element| element.as_string())
    }

    pub fn values(&self) -> impl Iterator<Item = JSONB<'d>> {
        ArrayIter::new(self.to_array()).skip(1).step_by(2)
    }
}

impl<'d> IntoIterator for JSONBObject<'d> {
    type Item = (JSONBString<'d>, JSONB<'d>);
    type IntoIter = ObjectIter<'d>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'d> JSONB<'d> {
    /// Read the input as a single JSONB value. Returns Err if the payload size stored inside the
    /// buffer does not match the length of the buffer.
    pub fn new(input: &'d [u8]) -> Result<Self> {
        let buffer = Buffer::new(input);
        if buffer.len() == buffer.expected_size()? {
            Ok(Self { buffer })
        } else {
            Err(Error::InvalidSize)
        }
    }

    fn data_type(&self) -> Result<DataType> {
        self.buffer.data_type()
    }

    fn payload(&self) -> Result<&'d [u8]> {
        self.buffer.payload()
    }

    pub fn as_null(&self) -> Option<()> {
        matches!(self.data_type(), Ok(DataType::Null)).then_some(())
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self.data_type() {
            Ok(DataType::True) => Some(true),
            Ok(DataType::False) => Some(false),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        // TODO do it correctly :P
        match self.data_type() {
            Ok(DataType::Int) => std::str::from_utf8(self.payload().ok()?).ok()?.parse().ok(),
            Ok(DataType::Int5) => std::str::from_utf8(self.payload().ok()?).ok()?.parse().ok(),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        // TODO do it correctly :P
        match self.data_type() {
            Ok(DataType::Float) => std::str::from_utf8(self.payload().ok()?).ok()?.parse().ok(),
            Ok(DataType::Float5) => std::str::from_utf8(self.payload().ok()?).ok()?.parse().ok(),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<JSONBString<'d>> {
        match self.data_type() {
            Ok(ty @ (DataType::Text | DataType::TextJ | DataType::Text5 | DataType::TextRaw)) => {
                Some(JSONBString {
                    data: self.payload().ok()?,
                    ty,
                })
            }
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<JSONBArray<'d>> {
        match self.data_type() {
            Ok(DataType::Array) => Some(JSONBArray {
                data: Buffer::new(self.payload().ok()?),
            }),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<JSONBObject<'d>> {
        match self.data_type() {
            Ok(DataType::Object) => Some(JSONBObject {
                data: Buffer::new(self.payload().ok()?),
            }),
            _ => None,
        }
    }
}

impl<'b> PartialEq<()> for JSONB<'b> {
    fn eq(&self, _other: &()) -> bool {
        self.as_null().is_some()
    }
}

impl<'b> PartialEq<bool> for JSONB<'b> {
    fn eq(&self, other: &bool) -> bool {
        self.as_bool().is_some_and(|value| value == *other)
    }
}

impl<'b> PartialEq<str> for JSONB<'b> {
    fn eq(&self, other: &str) -> bool {
        self.as_string().is_some_and(|value| value == other)
    }
}

impl<'b> PartialEq<&str> for JSONB<'b> {
    fn eq(&self, other: &&str) -> bool {
        self.as_string().is_some_and(|value| value == *other)
    }
}

impl<'b> PartialEq<i64> for JSONB<'b> {
    fn eq(&self, other: &i64) -> bool {
        self.as_int().is_some_and(|value| value == *other)
    }
}

impl<'b> PartialEq<i32> for JSONB<'b> {
    fn eq(&self, other: &i32) -> bool {
        self.as_int().is_some_and(|value| value == (*other).into())
    }
}

impl<'b> PartialEq<i16> for JSONB<'b> {
    fn eq(&self, other: &i16) -> bool {
        self.as_int().is_some_and(|value| value == (*other).into())
    }
}

impl<'b> PartialEq<i8> for JSONB<'b> {
    fn eq(&self, other: &i8) -> bool {
        self.as_int().is_some_and(|value| value == (*other).into())
    }
}

impl<'b> PartialEq<u64> for JSONB<'b> {
    fn eq(&self, other: &u64) -> bool {
        self.as_int()
            .zip((*other).try_into().ok())
            .is_some_and(|(value, other)| value == other)
    }
}

impl<'b> PartialEq<u32> for JSONB<'b> {
    fn eq(&self, other: &u32) -> bool {
        self.as_int().is_some_and(|value| value == (*other).into())
    }
}

impl<'b> PartialEq<u16> for JSONB<'b> {
    fn eq(&self, other: &u16) -> bool {
        self.as_int().is_some_and(|value| value == (*other).into())
    }
}

impl<'b> PartialEq<u8> for JSONB<'b> {
    fn eq(&self, other: &u8) -> bool {
        self.as_int().is_some_and(|value| value == (*other).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_null() -> Result<()> {
        assert_eq!(JSONB::new(&[0])?.as_null(), Some(()));
        assert_eq!(JSONB::new(&[1])?.as_null(), None);
        assert_eq!(JSONB::new(&[2])?.as_null(), None);

        Ok(())
    }

    #[test]
    fn as_bool() -> Result<()> {
        assert_eq!(JSONB::new(&[0])?.as_bool(), None);
        assert_eq!(JSONB::new(&[1])?.as_bool(), Some(true));
        assert_eq!(JSONB::new(&[2])?.as_bool(), Some(false));

        Ok(())
    }

    #[test]
    fn as_string() -> Result<()> {
        let Some(s) =
            JSONB::new(&[0x87, 0x61, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67])?.as_string()
        else {
            panic!("expected string");
        };
        assert_eq!(s.as_str(), Some("a string"));

        Ok(())
    }

    #[test]
    fn as_array() -> Result<()> {
        let Some(s) = JSONB::new(&[
            0xbb, 0x13, 0x31, 0x13, 0x32, 0x13, 0x33, 0x47, 0x66, 0x6f, 0x75, 0x72,
        ])?
        .as_array() else {
            panic!("expected array");
        };

        let mut iter = s.into_iter();
        assert_eq!(iter.next().unwrap().as_int(), Some(1));
        assert_eq!(iter.next().unwrap().as_int(), Some(2));
        assert_eq!(iter.next().unwrap().as_int(), Some(3));
        assert_eq!(
            iter.next().unwrap().as_string().unwrap().as_str(),
            Some("four")
        );
        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    fn as_object() -> Result<()> {
        let Some(s) = JSONB::new(&[
            0xcc, 0x30, 0x57, 0x70, 0x6c, 0x61, 0x69, 0x6e, 0x67, 0x6f, 0x62, 0x6a, 0x65, 0x63,
            0x74, 0x47, 0x77, 0x69, 0x74, 0x68, 0xcb, 0x0e, 0x47, 0x6b, 0x65, 0x79, 0x73, 0x17,
            0x26, 0x67, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x77, 0x6e, 0x65, 0x73, 0x74, 0x69,
            0x6e, 0x67, 0x5b, 0x13, 0x31, 0x13, 0x32, 0x0c,
        ])?
        .as_object() else {
            panic!("expected object");
        };

        let mut iter = s.into_iter();

        let (key, value) = iter.next().unwrap();
        assert_eq!(key, "plain");
        assert_eq!(value, "object");

        let (key, value) = iter.next().unwrap();
        assert_eq!(key, "with");
        assert!(value.as_array().is_some());

        let (key, value) = iter.next().unwrap();
        assert_eq!(key, "nesting");
        assert!(value.as_array().is_some());

        assert!(iter.next().is_none());

        Ok(())
    }
}
