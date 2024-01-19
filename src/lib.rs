use std::fmt;

#[cfg(feature = "serde")]
pub mod serde;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DataType {
    Null = 0,
    True = 1,
    False = 2,
    /// JSON RFC-8259 integer literal.
    Int = 3,
    /// JSON5 integer literal.
    Int5 = 4,
    /// JSON RFC-8259 float literal.
    Float = 5,
    /// JSON5 float literal.
    Float5 = 6,
    /// String contents that can be used directly both in JSON and SQL.
    Text = 7,
    /// JSON RFC-8259 string contents.
    TextJ = 8,
    /// JSON5 string contents.
    Text5 = 9,
    /// Unescaped string contents.
    TextRaw = 10,
    Array = 11,
    Object = 12,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderSize {
    Embedded = 0,
    U8 = 1,
    U16 = 2,
    U32 = 4,
    U64 = 8,
}

/// A buffer representing a single JSONB value.
#[derive(Debug)]
pub struct JSONB<'d> {
    data: &'d [u8],
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
    data: &'d [u8],
}

#[derive(Clone)]
pub struct JSONBObject<'d> {
    data: &'d [u8],
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
        Self(JSONB { data: array.data })
    }
}

impl<'d> Iterator for ArrayIter<'d> {
    type Item = JSONB<'d>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.0.data.is_empty() {
            return None;
        }
        match self.0.expected_size() {
            Ok(size) => {
                let element;
                (element, self.0.data) = self.0.data.split_at(size);
                Some(JSONB::new(element))
            }
            // TODO error handling
            Err(_) => None,
        }
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

impl From<HeaderSize> for usize {
    fn from(size: HeaderSize) -> usize {
        size as usize
    }
}
fn header_size_from_byte(b: u8) -> Option<HeaderSize> {
    match (b & 0xF0) >> 4 {
        0..=11 => Some(HeaderSize::Embedded),
        12 => Some(HeaderSize::U8),
        13 => Some(HeaderSize::U16),
        14 => Some(HeaderSize::U32),
        15 => Some(HeaderSize::U64),
        _ => None,
    }
}

impl<'d> JSONB<'d> {
    /// TODO should only have a checked conversion.
    pub fn new(data: &'d [u8]) -> Self {
        Self { data }
    }

    pub fn data_type(&self) -> Result<DataType> {
        let b = *self.data.first().ok_or(Error::Eof)?;
        DataType::try_from(b & 0x0F)
    }

    pub fn header_size(&self) -> Result<HeaderSize> {
        let b = *self.data.first().ok_or(Error::Eof)?;
        header_size_from_byte(b).ok_or(Error::InvalidSize)
    }

    /// TODO this panics when reading past end of data buffer
    pub fn payload_size(&self) -> Result<usize> {
        fn get_array<const N: usize>(slice: &[u8]) -> [u8; N] {
            let mut array = [0; N];
            array.copy_from_slice(slice);
            array
        }

        let b = self.data.get(0).cloned().unwrap_or_default();
        match (b & 0xF0) >> 4 {
            n @ 0..=11 => Ok(usize::from(n)),
            12 => Ok(usize::from(*self.data.get(1).ok_or(Error::Eof)?)),
            13 => Ok(usize::from(u16::from_le_bytes(get_array(&self.data[1..3])))),
            14 => Ok(usize::try_from(u32::from_le_bytes(get_array(&self.data[1..5]))).unwrap()),
            15 => Ok(usize::try_from(u64::from_le_bytes(get_array(&self.data[1..9]))).unwrap()),
            _ => Err(Error::InvalidSize),
        }
    }

    pub fn payload(&self) -> Result<&'d [u8]> {
        let start = self.start_of_payload()?;
        Ok(&self.data[start..])
    }

    pub fn start_of_payload(&self) -> Result<usize> {
        Ok(1 + usize::from(self.header_size()?))
    }

    pub fn expected_size(&self) -> Result<usize> {
        Ok(self.start_of_payload()? + self.payload_size()?)
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
                data: self.payload().ok()?,
            }),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<JSONBObject<'d>> {
        match self.data_type() {
            Ok(DataType::Object) => Some(JSONBObject {
                data: self.payload().ok()?,
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
    fn data_type() -> Result<()> {
        assert_eq!(JSONB::new(&[0]).data_type()?, DataType::Null);
        assert_eq!(JSONB::new(&[1]).data_type()?, DataType::True);
        assert_eq!(JSONB::new(&[2]).data_type()?, DataType::False);
        assert_eq!(JSONB::new(&[0x13, 0x30]).data_type()?, DataType::Int);
        assert_eq!(
            JSONB::new(&[0x35, 0x30, 0x2e, 0x30]).data_type()?,
            DataType::Float
        );
        assert_eq!(JSONB::new(&[0x07]).data_type()?, DataType::Text);
        assert_eq!(JSONB::new(&[0x0b]).data_type()?, DataType::Array);
        assert_eq!(JSONB::new(&[0x0c]).data_type()?, DataType::Object);
        Ok(())
    }

    fn check_size(input: &[u8]) -> Result<()> {
        assert_eq!(JSONB::new(input).expected_size()?, input.len());
        Ok(())
    }

    #[test]
    fn data_size() -> Result<()> {
        check_size(&[0])?;
        check_size(&[1])?;
        check_size(&[2])?;
        check_size(&[0x13, 0x30])?;
        check_size(&[0x35, 0x30, 0x2e, 0x30])?;
        check_size(&[0x07])?;
        check_size(&[0x0b])?;
        check_size(&[0x0c])?;

        Ok(())
    }

    #[test]
    fn as_null() {
        assert_eq!(JSONB::new(&[0]).as_null(), Some(()));
        assert_eq!(JSONB::new(&[1]).as_null(), None);
        assert_eq!(JSONB::new(&[2]).as_null(), None);
    }

    #[test]
    fn as_bool() {
        assert_eq!(JSONB::new(&[0]).as_bool(), None);
        assert_eq!(JSONB::new(&[1]).as_bool(), Some(true));
        assert_eq!(JSONB::new(&[2]).as_bool(), Some(false));
    }

    #[test]
    fn as_string() {
        let Some(s) =
            JSONB::new(&[0x87, 0x61, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67]).as_string()
        else {
            panic!("expected string");
        };
        assert_eq!(s.as_str(), Some("a string"));
    }

    #[test]
    fn as_array() {
        let Some(s) = JSONB::new(&[
            0xbb, 0x13, 0x31, 0x13, 0x32, 0x13, 0x33, 0x47, 0x66, 0x6f, 0x75, 0x72,
        ])
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
    }

    #[test]
    fn as_object() {
        let Some(s) = JSONB::new(&[
            0xcc, 0x30, 0x57, 0x70, 0x6c, 0x61, 0x69, 0x6e, 0x67, 0x6f, 0x62, 0x6a, 0x65, 0x63,
            0x74, 0x47, 0x77, 0x69, 0x74, 0x68, 0xcb, 0x0e, 0x47, 0x6b, 0x65, 0x79, 0x73, 0x17,
            0x26, 0x67, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x77, 0x6e, 0x65, 0x73, 0x74, 0x69,
            0x6e, 0x67, 0x5b, 0x13, 0x31, 0x13, 0x32, 0x0c,
        ])
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
    }
}
