use crate::Error;
use crate::Result;

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

#[derive(Debug, Clone, Copy)]
pub struct Buffer<'d> {
    data: &'d [u8],
}

impl<'d> Buffer<'d> {
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

        let b = *self.data.first().ok_or(Error::Eof)?;
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
        let end = start + self.payload_size()?;
        Ok(&self.data[start..end])
    }

    pub fn start_of_payload(&self) -> Result<usize> {
        Ok(1 + usize::from(self.header_size()?))
    }

    pub fn expected_size(&self) -> Result<usize> {
        Ok(self.start_of_payload()? + self.payload_size()?)
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn current(&self) -> Result<Buffer<'d>> {
        let size = self.expected_size()?;
        Ok(Buffer {
            data: &self.data[0..size],
        })
    }

    pub fn next(&mut self) -> Result<Buffer<'d>> {
        let size = self.expected_size()?;
        let (element, remainder) = self.data.split_at(size);
        self.data = remainder;
        Ok(Buffer { data: element })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_type() -> Result<()> {
        assert_eq!(Buffer::new(&[0]).data_type()?, DataType::Null);
        assert_eq!(Buffer::new(&[1]).data_type()?, DataType::True);
        assert_eq!(Buffer::new(&[2]).data_type()?, DataType::False);
        assert_eq!(Buffer::new(&[0x13, 0x30]).data_type()?, DataType::Int);
        assert_eq!(
            Buffer::new(&[0x35, 0x30, 0x2e, 0x30]).data_type()?,
            DataType::Float
        );
        assert_eq!(Buffer::new(&[0x07]).data_type()?, DataType::Text);
        assert_eq!(Buffer::new(&[0x0b]).data_type()?, DataType::Array);
        assert_eq!(Buffer::new(&[0x0c]).data_type()?, DataType::Object);
        Ok(())
    }

    fn check_size(input: &[u8]) -> Result<()> {
        assert_eq!(Buffer::new(input).expected_size()?, input.len());
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
}
