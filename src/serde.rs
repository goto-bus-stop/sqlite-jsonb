use crate::buffer::Buffer;
use crate::ArrayIter;
use crate::DataType;
use crate::Error;
use crate::Result;
use crate::JSONB;
use serde::de::{
    self, DeserializeSeed, EnumAccess, IgnoredAny, IntoDeserializer, MapAccess, SeqAccess,
    VariantAccess, Visitor,
};
use serde::Deserialize;
use std::fmt::Display;

impl serde::ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Custom(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Custom(msg.to_string())
    }
}

pub struct Deserializer<'de> {
    input: Buffer<'de>,
}

impl<'de> Deserializer<'de> {
    fn next(&mut self) -> Result<JSONB<'de>> {
        let element = self.input.next()?;
        Ok(JSONB { buffer: element })
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.input.data_type()? {
            DataType::Null => self.deserialize_unit(visitor),
            DataType::True | DataType::False => self.deserialize_bool(visitor),
            DataType::Text | DataType::TextJ | DataType::Text5 | DataType::TextRaw => {
                self.deserialize_str(visitor)
            }
            DataType::Int | DataType::Int5 => self.deserialize_i64(visitor),
            DataType::Float | DataType::Float5 => self.deserialize_i64(visitor),
            DataType::Array => self.deserialize_seq(visitor),
            DataType::Object => self.deserialize_map(visitor),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        visitor.visit_bool(element.as_bool().ok_or(Error::WrongDataType {
            actual: element.data_type().unwrap(),
            expected: DataType::True,
        })?)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        visitor.visit_i64(element.as_int().ok_or(Error::WrongDataType {
            actual: element.data_type().unwrap(),
            expected: DataType::Int,
        })?)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        visitor.visit_i8(element.as_int().and_then(|i| i.try_into().ok()).ok_or(
            Error::WrongDataType {
                actual: element.data_type().unwrap(),
                expected: DataType::Int,
            },
        )?)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        visitor.visit_i16(element.as_int().and_then(|i| i.try_into().ok()).ok_or(
            Error::WrongDataType {
                actual: element.data_type().unwrap(),
                expected: DataType::Int,
            },
        )?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        visitor.visit_i32(element.as_int().and_then(|i| i.try_into().ok()).ok_or(
            Error::WrongDataType {
                actual: element.data_type().unwrap(),
                expected: DataType::Int,
            },
        )?)
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        visitor.visit_i128(element.as_int().and_then(|i| i.try_into().ok()).ok_or(
            Error::WrongDataType {
                actual: element.data_type().unwrap(),
                expected: DataType::Int,
            },
        )?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        visitor.visit_u64(element.as_int().and_then(|i| i.try_into().ok()).ok_or(
            Error::WrongDataType {
                actual: element.data_type().unwrap(),
                expected: DataType::Int,
            },
        )?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        visitor.visit_u8(element.as_int().and_then(|i| i.try_into().ok()).ok_or(
            Error::WrongDataType {
                actual: element.data_type().unwrap(),
                expected: DataType::Int,
            },
        )?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        visitor.visit_u16(element.as_int().and_then(|i| i.try_into().ok()).ok_or(
            Error::WrongDataType {
                actual: element.data_type().unwrap(),
                expected: DataType::Int,
            },
        )?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        visitor.visit_u32(element.as_int().and_then(|i| i.try_into().ok()).ok_or(
            Error::WrongDataType {
                actual: element.data_type().unwrap(),
                expected: DataType::Int,
            },
        )?)
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        visitor.visit_u128(element.as_int().and_then(|i| i.try_into().ok()).ok_or(
            Error::WrongDataType {
                actual: element.data_type().unwrap(),
                expected: DataType::Int,
            },
        )?)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        visitor.visit_f64(element.as_float().ok_or(Error::WrongDataType {
            actual: element.data_type().unwrap(),
            expected: DataType::Float,
        })?)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        visitor.visit_f32(
            element
                .as_float()
                .map(|i| i as f32)
                .ok_or(Error::WrongDataType {
                    actual: element.data_type().unwrap(),
                    expected: DataType::Float,
                })?,
        )
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        // TODO handle encodings, read unicode char
        visitor.visit_char(
            element
                .as_string()
                .and_then(|s| (s.data.len() == 1).then_some(char::from(s.data[0])))
                .ok_or(Error::WrongDataType {
                    actual: element.data_type().unwrap(),
                    expected: DataType::Text,
                })?,
        )
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        // TODO handle encodings
        visitor.visit_borrowed_str(element.as_string().and_then(|s| s.as_str()).ok_or(
            Error::WrongDataType {
                actual: element.data_type().unwrap(),
                expected: DataType::Text,
            },
        )?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        // TODO
        visitor.visit_str(element.as_string().and_then(|s| s.as_str()).ok_or(
            Error::WrongDataType {
                actual: element.data_type().unwrap(),
                expected: DataType::Text,
            },
        )?)
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        element.as_null().ok_or(Error::WrongDataType {
            actual: element.data_type().unwrap(),
            expected: DataType::Null,
        })?;
        visitor.visit_unit()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        let array = element.as_array().ok_or(Error::WrongDataType {
            actual: element.data_type().unwrap(),
            expected: DataType::Array,
        })?;
        visitor.visit_seq(ArrayDeserializer {
            iter: array.into_iter(),
        })
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element = self.next()?;
        let object = element.as_object().ok_or(Error::WrongDataType {
            actual: element.data_type().unwrap(),
            expected: DataType::Object,
        })?;
        visitor.visit_map(ObjectDeserializer {
            iter: object.to_array().into_iter(),
        })
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if self.input.data_type()? == DataType::Null {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // TODO skip
        self.deserialize_any(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }
}

struct ArrayDeserializer<'de> {
    iter: ArrayIter<'de>,
}
impl<'de> SeqAccess<'de> for ArrayDeserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        match self.iter.next() {
            None => Ok(None),
            Some(element) => seed
                .deserialize(&mut Deserializer {
                    input: element.buffer,
                })
                .map(Some),
        }
    }
}

struct ObjectDeserializer<'de> {
    iter: ArrayIter<'de>,
}
impl<'de> MapAccess<'de> for ObjectDeserializer<'de> {
    type Error = Error;

    fn next_key_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        match self.iter.next() {
            None => Ok(None),
            Some(element) => seed
                .deserialize(&mut Deserializer {
                    input: element.buffer,
                })
                .map(Some),
        }
    }

    fn next_value_seed<T>(&mut self, seed: T) -> Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        let Some(element) = self.iter.next() else {
            return Err(Error::Eof);
        };
        seed.deserialize(&mut Deserializer {
            input: element.buffer,
        })
    }
}

pub fn from_slice<'a, T>(s: &'a [u8]) -> Result<T>
where
    T: Deserialize<'a>,
{
    let mut deserializer = Deserializer {
        input: Buffer::new(s),
    };
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        Err(Error::Eof)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals() {
        assert_eq!(from_slice::<()>(&[0x00]).unwrap(), ());
        assert_eq!(from_slice::<bool>(&[0x01]).unwrap(), true);
        assert_eq!(from_slice::<bool>(&[0x02]).unwrap(), false);
    }

    #[test]
    fn numbers() {
        assert_eq!(from_slice::<i64>(&[0x13, 0x30]).unwrap(), 0);
        assert_eq!(from_slice::<f64>(&[0x35, 0x30, 0x2e, 0x30]).unwrap(), 0.0);
        assert_eq!(from_slice::<i64>(&[0x13, 0x30]).unwrap(), 0);
        assert_eq!(
            from_slice::<&str>(&[0x87, 0x61, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67]).unwrap(),
            "a string"
        );
    }

    #[test]
    fn tuple() {
        type Tup = (i64, i64, i64, String);
        assert_eq!(
            from_slice::<Tup>(&[
                0xbb, 0x13, 0x31, 0x13, 0x32, 0x13, 0x33, 0x47, 0x66, 0x6f, 0x75, 0x72,
            ])
            .unwrap(),
            (1, 2, 3, "four".to_string())
        );
    }
}
