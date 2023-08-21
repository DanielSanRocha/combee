use parquet::record::{Field, Row};
use serde::de::{self, Deserialize, Visitor};
use serde::forward_to_deserialize_any;

use crate::errors::Error;

type KeyVal<'de> = (Value<'de>, Value<'de>);

#[derive(Clone, Debug, PartialEq)]
enum Value<'de> {
    Name(&'de str),
    Field(&'de Field),
    Seq(Vec<KeyVal<'de>>),
}

struct MapAccess<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
    iter: std::vec::IntoIter<KeyVal<'de>>,
    value: Option<Value<'de>>,
    len: usize,
}

impl<'a, 'de: 'a> MapAccess<'a, 'de> {
    fn new(de: &'a mut Deserializer<'de>, v: Vec<KeyVal<'de>>) -> Self {
        let len = v.len();
        MapAccess {
            de,
            iter: v.into_iter(),
            value: None,
            len,
        }
    }
}

impl<'de, 'a> de::MapAccess<'de> for MapAccess<'a, 'de> {
    type Error = Error;

    fn next_key_seed<T: de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Error> {
        match self.iter.next() {
            Some((name, field)) => {
                self.len -= 1;
                self.de.value = Some(name);
                self.value = Some(field);
                Ok(Some(seed.deserialize(&mut *self.de)?))
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<T: de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<T::Value, Error> {
        let value = self.value.take().unwrap();
        self.de.value = Some(value);
        Ok(seed.deserialize(&mut *self.de)?)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len)
    }
}

struct SeqAccess<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
    iter: std::slice::Iter<'de, Field>,
    len: usize,
}

impl<'a, 'de: 'a> SeqAccess<'a, 'de> {
    fn new(de: &'a mut Deserializer<'de>, v: &'de [Field]) -> Self {
        let len = v.len();
        SeqAccess {
            de,
            iter: v.into_iter(),
            len,
        }
    }
}

impl<'de, 'a> de::SeqAccess<'de> for SeqAccess<'a, 'de> {
    type Error = Error;
    fn next_element_seed<T: de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Error> {
        match self.iter.next() {
            Some(value) => {
                self.len -= 1;
                self.de.value = Some(Value::Field(value));
                Ok(Some(seed.deserialize(&mut *self.de)?))
            }
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len)
    }
}

pub struct Deserializer<'de> {
    input: &'de Row,
    value: Option<Value<'de>>,
}

impl<'de> Deserializer<'de> {
    pub fn from_row(r: &'de Row) -> Self {
        Deserializer {
            input: r,

            value: None,
        }
    }

    fn get_next_value(&mut self) -> Result<Value<'de>, Error> {
        match self.value.take() {
            Some(v) => Ok(v),
            None => {
                let map = self
                    .input
                    .get_column_iter()
                    .map(|(n, f)| (Value::Name(n), Value::Field(f)))
                    .collect::<Vec<_>>();
                let value = Value::Seq(map);
                Ok(value)
            }
        }
    }
}

pub fn from_row<'a, T>(r: &'a Row) -> Result<T, Error>
where
    T: Deserialize<'a>,
{
    let mut deserializer = Deserializer::from_row(r);
    let t = T::deserialize(&mut deserializer)?;
    Ok(t)
}

impl<'de: 'a, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(mut self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        let value = self.get_next_value()?;
        match value {
            Value::Seq(v) => visitor.visit_map(MapAccess::new(&mut self, v)),
            Value::Name(s) => visitor.visit_str(s),
            Value::Field(f) => match f {
                Field::Null => visitor.visit_unit(),
                Field::Bool(v) => visitor.visit_bool(*v),
                Field::Byte(v) => visitor.visit_i8(*v),
                Field::Short(v) => visitor.visit_i16(*v),
                Field::Int(v) => visitor.visit_i32(*v),
                Field::Long(v) => visitor.visit_i64(*v),
                Field::UByte(v) => visitor.visit_u8(*v),
                Field::UShort(v) => visitor.visit_u16(*v),
                Field::UInt(v) => visitor.visit_u32(*v),
                Field::ULong(v) => visitor.visit_u64(*v),
                Field::Float(v) => visitor.visit_f32(*v),
                Field::Double(v) => visitor.visit_f64(*v),
                Field::Decimal(_) => todo!(),
                Field::Str(v) => visitor.visit_str(v),
                Field::Bytes(v) => visitor.visit_bytes(v.data()),
                Field::Date(v) => visitor.visit_u32(*v as u32),
                Field::TimestampMillis(v) => visitor.visit_u64(*v as u64),
                Field::TimestampMicros(v) => visitor.visit_u64(*v as u64),
                Field::Group(v) => visitor.visit_map(MapAccess::new(
                    &mut self,
                    v.get_column_iter()
                        .map(|(n, f)| (Value::Name(n), Value::Field(f)))
                        .collect(),
                )),
                Field::MapInternal(v) => visitor.visit_map(MapAccess::new(
                    &mut self,
                    v.entries()
                        .iter()
                        .map(|(k, v)| (Value::Field(k), Value::Field(v)))
                        .collect(),
                )),
                Field::ListInternal(v) => {
                    visitor.visit_seq(SeqAccess::new(&mut self, v.elements()))
                }
            },
        }
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit seq
            bytes byte_buf map tuple_struct struct identifier
            tuple ignored_any unit_struct enum option newtype_struct
    }
}
