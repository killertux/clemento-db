use bytes::Bytes;
use std::borrow::Cow;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{types::UVarInt, MapErrIntoOtherError};

#[derive(Debug, Clone)]
pub(crate) struct Memtable {
    size: usize,
    data: Vec<(Key, Value)>,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            size: 0,
            data: vec![],
        }
    }

    pub fn put(&mut self, key: Cow<'_, Bytes>, value: Value) {
        let key_size = key.len();
        if key_size == 0 {
            return;
        }
        let value_size = value.size();
        match self.data.binary_search_by(|entry| entry.0.cmp(&key)) {
            Ok(index) => {
                let old_value_size = self.data[index].1.size();
                self.data[index].1 = value;
                self.size -= old_value_size;
                self.size += value_size;
            }
            Err(index) => {
                self.size += key_size + value_size;
                self.data.insert(index, (key.into_owned(), value))
            }
        }
    }

    pub fn get(&self, key: &Bytes) -> Option<&Value> {
        match self.data.binary_search_by(|entry| entry.0.cmp(key)) {
            Ok(index) => Some(&self.data[index].1),
            Err(_) => None,
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn data(&self) -> &[(Key, Value)] {
        &self.data
    }

    pub async fn write<W>(&self, writer: &mut W) -> Result<(), std::io::Error>
    where
        W: AsyncWrite + Unpin,
    {
        UVarInt::try_from(self.size)
            .map_err_into_other_error()?
            .write(writer)
            .await?;
        UVarInt::try_from(self.data.len())
            .map_err_into_other_error()?
            .write(writer)
            .await?;
        for (key, value) in self.data.iter() {
            UVarInt::try_from(key.len())
                .map_err_into_other_error()?
                .write(writer)
                .await?;
            writer.write_all(key).await?;
            value.write(writer).await?;
        }
        Ok(())
    }

    pub async fn read<R>(reader: &mut R) -> Result<Self, std::io::Error>
    where
        R: AsyncRead + Unpin,
    {
        let size = UVarInt::read(reader)
            .await?
            .try_into()
            .map_err_into_other_error()?;
        let data_size = UVarInt::read(reader)
            .await?
            .try_into()
            .map_err_into_other_error()?;
        let mut data = Vec::with_capacity(data_size);
        for _ in 0..data_size {
            let key_size = UVarInt::read(reader)
                .await?
                .try_into()
                .map_err_into_other_error()?;
            let mut key = vec![0; key_size];
            reader.read_exact(&mut key).await?;
            let value = Value::read(reader).await?;
            data.push((key.into(), value));
        }
        Ok(Self { size, data })
    }
}

pub(crate) type Key = Bytes;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Value {
    TombStone,
    Data(Bytes),
}

impl Value {
    pub fn size(&self) -> usize {
        match self {
            Value::TombStone => 1,
            Value::Data(value) => value.len(),
        }
    }

    pub async fn write<W>(&self, writer: &mut W) -> Result<usize, std::io::Error>
    where
        W: AsyncWrite + Unpin,
    {
        Ok(match self {
            Value::TombStone => {
                UVarInt::try_from(0)
                    .map_err_into_other_error()?
                    .write(writer)
                    .await?
            }
            Value::Data(value) => {
                let size = UVarInt::try_from(value.len() + 1)
                    .map_err_into_other_error()?
                    .write(writer)
                    .await?;
                writer.write_all(value).await?;
                size + value.len()
            }
        })
    }

    pub async fn read<R>(reader: &mut R) -> Result<Self, std::io::Error>
    where
        R: AsyncRead + Unpin,
    {
        let size = UVarInt::read(reader).await?;
        if size.is_zero() {
            return Ok(Value::TombStone);
        }
        let size: usize = size.try_into().map_err_into_other_error()?;
        let mut value = vec![0; size - 1];
        reader.read_exact(&mut value).await?;
        Ok(Value::Data(value.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_non_existent() {
        let lsm = Memtable::new();
        let key = Bytes::from("key");
        assert_eq!(lsm.get(&key), None);
        assert_eq!(lsm.size(), 0);
    }

    #[test]
    fn test_put_get() {
        let mut lsm = Memtable::new();
        let key = Bytes::from("key");
        let value = Bytes::from("value");
        lsm.put(Cow::Borrowed(&key), Value::Data(value.clone()));
        assert_eq!(lsm.get(&key), Some(&Value::Data(value)));
        assert_eq!(lsm.size(), 8);
    }

    #[test]
    fn test_put_existent_should_overwrite() {
        let mut lsm = Memtable::new();
        let key = Bytes::from("key");
        let value_1 = Bytes::from("value 1");
        let value_2 = Bytes::from("value 2");
        lsm.put(Cow::Borrowed(&key), Value::Data(value_1.clone()));
        lsm.put(Cow::Borrowed(&key), Value::Data(value_2.clone()));
        assert_eq!(lsm.get(&key), Some(&Value::Data(value_2)));
        assert_eq!(lsm.size(), 10);
        lsm.put(Cow::Borrowed(&key), Value::TombStone);
        assert_eq!(lsm.get(&key), Some(&Value::TombStone));
        assert_eq!(lsm.size(), 4);
    }
}
