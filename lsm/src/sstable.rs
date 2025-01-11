use std::io::{ErrorKind, SeekFrom};

use bloomfilter::Bloom;
use bytes::{BufMut, Bytes, BytesMut};
use itertools::Itertools;
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter},
};

use crate::{
    memtable::{Key, Memtable, Value},
    types::{Fixed28BitsInt, Fixed56BitsInt, UVarInt},
};

pub struct SSTable {
    metadata: SSTableMetadata,
    data: Vec<(Key, Value)>,
}

const BLOOM_FILTER_FP: f64 = 0.1;

impl SSTable {
    pub fn try_from_memtable(
        memtable: Memtable,
        file_id: u64,
    ) -> Result<Self, ErrorCreatingSSTable> {
        let data = memtable.data();
        let mut bloom_filter = Bloom::new_for_fp_rate(data.len(), BLOOM_FILTER_FP)
            .map_err(|err| ErrorCreatingSSTable::BloomFilterError(err))?;
        data.iter().for_each(|entry| bloom_filter.set(&entry.0));
        let metadata = SSTableMetadata {
            bloom_filter,
            level: 0,
            file_id,
            n_entries: data.len() as u64,
        };
        Ok(Self { metadata, data })
    }

    pub async fn store_and_return_metadata(
        self,
        base_path: &str,
    ) -> Result<SSTableMetadata, std::io::Error> {
        let mut keys_file = BufWriter::new(
            File::create(format!(
                "{base_path}/{}_{:08}.sst_keys",
                self.metadata.level, self.metadata.file_id
            ))
            .await?,
        );
        let mut values_file = BufWriter::new(
            File::create(format!(
                "{base_path}/{}_{:08}.sst_values",
                self.metadata.level, self.metadata.file_id
            ))
            .await?,
        );
        self.metadata.store(base_path).await?;
        let mut values_cursor: usize = 0;
        for (key, value) in self.data {
            keys_file.write_u8(0).await?;
            let key = quote_null_bytes(key);
            let key_size = key.len();
            Fixed28BitsInt::new(key_size.try_into().map_err_into_other_error()?)
                .write(&mut keys_file)
                .await?;
            keys_file.write_all(&key).await?;
            Fixed56BitsInt::new(values_cursor.try_into().map_err_into_other_error()?)
                .write(&mut keys_file)
                .await?;
            match value {
                Value::Data(value) => {
                    values_cursor += UVarInt::try_from(value.len() + 1)
                        .map_err_into_other_error()?
                        .write(&mut values_file)
                        .await?;
                    values_cursor += value.len();
                    values_file.write_all(&value).await?;
                }
                Value::TombStone => {
                    values_cursor += UVarInt::try_from(0)
                        .map_err_into_other_error()?
                        .write(&mut values_file)
                        .await?;
                }
            }
        }
        keys_file.flush().await?;
        values_file.flush().await?;
        Ok(self.metadata)
    }

    pub async fn load_value(
        metadata: &SSTableMetadata,
        base_path: &str,
        key: &Key,
    ) -> Result<Option<Value>, std::io::Error> {
        let mut keys_file = BufReader::new(
            File::open(format!(
                "{base_path}/{}_{:08}.sst_keys",
                metadata.level, metadata.file_id
            ))
            .await?,
        );
        let mut start = 0;
        let mut end = keys_file.seek(SeekFrom::End(0)).await?;
        'external: loop {
            let mut mid = (start + end) / 2;
            let initial_mid = mid;
            let pos = keys_file.seek(SeekFrom::Start(mid)).await?;
            let mut old_bytes = None;
            'inner: loop {
                match keys_file.read_u8().await {
                    Ok(0) if old_bytes.is_some() && old_bytes != Some(0xff) || pos == 0 => {
                        break 'inner;
                    }
                    Ok(byte) => {
                        old_bytes = Some(byte);
                        if mid == start {
                            return Ok(None);
                        }
                        mid += 1;
                        if mid > end {
                            end = initial_mid;
                            continue 'external;
                        }
                    }
                    Err(err) if matches!(err.kind(), ErrorKind::UnexpectedEof) => {
                        end = initial_mid;
                        continue 'external;
                    }
                    Err(err) => return Err(err),
                }
            }

            let key_size: usize = Fixed28BitsInt::read(&mut keys_file)
                .await?
                .value()
                .try_into()
                .map_err_into_other_error()?;
            let loaded_key = {
                let mut key = vec![0u8; key_size];
                keys_file.read_exact(&mut key).await?;
                unquote_null_bytes(key.into())
            };
            let n_loaded_key = u64::from_be_bytes(loaded_key.as_ref().try_into().unwrap());
            let n_key = u64::from_be_bytes(key.as_ref().try_into().unwrap());
            println!(
                "{},{},{},{}",
                n_loaded_key,
                n_key,
                n_loaded_key < n_key,
                loaded_key < *key
            );
            if loaded_key == *key {
                let value_pos: usize = Fixed56BitsInt::read(&mut keys_file)
                    .await?
                    .value()
                    .try_into()
                    .map_err_into_other_error()?;
                let mut values_file = BufReader::new(
                    File::open(format!(
                        "{base_path}/{}_{:08}.sst_values",
                        metadata.level, metadata.file_id
                    ))
                    .await?,
                );
                values_file.seek(SeekFrom::Start(value_pos as u64)).await?;
                let value_size = UVarInt::read(&mut values_file).await?;
                if value_size.is_zero() {
                    return Ok(Some(Value::TombStone));
                }
                let value_size: usize = value_size.try_into().map_err_into_other_error()?;
                let mut value = vec![0u8; value_size - 1];
                values_file.read_exact(&mut value).await?;
                return Ok(Some(Value::Data(value.into())));
            }
            if loaded_key < *key {
                start = initial_mid;
            } else {
                end = initial_mid;
            }
        }
    }

    pub async fn compact(
        metadatas: &[SSTableMetadata],
        base_path: &str,
        new_level: u8,
        new_file_id: u64,
    ) -> Result<SSTableMetadata, SSTableCompactError> {
        println!("Compacting...");
        let mut readers = Vec::new();
        for metadata in metadatas.iter().rev() {
            let reader = Self::key_value_reader(base_path, metadata).await?;
            readers.push(reader);
        }

        let mut keys_file = BufWriter::new(
            File::options()
                .create(true)
                .write(true)
                .read(true)
                .open(format!(
                    "{base_path}/{}_{:08}.sst_keys",
                    new_level, new_file_id
                ))
                .await?,
        );
        let mut values_file = BufWriter::new(
            File::options()
                .create(true)
                .write(true)
                .read(true)
                .open(format!(
                    "{base_path}/{}_{:08}.sst_values",
                    new_level, new_file_id
                ))
                .await?,
        );

        let mut n_entries = 0;
        let mut values_cursor = 0;
        loop {
            let mut min_key: Option<Key> = None;
            let mut min_value = None;
            for reader in readers.iter_mut() {
                if let Some((key, value)) = reader.read_without_consuming().await? {
                    if min_key.as_ref().map(|m_key| *key < m_key).unwrap_or(true) {
                        min_key = Some(key.clone());
                        min_value = Some(value);
                    }
                }
            }
            let Some(key) = min_key else {
                break;
            };
            let value = min_value.expect("We should always have a value");
            if *value == Value::TombStone {
                for reader in readers.iter_mut() {
                    if let Some((reader_key, _)) = reader.read_without_consuming().await? {
                        if reader_key == &key {
                            reader.consume();
                        }
                    }
                }
                continue;
            }
            n_entries += 1;
            keys_file.write_u8(0).await?;
            let quoted_key = quote_null_bytes(key.clone());
            Fixed28BitsInt::new(quoted_key.len().try_into().map_err_into_other_error()?)
                .write(&mut keys_file)
                .await?;
            keys_file.write_all(&quoted_key).await?;
            Fixed56BitsInt::new(values_cursor.try_into().map_err_into_other_error()?)
                .write(&mut keys_file)
                .await?;
            match value {
                Value::Data(value) => {
                    values_cursor += UVarInt::try_from(value.len() + 1)
                        .map_err_into_other_error()?
                        .write(&mut values_file)
                        .await?;
                    values_cursor += value.len();
                    values_file.write_all(&value).await?;
                }
                Value::TombStone => {
                    values_cursor += UVarInt::try_from(0)
                        .map_err_into_other_error()?
                        .write(&mut values_file)
                        .await?;
                }
            }
            for reader in readers.iter_mut() {
                if let Some((reader_key, _)) = reader.read_without_consuming().await? {
                    if reader_key == &key {
                        reader.consume();
                    }
                }
            }
        }
        keys_file.flush().await?;
        values_file.flush().await?;
        let mut bloom_filter = Bloom::new_for_fp_rate(n_entries, BLOOM_FILTER_FP)
            .map_err(|err| ErrorCreatingSSTable::BloomFilterError(err))?;
        {
            let mut file = keys_file.into_inner();
            file.seek(SeekFrom::Start(0)).await?;
            let mut reader = KeyReader {
                keys_reader: BufReader::new(file),
                n_entries: n_entries as u64,
                read_entries: 0,
            };
            while let Some(key) = reader.read().await? {
                bloom_filter.set(&key);
            }
        }
        let metadata = SSTableMetadata {
            bloom_filter,
            level: new_level,
            file_id: new_file_id,
            n_entries: n_entries as u64,
        };
        metadata.store(base_path).await?;
        for metadata in metadatas {
            tokio::fs::remove_file(format!(
                "{base_path}/{}_{:08}.sst_keys",
                metadata.level, metadata.file_id
            ))
            .await?;
            tokio::fs::remove_file(format!(
                "{base_path}/{}_{:08}.sst_values",
                metadata.level, metadata.file_id
            ))
            .await?;
            tokio::fs::remove_file(format!(
                "{base_path}/{}_{:08}.sst_metadata",
                metadata.level, metadata.file_id
            ))
            .await?;
        }
        Ok(metadata)
    }

    async fn key_value_reader(
        base_path: &str,
        metadata: &SSTableMetadata,
    ) -> Result<KeyValueReader<BufReader<File>, BufReader<File>>, std::io::Error> {
        let keys_file = BufReader::new(
            File::open(format!(
                "{base_path}/{}_{:08}.sst_keys",
                metadata.level, metadata.file_id
            ))
            .await
            .unwrap(),
        );
        let values_file = BufReader::new(
            File::open(format!(
                "{base_path}/{}_{:08}.sst_values",
                metadata.level, metadata.file_id
            ))
            .await
            .unwrap(),
        );
        Ok(KeyValueReader {
            keys_reader: keys_file,
            value_reder: values_file,
            n_entries: metadata.n_entries,
            read_entries: 0,
            element: None,
        })
    }
}

#[derive(Debug)]
struct KeyValueReader<KeyR, ValueR> {
    keys_reader: KeyR,
    value_reder: ValueR,
    n_entries: u64,
    read_entries: u64,
    element: Option<(Key, Value)>,
}

impl<KeyR, ValueR> KeyValueReader<KeyR, ValueR>
where
    KeyR: AsyncRead + Unpin,
    ValueR: AsyncRead + Unpin,
{
    async fn read_without_consuming(&mut self) -> Result<Option<&(Key, Value)>, std::io::Error> {
        if self.element.is_none() {
            self.element = self.read().await?;
        }
        Ok(self.element.as_ref())
    }

    fn consume(&mut self) {
        self.element = None;
    }

    async fn read(&mut self) -> Result<Option<(Key, Value)>, std::io::Error> {
        if self.read_entries == self.n_entries {
            return Ok(None);
        }
        if self.keys_reader.read_u8().await? != 0 {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Invalid entry in SSTable",
            ));
        };
        self.read_entries += 1;
        let key_size = Fixed28BitsInt::read(&mut self.keys_reader).await?.value();
        let key_size: usize = key_size.try_into().map_err_into_other_error()?;
        let key = {
            let mut key = vec![0u8; key_size];
            self.keys_reader.read_exact(&mut key).await?;
            unquote_null_bytes(key.into())
        };
        let _ = Fixed56BitsInt::read(&mut self.keys_reader).await?;
        let value_size = UVarInt::read(&mut self.value_reder).await?;
        if value_size.is_zero() {
            return Ok(Some((key, Value::TombStone)));
        }
        let value_size: usize = value_size.try_into().map_err_into_other_error()?;
        let value_size = value_size - 1usize;
        let mut value = vec![0u8; value_size as usize];
        self.value_reder.read_exact(&mut value).await?;
        Ok(Some((key, Value::Data(value.into()))))
    }
}

#[derive(Debug)]
struct KeyReader<R> {
    keys_reader: R,
    n_entries: u64,
    read_entries: u64,
}

impl<R> KeyReader<R>
where
    R: AsyncRead + Unpin,
{
    async fn read(&mut self) -> Result<Option<Key>, std::io::Error> {
        if self.read_entries == self.n_entries {
            return Ok(None);
        }
        if self.keys_reader.read_u8().await? != 0 {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Invalid entry in SSTable",
            ));
        };
        self.read_entries += 1;
        let key_size = Fixed28BitsInt::read(&mut self.keys_reader).await?.value();
        let key_size: usize = key_size.try_into().map_err_into_other_error()?;
        let key = {
            let mut key = vec![0u8; key_size];
            self.keys_reader.read_exact(&mut key).await?;
            unquote_null_bytes(key.into())
        };
        let _ = Fixed56BitsInt::read(&mut self.keys_reader).await?;
        Ok(Some(key))
    }
}

fn quote_null_bytes(bytes: Bytes) -> Bytes {
    bytes
        .into_iter()
        .fold(BytesMut::new(), |mut acc, byte| {
            if byte == 0u8 {
                acc.put_u8(0xff);
                acc.put_u8(0);
                acc
            } else {
                acc.put_u8(byte);
                acc
            }
        })
        .into()
}

fn unquote_null_bytes(bytes: Bytes) -> Bytes {
    let last = bytes.last().cloned();
    bytes
        .into_iter()
        .tuple_windows()
        .filter_map(|(first, second)| {
            if first == 0xff && second == 0 {
                None
            } else {
                Some(first)
            }
        })
        .chain(last)
        .collect()
}

#[derive(Debug, Error)]
pub enum ErrorCreatingSSTable {
    #[error("Error creating bloom filter: {0}")]
    BloomFilterError(&'static str),
}

#[derive(Debug, Error)]
pub enum SSTableCompactError {
    #[error(transparent)]
    ErrorCreatingSSTable(#[from] ErrorCreatingSSTable),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
}

#[derive(Debug)]
pub struct SSTableMetadata {
    bloom_filter: Bloom<Key>,
    level: u8,
    file_id: u64,
    n_entries: u64,
}

impl SSTableMetadata {
    pub fn level(&self) -> u8 {
        self.level
    }

    pub fn check(&self, key: &Key) -> bool {
        self.bloom_filter.check(&key)
    }

    async fn store(&self, base_path: &str) -> Result<(), std::io::Error> {
        let mut file = BufWriter::new(
            File::create(format!(
                "{base_path}/{}_{:08}.sst_metadata",
                self.level, self.file_id
            ))
            .await?,
        );
        let bloom_filter_slice = self.bloom_filter.as_slice();
        UVarInt::try_from(bloom_filter_slice.len())
            .map_err_into_other_error()?
            .write(&mut file)
            .await?;
        file.write_all(bloom_filter_slice).await?;
        file.write_u8(self.level).await?;
        file.write_u64(self.file_id).await?;
        file.write_u64(self.n_entries).await
    }
}

trait MapErrIntoOtherError<T, E> {
    fn map_err_into_other_error(self) -> Result<T, std::io::Error>;
}

impl<T, E> MapErrIntoOtherError<T, E> for Result<T, E>
where
    E: ToString,
{
    fn map_err_into_other_error(self) -> Result<T, std::io::Error> {
        self.map_err(|err| std::io::Error::new(ErrorKind::Other, err.to_string()))
    }
}

#[cfg(test)]
mod test {
    use std::borrow::Cow;

    use super::*;

    #[test]
    fn quote_unqute() {
        let bytes = vec![0u8, 1, 0xff, 0, 2, 0xff, 0xff, 3];
        let quoted = super::quote_null_bytes(bytes.clone().into());
        assert_eq!(vec![0xff, 0u8, 1, 0xff, 0xff, 0, 2, 0xff, 0xff, 3], quoted);
        let unquoted = super::unquote_null_bytes(quoted);
        assert_eq!(bytes, unquoted.into_iter().collect::<Vec<u8>>());
    }

    #[tokio::test]
    async fn test_compact() {
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let mut memtable = Memtable::new();
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");
        let key3 = Bytes::from("key3");

        memtable.put(Cow::Borrowed(&key1), Value::Data(Bytes::from("value1")));
        memtable.put(Cow::Borrowed(&key2), Value::Data(Bytes::from("value2")));
        memtable.put(Cow::Borrowed(&key3), Value::Data(Bytes::from("value3")));
        let sstable1 = SSTable::try_from_memtable(memtable, 1).unwrap();
        let mut memtable = Memtable::new();
        memtable.put(Cow::Borrowed(&key1), Value::Data(Bytes::from("value10")));
        memtable.put(Cow::Borrowed(&key2), Value::TombStone);
        let sstable2 = SSTable::try_from_memtable(memtable, 2).unwrap();
        let metadata_1 = sstable1.store_and_return_metadata(base_path).await.unwrap();
        let metadata_2 = sstable2.store_and_return_metadata(base_path).await.unwrap();
        let metadata = SSTable::compact(&[metadata_1, metadata_2], base_path, 1, 3)
            .await
            .unwrap();
        let mut reader = SSTable::key_value_reader(base_path, &metadata)
            .await
            .unwrap();
        let (key, value) = reader.read().await.unwrap().unwrap();
        assert_eq!(key, key1);
        assert_eq!(value, Value::Data(Bytes::from("value10")));
        let (key, value) = reader.read().await.unwrap().unwrap();
        assert_eq!(key, key3);
        assert_eq!(value, Value::Data(Bytes::from("value3")));

        assert_eq!(2, metadata.n_entries);
        assert_eq!(3, metadata.file_id);
    }
}
