use std::{
    io::{ErrorKind, SeekFrom},
    num::TryFromIntError,
    path::Path,
};

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
    MapErrIntoOtherError,
};

pub struct SSTable {
    metadata: SSTableMetadata,
    data: Vec<(Key, Value)>,
}

const BLOOM_FILTER_FP: f64 = 0.1;

impl SSTable {
    pub fn try_from_memtable(
        memtable: &Memtable,
        file_id: u64,
    ) -> Result<Self, ErrorCreatingSSTable> {
        let data = memtable.data();
        let mut bloom_filter = Bloom::new_for_fp_rate(data.len(), BLOOM_FILTER_FP)
            .map_err(ErrorCreatingSSTable::BloomFilterError)?;
        data.iter().for_each(|entry| bloom_filter.set(&entry.0));
        let metadata = SSTableMetadata {
            bloom_filter,
            level: 0,
            file_id,
            n_entries: data.len() as u64,
        };
        Ok(Self {
            metadata,
            data: data.to_vec(),
        })
    }

    pub async fn store_and_return_metadata(
        self,
        base_path: &str,
    ) -> Result<SSTableMetadata, SaveSStableError> {
        let mut keys_file = BufWriter::new(
            File::create(format!(
                "{base_path}/{}_{:08}.sst_keys",
                self.metadata.level, self.metadata.file_id
            ))
            .await
            .map_err(SaveSStableError::CreateKeysFile)?,
        );
        let mut values_file = BufWriter::new(
            File::create(format!(
                "{base_path}/{}_{:08}.sst_values",
                self.metadata.level, self.metadata.file_id
            ))
            .await
            .map_err(SaveSStableError::CreateValuesFile)?,
        );
        self.metadata
            .store(base_path)
            .await
            .map_err(SaveSStableError::SaveMetadata)?;
        let mut values_cursor: usize = 0;
        for (key, value) in self.data {
            keys_file
                .write_u8(0)
                .await
                .map_err(SaveSStableError::ErrorWritingSeparator)?;
            let key = quote_null_bytes(key);
            let key_size = key.len();
            Fixed28BitsInt::new(key_size.try_into()?)
                .write(&mut keys_file)
                .await
                .map_err(SaveSStableError::ErrorWritingKeySize)?;
            keys_file
                .write_all(&key)
                .await
                .map_err(SaveSStableError::ErrorWritingKey)?;
            Fixed56BitsInt::new(values_cursor.try_into()?)
                .write(&mut keys_file)
                .await
                .map_err(SaveSStableError::ErrorWritingValueCursor)?;
            values_cursor += value
                .write(&mut values_file)
                .await
                .map_err(SaveSStableError::ErrorWritingValue)?;
        }
        keys_file
            .flush()
            .await
            .map_err(SaveSStableError::ErrorFlushinKeysFile)?;
        values_file
            .flush()
            .await
            .map_err(SaveSStableError::ErrorFlushinValuesFile)?;
        Ok(self.metadata)
    }

    pub async fn load_value(
        metadata: &SSTableMetadata,
        base_path: &str,
        key: &Key,
    ) -> Result<Option<Value>, LoadSStableValueError> {
        let mut keys_file = BufReader::new(
            File::open(format!(
                "{base_path}/{}_{:08}.sst_keys",
                metadata.level, metadata.file_id
            ))
            .await
            .map_err(LoadSStableValueError::OpenKeysFile)?,
        );
        let mut start = 0;
        let mut end = keys_file
            .seek(SeekFrom::End(0))
            .await
            .map_err(LoadSStableValueError::SeekToEnd)?;
        'external: loop {
            let mut mid = (start + end) / 2;
            let initial_mid = mid;
            let pos = keys_file
                .seek(SeekFrom::Start(mid))
                .await
                .map_err(|err| LoadSStableValueError::SeekToPosition(mid, err))?;
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
                        if mid > end {
                            end = initial_mid;
                            continue 'external;
                        }
                        mid += 1;
                    }
                    Err(err) if matches!(err.kind(), ErrorKind::UnexpectedEof) => {
                        end = initial_mid;
                        continue 'external;
                    }
                    Err(err) => return Err(LoadSStableValueError::ReadByte(err)),
                }
            }

            let key_size: usize = Fixed28BitsInt::read(&mut keys_file)
                .await
                .map_err(LoadSStableValueError::ReadKeySize)?
                .value()
                .try_into()?;
            let loaded_key = {
                let mut key = vec![0u8; key_size];
                keys_file
                    .read_exact(&mut key)
                    .await
                    .map_err(LoadSStableValueError::ReadKey)?;
                unquote_null_bytes(key.into())
            };

            if loaded_key == *key {
                let value_pos: usize = Fixed56BitsInt::read(&mut keys_file)
                    .await
                    .map_err(LoadSStableValueError::ReadValueCursor)?
                    .value()
                    .try_into()?;
                let mut values_file = BufReader::new(
                    File::open(format!(
                        "{base_path}/{}_{:08}.sst_values",
                        metadata.level, metadata.file_id
                    ))
                    .await
                    .map_err(LoadSStableValueError::OpenValuesFile)?,
                );
                let value_pos: u64 = value_pos.try_into()?;
                values_file
                    .seek(SeekFrom::Start(value_pos))
                    .await
                    .map_err(|err| {
                        LoadSStableValueError::SeekToPositionValuesFile(value_pos, err)
                    })?;
                return Ok(Some(
                    Value::read(&mut values_file)
                        .await
                        .map_err(LoadSStableValueError::ReadValue)?,
                ));
            }
            if start == mid {
                return Ok(None);
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
        let mut readers = Vec::new();
        for metadata in metadatas.iter().rev() {
            let reader = Self::key_value_reader(base_path, metadata)
                .await
                .map_err(|err| SSTableCompactError::CreateKeyValueReader(metadata.clone(), err))?;
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
                .await
                .map_err(SSTableCompactError::CreateKeysFile)?,
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
                .await
                .map_err(SSTableCompactError::CreateValuesFile)?,
        );

        let mut n_entries = 0;
        let mut values_cursor = 0;
        loop {
            let mut min_key: Option<Key> = None;
            let mut min_value = None;
            for reader in readers.iter_mut() {
                if let Some((key, value)) = reader
                    .read_without_consuming()
                    .await
                    .map_err(SSTableCompactError::ReadKeyValuePair)?
                {
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
                    if let Some((reader_key, _)) = reader
                        .read_without_consuming()
                        .await
                        .map_err(SSTableCompactError::ReadKeyValuePair)?
                    {
                        if reader_key == &key {
                            reader.consume();
                        }
                    }
                }
                continue;
            }
            n_entries += 1;
            keys_file
                .write_u8(0)
                .await
                .map_err(SSTableCompactError::WriteSeparator)?;
            let quoted_key = quote_null_bytes(key.clone());
            Fixed28BitsInt::new(quoted_key.len().try_into()?)
                .write(&mut keys_file)
                .await
                .map_err(SSTableCompactError::WriteKeySize)?;
            keys_file
                .write_all(&quoted_key)
                .await
                .map_err(SSTableCompactError::WriteKey)?;
            Fixed56BitsInt::new(values_cursor.try_into()?)
                .write(&mut keys_file)
                .await
                .map_err(SSTableCompactError::WriteValueCursor)?;
            values_cursor += value
                .write(&mut values_file)
                .await
                .map_err(SSTableCompactError::WriteValue)?;

            for reader in readers.iter_mut() {
                if let Some((reader_key, _)) = reader
                    .read_without_consuming()
                    .await
                    .map_err(SSTableCompactError::ReadKeyValuePair)?
                {
                    if reader_key == &key {
                        reader.consume();
                    }
                }
            }
        }
        keys_file
            .flush()
            .await
            .map_err(SSTableCompactError::FlushKeysFile)?;
        values_file
            .flush()
            .await
            .map_err(SSTableCompactError::FlushValuesFile)?;
        let mut bloom_filter = Bloom::new_for_fp_rate(n_entries, BLOOM_FILTER_FP)
            .map_err(ErrorCreatingSSTable::BloomFilterError)?;
        {
            let mut file = keys_file.into_inner();
            file.seek(SeekFrom::Start(0))
                .await
                .map_err(SSTableCompactError::SeekToKeysStart)?;
            let mut reader = KeyReader {
                keys_reader: BufReader::new(file),
                n_entries: n_entries as u64,
                read_entries: 0,
            };
            while let Some(key) = reader.read().await.map_err(SSTableCompactError::ReadKey)? {
                bloom_filter.set(&key);
            }
        }
        let metadata = SSTableMetadata {
            bloom_filter,
            level: new_level,
            file_id: new_file_id,
            n_entries: n_entries as u64,
        };
        metadata
            .store(base_path)
            .await
            .map_err(SSTableCompactError::SaveMetadata)?;
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

#[derive(Debug, Error)]
pub enum SaveSStableError {
    #[error("Error creating keys file: {0}")]
    CreateKeysFile(std::io::Error),
    #[error("Error creating values file: {0}")]
    CreateValuesFile(std::io::Error),
    #[error("Error saving metadata: {0}")]
    SaveMetadata(std::io::Error),
    #[error("Error writing separator: {0}")]
    ErrorWritingSeparator(std::io::Error),
    #[error("Error writing key size: {0}")]
    ErrorWritingKeySize(std::io::Error),
    #[error("Error writing key: {0}")]
    ErrorWritingKey(std::io::Error),
    #[error("Error writing value cursor: {0}")]
    ErrorWritingValueCursor(std::io::Error),
    #[error("Error writing value: {0}")]
    ErrorWritingValue(std::io::Error),
    #[error("Error flushing keys file: {0}")]
    ErrorFlushinKeysFile(std::io::Error),
    #[error("Error flushing values file: {0}")]
    ErrorFlushinValuesFile(std::io::Error),
    #[error(transparent)]
    ConvertError(#[from] TryFromIntError),
}

#[derive(Debug, Error)]
pub enum LoadSStableValueError {
    #[error("Error opening keys file: {0}")]
    OpenKeysFile(std::io::Error),
    #[error("Error seeking to the end of keys file: {0}")]
    SeekToEnd(std::io::Error),
    #[error("Error seeking to position {0}: {1}")]
    SeekToPosition(u64, std::io::Error),
    #[error("Error seeking to position {0} in values file: {1}")]
    SeekToPositionValuesFile(u64, std::io::Error),
    #[error("Error reading byte during search: {0}")]
    ReadByte(std::io::Error),
    #[error("Error reading key size: {0}")]
    ReadKeySize(std::io::Error),
    #[error("Error reading key: {0}")]
    ReadKey(std::io::Error),
    #[error("Error reading value cursor: {0}")]
    ReadValueCursor(std::io::Error),
    #[error("Error opening values file: {0}")]
    OpenValuesFile(std::io::Error),
    #[error("Error reading value: {0}")]
    ReadValue(std::io::Error),
    #[error(transparent)]
    ConvertError(#[from] TryFromIntError),
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
        Ok(Some((key, Value::read(&mut self.value_reder).await?)))
    }
}

#[derive(Debug)]
struct KeyReader<R> {
    pub keys_reader: R,
    pub n_entries: u64,
    pub read_entries: u64,
}

impl<R> KeyReader<R>
where
    R: AsyncRead + Unpin,
{
    pub async fn read(&mut self) -> Result<Option<Key>, std::io::Error> {
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
    #[error("Error creating key value reader for metadata {0:?}: {1}")]
    CreateKeyValueReader(SSTableMetadata, std::io::Error),
    #[error("Error creating keys file: {0}")]
    CreateKeysFile(std::io::Error),
    #[error("Error creating values file: {0}")]
    CreateValuesFile(std::io::Error),
    #[error("Error reading key value pair: {0}")]
    ReadKeyValuePair(std::io::Error),
    #[error("Error writing separator: {0}")]
    WriteSeparator(std::io::Error),
    #[error("Error writing key size: {0}")]
    WriteKeySize(std::io::Error),
    #[error("Error writing key: {0}")]
    WriteKey(std::io::Error),
    #[error("Error writing value cursor: {0}")]
    WriteValueCursor(std::io::Error),
    #[error("Error writing value: {0}")]
    WriteValue(std::io::Error),
    #[error("Error flushing keys file: {0}")]
    FlushKeysFile(std::io::Error),
    #[error("Error flushing values file: {0}")]
    FlushValuesFile(std::io::Error),
    #[error("Error seeking to keys file start: {0}")]
    SeekToKeysStart(std::io::Error),
    #[error("Error reading key: {0}")]
    ReadKey(std::io::Error),
    #[error("Error saveing metadata file: {0}")]
    SaveMetadata(std::io::Error),
    #[error(transparent)]
    ErrorCreatingSSTable(#[from] ErrorCreatingSSTable),
    #[error(transparent)]
    ConvertError(#[from] TryFromIntError),
}

#[derive(Debug, Clone)]
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

    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    pub fn check(&self, key: &Key) -> bool {
        self.bloom_filter.check(key)
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
        file.write_u64(self.n_entries).await?;
        file.flush().await
    }

    pub async fn read_from_file(file: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let mut file = BufReader::new(File::open(file).await?);
        let bloom_filter_size = UVarInt::read(&mut file).await?;
        let bloom_filter_size: usize = bloom_filter_size.try_into().map_err_into_other_error()?;
        let mut bloom_filter = vec![0u8; bloom_filter_size];
        file.read_exact(&mut bloom_filter).await?;
        let bloom_filter = Bloom::from_slice(&bloom_filter).map_err_into_other_error()?;
        let level = file.read_u8().await?;
        let file_id = file.read_u64().await?;
        let n_entries = file.read_u64().await?;
        Ok(Self {
            bloom_filter,
            level,
            file_id,
            n_entries,
        })
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
        let sstable1 = SSTable::try_from_memtable(&memtable, 1).unwrap();
        let mut memtable = Memtable::new();
        memtable.put(Cow::Borrowed(&key1), Value::Data(Bytes::from("value10")));
        memtable.put(Cow::Borrowed(&key2), Value::TombStone);
        let sstable2 = SSTable::try_from_memtable(&memtable, 2).unwrap();
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
