use std::io::{ErrorKind, SeekFrom};

use bloomfilter::Bloom;
use bytes::{BufMut, Bytes, BytesMut};
use itertools::Itertools;
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
};

use crate::{
    memtable::{Key, Memtable, Value},
    types::UVarInt,
};

pub struct SSTable {
    metadata: SSTableMetadata,
    data: Vec<(Key, Value)>,
}

impl SSTable {
    pub fn try_from_memtable(
        memtable: Memtable,
        file_id: u64,
    ) -> Result<Self, ErrorCreatingSSTable> {
        let data = memtable.data();
        let mut bloom_filter = Bloom::new_for_fp_rate(data.len(), 0.01)
            .map_err(|err| ErrorCreatingSSTable::BloomFilterError(err))?;
        data.iter().for_each(|entry| bloom_filter.set(&entry.0));
        let metadata = SSTableMetadata {
            bloom_filter,
            level: 0,
            file_id,
        };
        Ok(Self { metadata, data })
    }

    pub async fn store_and_return_metadata(
        self,
        base_path: &str,
    ) -> Result<SSTableMetadata, std::io::Error> {
        let mut file = File::create(format!(
            "{base_path}/{}_{:08}.sst",
            self.metadata.level, self.metadata.file_id
        ))
        .await?;
        self.metadata.store(&mut file).await?;
        file.write_u8(0).await?;
        for (key, value) in self.data {
            let key = quote_null_bytes(key);
            let key_size = key.len();
            UVarInt::try_from(key_size)
                .map_err_into_other_error()?
                .write(&mut file)
                .await?;
            file.write_all(&key).await?;
            match value {
                Value::Data(value) => {
                    let value = quote_null_bytes(value);
                    UVarInt::try_from(value.len() + 2)
                        .map_err_into_other_error()?
                        .write(&mut file)
                        .await?;
                    file.write_all(&value).await?;
                }
                Value::TombStone => {
                    UVarInt::try_from(1)
                        .map_err_into_other_error()?
                        .write(&mut file)
                        .await?;
                }
            }
            file.write_u8(0).await?;
        }
        Ok(self.metadata)
    }

    pub async fn load_value(
        metadata: &SSTableMetadata,
        base_path: &str,
        key: &Key,
    ) -> Result<Option<Value>, std::io::Error> {
        let mut file = BufReader::new(
            File::open(format!(
                "{base_path}/{}_{:08}.sst",
                metadata.level, metadata.file_id
            ))
            .await?,
        );
        let bloom_filter_size: u64 = UVarInt::read(&mut file)
            .await?
            .try_into()
            .map_err_into_other_error()?;
        let metadata_size = bloom_filter_size + 9;
        let mut start = file.seek(SeekFrom::Current(metadata_size as i64)).await?;
        let mut end = file.seek(SeekFrom::End(0)).await?;
        'external: loop {
            let mut mid = (start + end) / 2;
            let initial_mid = mid;
            file.seek(SeekFrom::Start(mid)).await?;
            let mut old_bytes = None;
            'inner: loop {
                match file.read_u8().await {
                    Ok(0) if old_bytes != Some(0xff) => {
                        break 'inner;
                    }
                    Ok(byte) => {
                        old_bytes = Some(byte);
                        if mid == start {
                            return Ok(None);
                        }
                        mid += 1;
                        if mid >= end {
                            end = initial_mid;
                            continue 'external;
                        }
                    }
                    Err(err) if matches!(err.kind(), ErrorKind::UnexpectedEof) => {
                        end = mid;
                        continue 'external;
                    }
                    Err(err) => return Err(err),
                }
            }

            let key_size: usize = UVarInt::read(&mut file)
                .await?
                .try_into()
                .map_err_into_other_error()?;
            let loaded_key = {
                let mut key = vec![0u8; key_size];
                file.read_exact(&mut key).await?;
                unquote_null_bytes(key.into())
            };
            if loaded_key == *key {
                let value_size = UVarInt::read(&mut file).await?;
                if value_size.is_one() {
                    return Ok(Some(Value::TombStone));
                }
                let value_size: usize = value_size.try_into().map_err_into_other_error()?;
                let value_size = value_size - 2usize;
                let mut value = vec![0u8; value_size as usize];
                file.read_exact(&mut value).await?;
                let value = unquote_null_bytes(value.into());
                return Ok(Some(Value::Data(value)));
            }
            if loaded_key < *key {
                start = mid;
            } else {
                end = mid;
            }
        }
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

pub struct SSTableMetadata {
    bloom_filter: Bloom<Key>,
    level: u8,
    file_id: u64,
}

impl SSTableMetadata {
    pub fn level(&self) -> u8 {
        self.level
    }

    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    pub fn check(&self, key: &Key) -> bool {
        self.bloom_filter.check(&key)
    }

    async fn store(&self, file: &mut File) -> Result<(), std::io::Error> {
        let bloom_filter_slice = self.bloom_filter.as_slice();
        UVarInt::try_from(bloom_filter_slice.len())
            .map_err_into_other_error()?
            .write(file)
            .await?;
        file.write_all(bloom_filter_slice).await?;
        file.write_u8(self.level).await?;
        file.write_u64(self.file_id).await
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
