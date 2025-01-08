use std::io::{Cursor, ErrorKind, SeekFrom};

use bloomfilter::Bloom;
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
        let n_entries: u64 = self.data.len().try_into().map_err_into_other_error()?;
        file.write_u64(n_entries).await?;
        let mut buffer = Cursor::new(Vec::new());
        let mut value_cursor = 0;
        for (key, value) in self.data.iter() {
            let key_size = key.len();
            UVarInt::try_from(key_size)
                .map_err_into_other_error()?
                .write(&mut buffer)
                .await?;
            buffer.write_all(&key).await?;
            UVarInt::try_from(value_cursor)
                .map_err_into_other_error()?
                .write(&mut buffer)
                .await?;
            value_cursor += 4 + value.size()
        }
        let buffer = buffer.into_inner();
        let buffer_len: u64 = buffer.len().try_into().map_err_into_other_error()?;
        file.write_u64(buffer_len).await?;
        file.write_all(&buffer).await?;
        for (_, value) in self.data {
            match value {
                Value::Data(value) => {
                    file.write_u32(u32::try_from(value.len()).map_err_into_other_error()? + 1)
                        .await?;
                    file.write_all(&value).await?;
                }
                Value::TombStone => {
                    file.write_u32(0).await?;
                }
            }
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
        let metadata_size = dbg!(bloom_filter_size + 9);
        let pos = file.seek(SeekFrom::Current(metadata_size as i64)).await?;
        let mut n_entries = dbg!(file.read_u64().await?);
        let value_offset = dbg!(file.read_u64().await?);
        loop {
            let key_size: usize = UVarInt::read(&mut file)
                .await?
                .try_into()
                .map_err_into_other_error()?;
            let mut loaded_key = vec![0; key_size];
            file.read_exact(&mut loaded_key).await?;
            let offset: u64 = dbg!(UVarInt::read(&mut file)
                .await?
                .try_into()
                .map_err_into_other_error()?);
            if loaded_key == *key {
                file.seek(SeekFrom::Start(pos + value_offset + offset + 16))
                    .await?;
                let value_size = dbg!(file.read_u32().await?);
                if value_size == 0 {
                    return Ok(Some(Value::TombStone));
                }
                let mut value = vec![0; value_size as usize - 1];
                file.read_exact(&mut value).await?;
                return Ok(Some(Value::Data(value.into())));
            }
            n_entries -= 1;
            if n_entries == 0 {
                break;
            }
        }
        Ok(None)
    }
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
        UVarInt::try_from(dbg!(bloom_filter_slice.len()))
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
