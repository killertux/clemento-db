use std::{borrow::Cow, collections::HashMap};

use bytes::Bytes;
use memtable::{Memtable, Value};
use sstable::{ErrorCreatingSSTable, SSTable, SSTableMetadata};
use thiserror::Error;

mod memtable;
mod sstable;
mod types;

pub struct LSM {
    max_sstables_per_level: usize,
    max_memtable_size: usize,
    memtable: Memtable,
    sstable_metadatas: HashMap<u8, Vec<SSTableMetadata>>,
    n_sstables: u64,
    base_path: String,
}

impl Default for LSM {
    fn default() -> Self {
        Self::new(64 * 1024 * 1024, 16, ".".to_string())
    }
}

impl LSM {
    pub fn new(max_memtable_size: usize, max_sstables_per_level: usize, base_path: String) -> Self {
        Self {
            max_memtable_size,
            max_sstables_per_level,
            memtable: Memtable::new(),
            sstable_metadatas: HashMap::new(),
            n_sstables: 0,
            base_path,
        }
    }

    pub async fn put(&mut self, key: Cow<'_, Bytes>, value: Bytes) -> Result<(), LsmError> {
        let value = Value::Data(value);
        self.internal_put(key, value).await
    }

    pub async fn get(&self, key: &Bytes) -> Result<Option<Cow<'_, Bytes>>, LsmError> {
        match self.memtable.get(&key) {
            Some(Value::Data(value)) => Ok(Some(Cow::Borrowed(value))),
            Some(Value::TombStone) => Ok(None),
            None => {
                for level in 0.. {
                    if let Some(metadatas) = self.sstable_metadatas.get(&level) {
                        for metadata in metadatas {
                            if metadata.check(&key) {
                                return Ok(SSTable::load_value(&metadata, &self.base_path, &key)
                                    .await?
                                    .and_then(|value| match value {
                                        Value::Data(value) => Some(Cow::Owned(value)),
                                        Value::TombStone => None,
                                    }));
                            }
                        }
                    } else {
                        break;
                    }
                }
                Ok(None)
            }
        }
    }

    pub async fn delete(&mut self, key: Cow<'_, Bytes>) -> Result<(), LsmError> {
        let value = Value::TombStone;
        self.internal_put(key, value).await
    }

    async fn internal_put(&mut self, key: Cow<'_, Bytes>, value: Value) -> Result<(), LsmError> {
        self.memtable.put(key, value);
        if self.memtable.size() >= self.max_memtable_size {
            let memtable = std::mem::replace(&mut self.memtable, Memtable::new());
            let sstable = SSTable::try_from_memtable(memtable, self.n_sstables)?;
            self.n_sstables += 1;
            let metadata = sstable.store_and_return_metadata(&self.base_path).await?;
            let entry = self
                .sstable_metadatas
                .entry(metadata.level())
                .or_insert_with(Vec::new);
            match entry.binary_search_by(|e| metadata.file_id().cmp(&e.file_id())) {
                Ok(index) => entry.insert(index, metadata),
                Err(index) => entry.insert(index, metadata),
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum LsmError {
    #[error(transparent)]
    ErrorCreatingSSTable(#[from] ErrorCreatingSSTable),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_non_existent() {
        let lsm = LSM::default();
        let key = Bytes::from("key");
        assert_eq!(lsm.get(&key).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_put_get() {
        let mut lsm = LSM::default();
        let key = Bytes::from("key");
        let value = Bytes::from("value");
        lsm.put(Cow::Borrowed(&key), value.clone()).await.unwrap();
        assert_eq!(lsm.get(&key).await.unwrap().unwrap().into_owned(), value);
    }

    #[tokio::test]
    async fn test_put_existent_should_overwrite() {
        let mut lsm = LSM::default();
        let key = Bytes::from("key");
        let value_1 = Bytes::from("value 1");
        let value_2 = Bytes::from("value 2");
        lsm.put(Cow::Borrowed(&key), value_1.clone()).await.unwrap();
        lsm.put(Cow::Borrowed(&key), value_2.clone()).await.unwrap();
        assert_eq!(lsm.get(&key).await.unwrap().unwrap().into_owned(), value_2);
    }

    #[tokio::test]
    async fn test_put_delete_get() {
        let mut lsm = LSM::default();
        let key = Bytes::from("key");
        let value = Bytes::from("value");
        lsm.put(Cow::Borrowed(&key), value.clone()).await.unwrap();
        lsm.delete(Cow::Borrowed(&key)).await.unwrap();
        assert_eq!(lsm.get(&key).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_put_multiples() {
        let mut lsm = LSM::default();
        let key_1 = Bytes::from("key 1");
        let key_2 = Bytes::from("key 2");
        let key_3 = Bytes::from("key 3");
        let value_1 = Bytes::from("value 1");
        let value_2 = Bytes::from("value 2");
        let value_3 = Bytes::from("value 3");
        lsm.put(Cow::Borrowed(&key_1), value_1.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_2), value_2.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_3), value_3.clone())
            .await
            .unwrap();
        assert_eq!(
            lsm.get(&key_1).await.unwrap().unwrap().into_owned(),
            value_1
        );
        assert_eq!(
            lsm.get(&key_2).await.unwrap().unwrap().into_owned(),
            value_2
        );
        assert_eq!(
            lsm.get(&key_3).await.unwrap().unwrap().into_owned(),
            value_3
        );
    }

    #[tokio::test]
    async fn test_more_than_memtable_limit() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut lsm = LSM::new(14, 10, temp_dir.path().to_string_lossy().to_string());
        let key_1 = Bytes::from("key 1");
        let key_2 = Bytes::from("key 2");
        let key_3 = Bytes::from("key 3");
        let value_1 = Bytes::from("v1");
        let value_2 = Bytes::from("v2");
        let value_3 = Bytes::from("v3");
        lsm.put(Cow::Borrowed(&key_1), value_1.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_2), value_2.clone())
            .await
            .unwrap();
        assert_eq!(lsm.memtable.size(), 0);
        lsm.put(Cow::Borrowed(&key_3), value_3.clone())
            .await
            .unwrap();
        assert_eq!(lsm.memtable.size(), 7);
        assert_eq!(
            lsm.get(&key_1).await.unwrap().unwrap().into_owned(),
            value_1
        );
        assert_eq!(
            lsm.get(&key_2).await.unwrap().unwrap().into_owned(),
            value_2
        );
        assert_eq!(
            lsm.get(&key_3).await.unwrap().unwrap().into_owned(),
            value_3
        );
    }
}
