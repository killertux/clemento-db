use std::{borrow::Cow, collections::HashMap};

use bytes::Bytes;
use memtable::{Memtable, Value};
use sstable::{ErrorCreatingSSTable, SSTable, SSTableCompactError, SSTableMetadata};
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
    max_level: u8,
}

impl Default for LSM {
    fn default() -> Self {
        Self::new(1 * 1024 * 1024, 16, "./".to_string())
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
            max_level: 0,
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
                for level in 0..=self.max_level {
                    if let Some(metadatas) = self.sstable_metadatas.get(&level) {
                        for metadata in metadatas.iter().rev() {
                            if metadata.check(&key) {
                                match SSTable::load_value(&metadata, &self.base_path, &key).await? {
                                    Some(Value::TombStone) => return Ok(None),
                                    Some(Value::Data(value)) => {
                                        return Ok(Some(Cow::Owned(value)));
                                    }
                                    None => continue,
                                }
                            }
                        }
                    } else {
                        continue;
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
            self.store_metadata_and_compact_if_necessary(metadata)
                .await?;
        }
        Ok(())
    }

    async fn store_metadata_and_compact_if_necessary(
        &mut self,
        metadata: SSTableMetadata,
    ) -> Result<(), LsmError> {
        let level = metadata.level();
        self.sstable_metadatas
            .entry(metadata.level())
            .or_insert_with(Vec::new)
            .push(metadata);
        if self.sstable_metadatas[&level].len() > self.max_sstables_per_level {
            Box::pin(self.compact(level)).await?;
        }
        Ok(())
    }

    async fn compact(&mut self, level: u8) -> Result<(), LsmError> {
        let metadatas = self
            .sstable_metadatas
            .remove(&level)
            .expect("We know that there are metadatas here");
        let new_level = level + 1;
        self.max_level = self.max_level.max(new_level);
        let metadata =
            SSTable::compact(&metadatas, &self.base_path, new_level, self.n_sstables).await?;
        self.n_sstables += 1;
        self.store_metadata_and_compact_if_necessary(metadata)
            .await?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum LsmError {
    #[error(transparent)]
    ErrorCreatingSSTable(#[from] ErrorCreatingSSTable),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    SSTableCompactError(#[from] SSTableCompactError),
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

    #[tokio::test]
    async fn test_more_than_memtable_limit_with_deletions_and_nulls() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut lsm = LSM::new(18, 10, temp_dir.path().to_string_lossy().to_string());
        let key_1 = Bytes::from("key 1");
        let key_2 = Bytes::from([0u8, 0xff, 0].as_slice());
        let key_3 = Bytes::from("key 3");
        let value_1 = Bytes::from("v1");
        let value_2 = Bytes::from("v2");
        let value_3 = Bytes::from([1, 0u8, 0xff, 0xff, 0u8].as_slice());
        lsm.put(Cow::Borrowed(&key_1), value_1.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_2), value_2.clone())
            .await
            .unwrap();
        lsm.delete(Cow::Borrowed(&key_2)).await.unwrap();
        lsm.put(Cow::Borrowed(&key_3), value_3.clone())
            .await
            .unwrap();
        assert_eq!(lsm.memtable.size(), 0);
        assert_eq!(
            lsm.get(&key_1).await.unwrap().unwrap().into_owned(),
            value_1
        );
        assert_eq!(lsm.get(&key_2).await.unwrap(), None);
        assert_eq!(
            lsm.get(&key_3).await.unwrap().unwrap().into_owned(),
            value_3
        );
    }

    #[tokio::test]
    async fn test_multiple_sstables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut lsm = LSM::new(20, 10, temp_dir.path().to_string_lossy().to_string());
        let key_1 = Bytes::from("key 1");
        let key_2 = Bytes::from("key 2");
        let key_3 = Bytes::from("key 3");
        let key_4 = Bytes::from("key 4");
        let value_1 = Bytes::from("v1");
        let value_2 = Bytes::from("v2");
        let value_3 = Bytes::from("v3");
        let value_4 = Bytes::from("v4");

        lsm.put(Cow::Borrowed(&key_1), value_1.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_2), value_2.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_3), value_3.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_4), value_4.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_2), value_3.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_3), value_2.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_4), value_1.clone())
            .await
            .unwrap();

        assert_eq!(lsm.sstable_metadatas[&0].len(), 2);
        assert_eq!(lsm.n_sstables, 2);
        assert_eq!(
            lsm.get(&key_1).await.unwrap().unwrap().into_owned(),
            value_1
        );
        assert_eq!(
            lsm.get(&key_2).await.unwrap().unwrap().into_owned(),
            value_3
        );
        assert_eq!(
            lsm.get(&key_3).await.unwrap().unwrap().into_owned(),
            value_2
        );
        assert_eq!(
            lsm.get(&key_4).await.unwrap().unwrap().into_owned(),
            value_1
        );
    }

    #[tokio::test]
    async fn test_multiple_levels() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut lsm = LSM::new(10, 2, temp_dir.path().to_string_lossy().to_string());
        let key_1 = Bytes::from("key 1");
        let key_2 = Bytes::from("key 2");
        let key_3 = Bytes::from("key 3");
        let key_4 = Bytes::from("key 4");
        let key_5 = Bytes::from("key 5");
        let value_1 = Bytes::from("v1");
        let value_2 = Bytes::from("v2");
        let value_3 = Bytes::from("v3");
        let value_4 = Bytes::from("v4");
        let value_5 = Bytes::from("v5");

        lsm.put(Cow::Borrowed(&key_1), value_1.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_2), value_2.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_3), value_3.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_4), value_4.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_5), value_5.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_2), value_3.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_3), value_2.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_4), value_1.clone())
            .await
            .unwrap();

        assert_eq!(lsm.sstable_metadatas[&0].len(), 1);
        assert_eq!(lsm.sstable_metadatas[&1].len(), 1);
        assert_eq!(lsm.n_sstables, 5);
        assert_eq!(
            lsm.get(&key_1).await.unwrap().unwrap().into_owned(),
            value_1
        );
        assert_eq!(
            lsm.get(&key_2).await.unwrap().unwrap().into_owned(),
            value_3
        );
        assert_eq!(
            lsm.get(&key_3).await.unwrap().unwrap().into_owned(),
            value_2
        );
        assert_eq!(
            lsm.get(&key_4).await.unwrap().unwrap().into_owned(),
            value_1
        );
        assert_eq!(
            lsm.get(&key_5).await.unwrap().unwrap().into_owned(),
            value_5
        );
    }
}
