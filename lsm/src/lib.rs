use std::{borrow::Cow, collections::HashMap, io::ErrorKind, sync::Arc};

use bytes::Bytes;
use flock::Flock;
use memtable::{Memtable, ReadMemtableError, Value, WriteMemtableError};
use sstable::{LoadSStableValueError, SSTableCompactError, SSTableMetadata, SaveSStableError};
use thiserror::Error;

use tokio::{
    fs::{read_dir, File},
    io::{AsyncWriteExt, BufWriter},
    sync::{
        mpsc::{channel, error::SendError, Receiver, Sender},
        Mutex, RwLock,
    },
    task::{JoinError, JoinHandle},
};

mod flock;
mod memtable;
mod sstable;
mod types;

type JoinHandleGuard = Arc<Mutex<Option<JoinHandle<Result<(), LsmError>>>>>;
#[derive(Debug, Clone)]
pub struct LSM {
    max_memtable_size: usize,
    base_path: String,
    worker_tx: Sender<WorkerCommands>,
    join_handle: JoinHandleGuard,
    inner: Arc<RwLock<InnerLSM>>,
    temp_memtable: Arc<RwLock<Option<Memtable>>>,
}

#[derive(Debug)]
struct InnerLSM {
    memtable: Memtable,
    sstable_metadatas: HashMap<u8, Vec<SSTableMetadata>>,
    n_sstables: u64,
    max_level: u8,
    flock: Option<Flock>,
}

impl LSM {
    pub async fn default(base_path: String) -> Result<Self, LsmError> {
        Self::new(16 * 1024 * 1024, 16, base_path).await
    }

    pub async fn new(
        max_memtable_size: usize,
        max_sstables_per_level: usize,
        base_path: String,
    ) -> Result<Self, LsmError> {
        let flock = Flock::new(&base_path).await?;
        let mut read_dir = read_dir(&base_path).await?;
        let mut metadatas = Vec::new();
        let mut memtable = None;
        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                let file_name = path.file_name().and_then(|f| f.to_str());
                if file_name
                    .map(|file_name| file_name.ends_with(".sst_metadata"))
                    .unwrap_or(false)
                {
                    tracing::debug!("Reading file {} as sst metadata", path.display());
                    let metadata = SSTableMetadata::read_from_file(path).await?;
                    metadatas.push(metadata);
                } else if file_name == Some("memtable") {
                    tracing::debug!("Reading file {} as memtable", path.display());
                    let mut file = File::open(path).await?;
                    memtable = Some(Memtable::read(&mut file).await?);
                }
            }
        }
        let mut n_sstables = 0;
        let mut max_level = 0;
        let mut sstable_metadatas = HashMap::new();
        metadatas.sort_by_key(|a| a.file_id());
        for metadata in metadatas {
            n_sstables += 1;
            max_level = max_level.max(metadata.level());
            sstable_metadatas
                .entry(metadata.level())
                .or_insert_with(Vec::new)
                .push(metadata);
        }
        tracing::debug!(
            "Memtable size {}",
            memtable
                .as_ref()
                .map(|memtable| memtable.size())
                .unwrap_or(0)
        );
        tracing::debug!("SSTables metadatas {sstable_metadatas:?}");
        tracing::debug!("n_sstables {n_sstables}");
        tracing::debug!("max_level {max_level}");

        let inner = Arc::new(RwLock::new(InnerLSM {
            memtable: memtable.unwrap_or_else(Memtable::new),
            sstable_metadatas,
            n_sstables,
            max_level,
            flock: Some(flock),
        }));
        let temp_memtable = Arc::new(RwLock::new(None));

        let (tx, rx) = channel(1024);
        let join_handle = tokio::spawn(Self::worker(
            rx,
            tx.clone(),
            base_path.clone(),
            max_sstables_per_level,
            temp_memtable.clone(),
            inner.clone(),
        ));

        Ok(Self {
            max_memtable_size,
            base_path,
            join_handle: Arc::new(Mutex::new(Some(join_handle))),
            worker_tx: tx,
            inner,
            temp_memtable,
        })
    }

    pub async fn put(&mut self, key: Cow<'_, Bytes>, value: Bytes) -> Result<(), LsmError> {
        tracing::debug!("Putting key {:?}", key);
        let value = Value::Data(value);
        self.internal_put(key, value).await
    }

    pub async fn get(&self, key: &Bytes) -> Result<Option<Bytes>, LsmError> {
        tracing::debug!("Getting key {:?}", key);
        let result = {
            let temp_memtable = self.temp_memtable.read().await;
            temp_memtable.as_ref().and_then(|memtable| {
                tracing::debug!("Searching for key in temp memtable");
                memtable.get(key).cloned()
            })
        };

        match result {
            Some(Value::Data(value)) => Ok(Some(value)),
            Some(Value::TombStone) => Ok(None),
            None => {
                let inner = self.inner.read().await;
                match inner.memtable.get(key) {
                    Some(Value::Data(value)) => Ok(Some(value.clone())),
                    Some(Value::TombStone) => Ok(None),
                    None => {
                        for level in 0..=inner.max_level {
                            if let Some(metadatas) = inner.sstable_metadatas.get(&level) {
                                for metadata in metadatas.iter().rev() {
                                    if metadata.check(key) {
                                        tracing::debug!("BloomFilter positive for key {key:?} in sstable {metadata:?}");
                                        match SSTableMetadata::load_value(
                                            metadata,
                                            &self.base_path,
                                            key,
                                        )
                                        .await
                                        .map_err(
                                            |err| LsmError::LoadSStableValueError(key.clone(), err),
                                        )? {
                                            Some(Value::TombStone) => return Ok(None),
                                            Some(Value::Data(value)) => {
                                                return Ok(Some(value));
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
        }
    }

    pub async fn delete(&mut self, key: Cow<'_, Bytes>) -> Result<(), LsmError> {
        tracing::debug!("Deleting key {:?}", key);
        let value = Value::TombStone;
        self.internal_put(key, value).await
    }

    pub async fn close(self) -> Result<(), LsmError> {
        tracing::debug!("Closing LSM");
        let _ = self.worker_tx.send(WorkerCommands::Stop).await;
        let join_handle = self.join_handle.lock().await.take();
        match join_handle {
            Some(join_handle) => join_handle.await?,
            None => Ok(()),
        }?;
        let base_path = self.base_path.clone();
        let mut inner = self.inner.write().await;
        let memtable = std::mem::replace(&mut inner.memtable, Memtable::new());
        Self::write_memtable(memtable, base_path).await?;
        Ok(match inner.flock.take() {
            Some(flock) => flock.unlock().await?,
            None => (),
        })
    }

    async fn write_memtable(memtable: Memtable, base_path: String) -> Result<(), LsmError> {
        tracing::debug!("Writing memtable to disk");
        if memtable.size() == 0 {
            return Ok(());
        }
        let mut file = BufWriter::new(File::create(format!("{}/memtable", base_path)).await?);
        memtable.write(&mut file).await?;
        file.flush().await?;
        Ok(())
    }

    async fn internal_put(&self, key: Cow<'_, Bytes>, value: Value) -> Result<(), LsmError> {
        let mut temp_memtable = self.temp_memtable.write().await;
        match temp_memtable.as_mut() {
            Some(temp_memtable) => {
                temp_memtable.put(key, value);
            }
            None => {
                let mut inner = self.inner.write().await;
                inner.memtable.put(key, value);
                if inner.memtable.size() >= self.max_memtable_size {
                    *temp_memtable = Some(Memtable::new());
                    self.worker_tx.send(WorkerCommands::WriteMemtable).await?;
                }
            }
        }
        Ok(())
    }

    async fn worker(
        mut rx: Receiver<WorkerCommands>,
        tx: Sender<WorkerCommands>,
        base_path: String,
        max_sstables_per_level: usize,
        temp_memtable: Arc<RwLock<Option<Memtable>>>,
        inner: Arc<RwLock<InnerLSM>>,
    ) -> Result<(), LsmError> {
        while let Some(command) = rx.recv().await {
            match command {
                WorkerCommands::WriteMemtable => {
                    tracing::debug!("Writing memtable to disk");
                    let (metadata, level) = {
                        let inner = inner.read().await;
                        let metadata = SSTableMetadata::write_from_memtable(
                            &inner.memtable,
                            &base_path,
                            inner.n_sstables,
                        )
                        .await?;
                        let level = metadata.level();
                        (metadata, level)
                    };
                    let mut inner = inner.write().await;
                    let temp_memtable = temp_memtable.write().await.take();
                    inner.memtable = temp_memtable.unwrap_or_else(Memtable::new);
                    inner.n_sstables += 1;
                    inner
                        .sstable_metadatas
                        .entry(metadata.level())
                        .or_default()
                        .push(metadata);
                    tx.send(WorkerCommands::CheckCompact(level)).await?;
                }
                WorkerCommands::CheckCompact(level) => {
                    let new_level = level + 1;
                    let result = {
                        let inner = inner.read().await;
                        if inner.sstable_metadatas[&level].len() > max_sstables_per_level {
                            let metadatas = inner
                                .sstable_metadatas
                                .get(&level)
                                .cloned()
                                .expect("We know that there are metadatas here");

                            Some((metadatas, inner.n_sstables))
                        } else {
                            None
                        }
                    };
                    let result = {
                        if let Some((metadatas, n_sstables)) = result {
                            tracing::debug!("Running compact for level {new_level}");
                            let metadata = SSTableMetadata::compact(
                                &metadatas, &base_path, new_level, n_sstables,
                            )
                            .await?;
                            let mut paths_to_delete = Vec::new();
                            for metadata in metadatas {
                                paths_to_delete.push(format!(
                                    "{base_path}/{}_{:08}.sst_keys",
                                    metadata.level(),
                                    metadata.file_id()
                                ));
                                paths_to_delete.push(format!(
                                    "{base_path}/{}_{:08}.sst_values",
                                    metadata.level(),
                                    metadata.file_id()
                                ));
                                paths_to_delete.push(format!(
                                    "{base_path}/{}_{:08}.sst_metadata",
                                    metadata.level(),
                                    metadata.file_id()
                                ));
                            }
                            Some((paths_to_delete, metadata))
                        } else {
                            None
                        }
                    };
                    if let Some((paths_to_delete, metadata)) = result {
                        let mut inner = inner.write().await;
                        inner.max_level = inner.max_level.max(new_level);
                        inner.n_sstables += 1;
                        inner.sstable_metadatas.remove(&level);
                        inner
                            .sstable_metadatas
                            .entry(metadata.level())
                            .or_default()
                            .push(metadata);
                        tracing::debug!("Compact done. Scheduling delete and new checks");
                        tx.send(WorkerCommands::DeletePaths(paths_to_delete))
                            .await?;
                        tx.send(WorkerCommands::CheckCompact(new_level)).await?;
                        tracing::debug!("Compact done");
                    }
                }
                WorkerCommands::DeletePaths(paths_to_delete) => {
                    for path in paths_to_delete {
                        tokio::fs::remove_file(path).await?;
                    }
                }
                WorkerCommands::Stop => {
                    rx.close();
                    break;
                }
                #[cfg(test)]
                WorkerCommands::Await(tx) => {
                    tx.send(()).unwrap();
                }
            }
        }
        Ok(())
    }
}

pub enum WorkerCommands {
    WriteMemtable,
    CheckCompact(u8),
    DeletePaths(Vec<String>),
    Stop,
    #[cfg(test)]
    Await(tokio::sync::oneshot::Sender<()>),
}

impl WorkerCommands {
    #[cfg(test)]
    async fn await_worker(tx: &Sender<WorkerCommands>) {
        let (oneshoot_tx, oneshoot_rx) = tokio::sync::oneshot::channel();
        tx.send(WorkerCommands::Await(oneshoot_tx)).await.unwrap();
        oneshoot_rx.await.unwrap();
    }
}

impl Drop for LSM {
    fn drop(&mut self) {
        let inner = self.inner.clone();
        let worker_tx = self.worker_tx.clone();
        let join_handle = self.join_handle.clone();
        let base_path = self.base_path.clone();
        tokio::spawn(async move {
            let _ = worker_tx.send(WorkerCommands::Stop).await;
            let join_handle = join_handle.lock().await.take();
            match join_handle {
                Some(join_handle) => join_handle.await.expect("Error joining worker"),
                None => Ok(()),
            }
            .expect("Error closing worker");
            let mut inner = inner.write().await;
            let base_path = base_path.clone();
            let memtable = std::mem::replace(&mut inner.memtable, Memtable::new());
            Self::write_memtable(memtable, base_path)
                .await
                .expect("Error writing memtable");
            match inner.flock.take() {
                Some(flock) => flock.unlock().await.expect("Error unlocking file lock"),
                None => (),
            }
        });
    }
}

pub(crate) trait MapErrIntoOtherError<T, E> {
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

#[derive(Debug, Error)]
pub enum LsmError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Error compacting SSTable. `{0}`")]
    SSTableCompactError(#[from] SSTableCompactError),
    #[error(transparent)]
    SendError(#[from] SendError<WorkerCommands>),
    #[error(transparent)]
    JoinError(#[from] JoinError),
    #[error("Error writing memtable to disk. `{0}`")]
    WriteMemtableError(#[from] WriteMemtableError),
    #[error("Error reading memtable from disk. `{0}`")]
    ReadMemtableError(#[from] ReadMemtableError),
    #[error("Error saving SSTable. `{0}`")]
    SaveSStableError(#[from] SaveSStableError),
    #[error("Error loading SSTable value for key {0:?}. `{1}`")]
    LoadSStableValueError(Bytes, LoadSStableValueError),
    #[error("Error with flock. `{0}`")]
    FlockError(#[from] flock::FlockError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_non_existent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let lsm = LSM::default(temp_dir.path().to_string_lossy().to_string())
            .await
            .unwrap();
        let key = Bytes::from("key");
        assert_eq!(lsm.get(&key).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_put_get() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut lsm = LSM::default(temp_dir.path().to_string_lossy().to_string())
            .await
            .unwrap();
        let key = Bytes::from("key");
        let value = Bytes::from("value");
        lsm.put(Cow::Borrowed(&key), value.clone()).await.unwrap();
        assert_eq!(lsm.get(&key).await.unwrap().unwrap(), value);
    }

    #[tokio::test]
    async fn test_put_existent_should_overwrite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut lsm = LSM::default(temp_dir.path().to_string_lossy().to_string())
            .await
            .unwrap();
        let key = Bytes::from("key");
        let value_1 = Bytes::from("value 1");
        let value_2 = Bytes::from("value 2");
        lsm.put(Cow::Borrowed(&key), value_1.clone()).await.unwrap();
        lsm.put(Cow::Borrowed(&key), value_2.clone()).await.unwrap();
        assert_eq!(lsm.get(&key).await.unwrap().unwrap(), value_2);
    }

    #[tokio::test]
    async fn test_put_delete_get() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut lsm = LSM::default(temp_dir.path().to_string_lossy().to_string())
            .await
            .unwrap();
        let key = Bytes::from("key");
        let value = Bytes::from("value");
        lsm.put(Cow::Borrowed(&key), value.clone()).await.unwrap();
        lsm.delete(Cow::Borrowed(&key)).await.unwrap();
        assert_eq!(lsm.get(&key).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_put_multiples() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut lsm = LSM::default(temp_dir.path().to_string_lossy().to_string())
            .await
            .unwrap();
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
        assert_eq!(lsm.get(&key_1).await.unwrap().unwrap(), value_1);
        assert_eq!(lsm.get(&key_2).await.unwrap().unwrap(), value_2);
        assert_eq!(lsm.get(&key_3).await.unwrap().unwrap(), value_3);
    }

    #[tokio::test]
    async fn test_more_than_memtable_limit() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut lsm = LSM::new(14, 10, temp_dir.path().to_string_lossy().to_string())
            .await
            .unwrap();
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
        WorkerCommands::await_worker(&lsm.worker_tx).await;
        lsm.put(Cow::Borrowed(&key_3), value_3.clone())
            .await
            .unwrap();
        WorkerCommands::await_worker(&lsm.worker_tx).await;
        assert_eq!(lsm.inner.read().await.memtable.size(), 7);
        assert_eq!(lsm.get(&key_1).await.unwrap().unwrap(), value_1);
        assert_eq!(lsm.get(&key_2).await.unwrap().unwrap(), value_2);
        assert_eq!(lsm.get(&key_3).await.unwrap().unwrap(), value_3);
    }

    #[tokio::test]
    async fn test_more_than_memtable_limit_with_deletions_and_nulls() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut lsm = LSM::new(18, 10, temp_dir.path().to_string_lossy().to_string())
            .await
            .unwrap();
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
        WorkerCommands::await_worker(&lsm.worker_tx).await;
        assert_eq!(lsm.inner.read().await.memtable.size(), 0);
        assert_eq!(lsm.get(&key_1).await.unwrap().unwrap(), value_1);
        assert_eq!(lsm.get(&key_2).await.unwrap(), None);
        assert_eq!(lsm.get(&key_3).await.unwrap().unwrap(), value_3);
    }

    #[tokio::test]
    async fn test_multiple_sstables() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut lsm = LSM::new(20, 10, temp_dir.path().to_string_lossy().to_string())
            .await
            .unwrap();
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
        WorkerCommands::await_worker(&lsm.worker_tx).await;
        lsm.put(Cow::Borrowed(&key_2), value_3.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_3), value_2.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_4), value_1.clone())
            .await
            .unwrap();
        WorkerCommands::await_worker(&lsm.worker_tx).await;
        assert_eq!(lsm.inner.read().await.sstable_metadatas[&0].len(), 2);
        assert_eq!(lsm.inner.read().await.n_sstables, 2);
        assert_eq!(lsm.get(&key_1).await.unwrap().unwrap(), value_1);
        assert_eq!(lsm.get(&key_2).await.unwrap().unwrap(), value_3);
        assert_eq!(lsm.get(&key_3).await.unwrap().unwrap(), value_2);
        assert_eq!(lsm.get(&key_4).await.unwrap().unwrap(), value_1);
    }

    #[tokio::test]
    async fn test_multiple_levels() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut lsm = LSM::new(10, 2, temp_dir.path().to_string_lossy().to_string())
            .await
            .unwrap();
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
        WorkerCommands::await_worker(&lsm.worker_tx).await;
        lsm.put(Cow::Borrowed(&key_3), value_3.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_4), value_4.clone())
            .await
            .unwrap();
        WorkerCommands::await_worker(&lsm.worker_tx).await;
        lsm.put(Cow::Borrowed(&key_5), value_5.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_2), value_3.clone())
            .await
            .unwrap();
        WorkerCommands::await_worker(&lsm.worker_tx).await;
        lsm.put(Cow::Borrowed(&key_3), value_2.clone())
            .await
            .unwrap();
        lsm.put(Cow::Borrowed(&key_4), value_1.clone())
            .await
            .unwrap();
        WorkerCommands::await_worker(&lsm.worker_tx).await;
        assert_eq!(lsm.inner.read().await.sstable_metadatas[&0].len(), 1);
        assert_eq!(lsm.inner.read().await.sstable_metadatas[&1].len(), 1);
        assert_eq!(lsm.inner.read().await.n_sstables, 5);
        assert_eq!(lsm.get(&key_1).await.unwrap().unwrap(), value_1);
        assert_eq!(lsm.get(&key_2).await.unwrap().unwrap(), value_3);
        assert_eq!(lsm.get(&key_3).await.unwrap().unwrap(), value_2);
        assert_eq!(lsm.get(&key_4).await.unwrap().unwrap(), value_1);
        assert_eq!(lsm.get(&key_5).await.unwrap().unwrap(), value_5);
    }

    #[tokio::test]
    async fn test_should_restore_state() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut lsm = LSM::new(20, 10, temp_dir.path().to_string_lossy().to_string())
            .await
            .unwrap();
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
        WorkerCommands::await_worker(&lsm.worker_tx).await;
        lsm.close().await.unwrap();
        let lsm = LSM::new(20, 10, temp_dir.path().to_string_lossy().to_string())
            .await
            .unwrap();
        assert_eq!(lsm.inner.read().await.sstable_metadatas[&0].len(), 1);
        assert_eq!(lsm.inner.read().await.n_sstables, 1);
        assert_eq!(lsm.get(&key_1).await.unwrap().unwrap(), value_1);
        assert_eq!(lsm.get(&key_2).await.unwrap().unwrap(), value_3);
        assert_eq!(lsm.get(&key_3).await.unwrap().unwrap(), value_2);
        assert_eq!(lsm.get(&key_4).await.unwrap().unwrap(), value_1);
    }

    #[tokio::test]
    async fn test_should_restore_state_if_drop_with_memtable() {
        let temp_dir = tempfile::tempdir().unwrap();
        let key_1 = Bytes::from("key 1");
        let key_2 = Bytes::from("key 2");
        let value_1 = Bytes::from("v1");
        let value_2 = Bytes::from("v2");

        {
            let mut lsm = LSM::new(100, 10, temp_dir.path().to_string_lossy().to_string())
                .await
                .unwrap();
            lsm.put(Cow::Borrowed(&key_1), value_1.clone())
                .await
                .unwrap();
            lsm.put(Cow::Borrowed(&key_2), value_2.clone())
                .await
                .unwrap();
        }
        let lsm = LSM::new(20, 10, temp_dir.path().to_string_lossy().to_string())
            .await
            .unwrap();
        assert_eq!(lsm.inner.read().await.n_sstables, 0);
        assert_eq!(lsm.get(&key_1).await.unwrap().unwrap(), value_1);
        assert_eq!(lsm.get(&key_2).await.unwrap().unwrap(), value_2);
    }
}
