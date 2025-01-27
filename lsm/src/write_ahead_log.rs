use std::{
    borrow::Cow,
    io::{Cursor, ErrorKind, SeekFrom},
    path::PathBuf,
};

use bytes::Bytes;
use thiserror::Error;
use tokio::{
    fs::{read_dir, File},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter},
};

use crate::{
    memtable::{self, Memtable, Value},
    types::{UVarInt, UVartIntConvertError},
};

#[derive(Debug)]
pub struct WriteAheadLog {
    writer: BufWriter<File>,
    base_path: String,
    with_fsync: bool,
}

impl WriteAheadLog {
    pub async fn new(
        base_path: String,
        last_entry_id: u64,
        with_fsync: bool,
    ) -> Result<(Self, Memtable), CreateWriteAheadLogError> {
        let mut memtable = memtable::Memtable::new();
        let wals = Self::list_wal_files(&base_path).await?;
        for wal_path in wals.iter() {
            let mut file =
                BufReader::new(tokio::fs::File::open(&wal_path).await.map_err(|err| {
                    CreateWriteAheadLogError::ReadWal(wal_path.display().to_string(), err)
                })?);
            loop {
                let mut buf = vec![0; 8];
                match file.read_exact(&mut buf).await {
                    Ok(_) => {}
                    Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                    Err(err) => return Err(CreateWriteAheadLogError::ReadEntryId(err)),
                }
                let entry_id = u64::from_be_bytes(buf.try_into().expect("Known size"));
                let entry_size = UVarInt::read(&mut file)
                    .await
                    .map_err(CreateWriteAheadLogError::ReadEntrySize)?;
                let entry_size: u64 = entry_size.try_into()?;
                if entry_id <= last_entry_id {
                    file.seek(SeekFrom::Current(entry_size as i64))
                        .await
                        .map_err(CreateWriteAheadLogError::SeekEntry)?;
                    continue;
                }
                let key_size = UVarInt::read(&mut file)
                    .await
                    .map_err(CreateWriteAheadLogError::ReadKeySize)?;
                let key_size: u64 = key_size.try_into()?;
                let mut key = vec![0; key_size as usize];
                file.read_exact(&mut key)
                    .await
                    .map_err(CreateWriteAheadLogError::ReadKey)?;
                let value = Value::read(&mut file)
                    .await
                    .map_err(CreateWriteAheadLogError::ReadValue)?;
                memtable.put(Cow::Owned(key.into()), value);
            }
        }

        let writer = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(
                wals.last()
                    .cloned()
                    .unwrap_or(format!("{}/{}.wal", base_path, last_entry_id).into()),
            )
            .await
            .map_err(CreateWriteAheadLogError::OpenWal)?;
        let writer = BufWriter::new(writer);
        Ok((
            WriteAheadLog {
                writer,
                base_path,
                with_fsync,
            },
            memtable,
        ))
    }

    pub async fn append(
        &mut self,
        entry_id: u64,
        key: &Bytes,
        value: &Value,
    ) -> Result<(), AppendWriteAheadLogError> {
        self.writer
            .write_all(&entry_id.to_be_bytes())
            .await
            .map_err(AppendWriteAheadLogError::WriteEntryId)?;
        let mut buffer = Cursor::new(Vec::new());
        UVarInt::U64(key.len() as u64)
            .write(&mut buffer)
            .await
            .map_err(AppendWriteAheadLogError::WriteKeySize)?;
        buffer
            .write_all(key)
            .await
            .map_err(AppendWriteAheadLogError::WriteKey)?;
        value
            .write(&mut buffer)
            .await
            .map_err(AppendWriteAheadLogError::WriteValue)?;
        let buffer = buffer.into_inner();
        UVarInt::U64(buffer.len() as u64)
            .write(&mut self.writer)
            .await
            .map_err(AppendWriteAheadLogError::WriteEntrySize)?;
        self.writer
            .write_all(&buffer)
            .await
            .map_err(AppendWriteAheadLogError::WriteEntry)?;
        self.writer
            .flush()
            .await
            .map_err(AppendWriteAheadLogError::Flush)?;
        if self.with_fsync {
            self.writer
                .get_ref()
                .sync_all()
                .await
                .map_err(AppendWriteAheadLogError::SyncAll)?;
        }
        Ok(())
    }

    pub async fn start_new(&mut self, entry_id: u64) -> Result<(), std::io::Error> {
        self.writer = BufWriter::new(
            tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(format!("{}/{}.wal", self.base_path, entry_id))
                .await?,
        );
        Ok(())
    }

    pub async fn list_wal_files(base_path: &str) -> Result<Vec<PathBuf>, ListWalFilesError> {
        let mut read_dir = read_dir(&base_path)
            .await
            .map_err(ListWalFilesError::ReadDir)?;
        let mut wals = vec![];
        while let Some(entry) = read_dir
            .next_entry()
            .await
            .map_err(ListWalFilesError::ReadDirEntry)?
        {
            let path = entry.path();
            if path.is_file() {
                let file_name = path.file_name().and_then(|f| f.to_str());
                if file_name
                    .map(|file_name| file_name.ends_with(".wal"))
                    .unwrap_or(false)
                {
                    wals.push(path);
                }
            }
        }
        wals.sort();
        Ok(wals)
    }
}

#[derive(Debug, Error)]
pub enum CreateWriteAheadLogError {
    #[error("Error listing wal files: {0}")]
    ListWalFilesError(#[from] ListWalFilesError),
    #[error("Error reading write ahead log in path {0}: {1}")]
    ReadWal(String, std::io::Error),
    #[error("Error reading entry id: {0}")]
    ReadEntryId(std::io::Error),
    #[error("Error reading entry size: {0}")]
    ReadEntrySize(std::io::Error),
    #[error("Error seeking forwarn entry size log: {0}")]
    SeekEntry(std::io::Error),
    #[error("Error reading key size: {0}")]
    ReadKeySize(std::io::Error),
    #[error("Error reading key: {0}")]
    ReadKey(std::io::Error),
    #[error("Error reading value: {0}")]
    ReadValue(std::io::Error),
    #[error("Error deleting write ahead log: {0}")]
    DeleteWal(std::io::Error),
    #[error("Error opening write ahead log: {0}")]
    OpenWal(std::io::Error),
    #[error(transparent)]
    UVartIntConvertError(#[from] UVartIntConvertError),
}

#[derive(Debug, Error)]
pub enum AppendWriteAheadLogError {
    #[error("Error opening write ahead log: {0}")]
    OpenWal(std::io::Error),
    #[error("Error writing entry id: {0}")]
    WriteEntryId(std::io::Error),
    #[error("Error writing entry size: {0}")]
    WriteEntrySize(std::io::Error),
    #[error("Error writing entry: {0}")]
    WriteEntry(std::io::Error),
    #[error("Error writing entry size: {0}")]
    WriteKeySize(std::io::Error),
    #[error("Error writing key: {0}")]
    WriteKey(std::io::Error),
    #[error("Error writing value: {0}")]
    WriteValue(std::io::Error),
    #[error("Error flushing write ahead log: {0}")]
    Flush(std::io::Error),
    #[error("Error syncing write ahead log: {0}")]
    SyncAll(std::io::Error),
}

#[derive(Debug, Error)]
pub enum ListWalFilesError {
    #[error("Error reading directory: {0}")]
    ReadDir(std::io::Error),
    #[error("Error reading entry: {0}")]
    ReadDirEntry(std::io::Error),
}
