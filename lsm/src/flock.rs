use std::{
    io::{Error, ErrorKind},
    time::Duration,
};

use thiserror::Error;
use tokio::{fs::File, io::AsyncWriteExt, time::sleep};

#[derive(Debug)]
pub struct Flock {
    file: File,
    path: String,
}

impl Flock {
    pub async fn new(base_path: &str) -> Result<Self, FlockError> {
        let path = format!("{base_path}/database.lock");
        let lock_start = std::time::Instant::now();
        let mut last_error = None;
        while lock_start.elapsed().as_secs() < 3 {
            let file = File::create_new(&path).await;
            match file {
                Ok(file) => {
                    return Ok(Flock { file, path });
                }
                Err(err) => {
                    last_error = Some(err);
                    sleep(Duration::from_millis(200)).await;
                }
            }
        }
        Err(FlockError::CreateFile(last_error.unwrap_or(Error::new(
            ErrorKind::Other,
            "Could not create lock file",
        ))))
    }

    pub async fn unlock(mut self) -> Result<(), FlockError> {
        self.file.flush().await.map_err(FlockError::Flush)?;
        tokio::fs::remove_file(&self.path)
            .await
            .map_err(FlockError::RemoveFile)
    }
}

#[derive(Debug, Error)]
pub enum FlockError {
    #[error("Error creating lock file: {0}")]
    CreateFile(Error),
    #[error("Error removing lock file: {0}")]
    RemoveFile(Error),
    #[error("Error flushing lock file: {0}")]
    Flush(Error),
}
