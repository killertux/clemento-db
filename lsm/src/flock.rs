use std::{
    fs::File,
    io::{Error, ErrorKind},
    thread::sleep,
    time::Duration,
};

use fs3::FileExt;
use thiserror::Error;
use tokio::task::spawn_blocking;

#[derive(Debug)]
pub struct Flock {
    file: File,
    path: String,
}

impl Flock {
    pub async fn new(base_path: &str) -> Result<Self, FlockError> {
        let path = format!("{base_path}/database.lock");
        spawn_blocking(|| {
            let lock_start = std::time::Instant::now();
            let mut last_error = None;
            while lock_start.elapsed().as_secs() < 3 {
                let file = File::create(&path).and_then(|file| {
                    file.try_lock_exclusive()?;
                    Ok(file)
                });
                match file {
                    Ok(file) => {
                        return Ok(Flock { file, path });
                    }
                    Err(err) => {
                        last_error = Some(err);
                        sleep(Duration::from_millis(200));
                    }
                }
            }
            Err(FlockError::CreateFile(last_error.unwrap_or(Error::new(
                ErrorKind::Other,
                "Could not create lock file",
            ))))
        })
        .await?
    }

    pub async fn unlock(self) -> Result<(), FlockError> {
        spawn_blocking(move || self.file.unlock())
            .await?
            .map_err(FlockError::UnlockError)
    }
}

#[derive(Debug, Error)]
pub enum FlockError {
    #[error("Error creating lock file: {0}")]
    CreateFile(Error),
    #[error("Error joining blocking task: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Error unlocking file: {0}")]
    UnlockError(Error),
}
