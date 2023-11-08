use std::sync::Arc;
use std::time::{SystemTime, SystemTimeError};

use async_trait::async_trait;
use tokio::fs::{File, OpenOptions};
use tokio::io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::{Bytes, Log, LogError, Offset, Record};

/// Represents the file-based log.
pub struct FileLog {
    file: Arc<Mutex<File>>,
}

impl FileLog {
    /// Creates a new FileLog, opening the log file for both reading and writing.
    /// The log file will be created if it does not exist.
    pub async fn new(path: &str) -> Result<Self, LogError> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(path)
            .await
            .map_err(LogError::IoError)?;

        Ok(FileLog {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub async fn new_with_file(file: File) -> Result<Self, LogError> {
        Ok(FileLog {
            file: Arc::new(Mutex::new(file)),
        })
    }
}

#[async_trait]
impl Log for FileLog {
    async fn append(&self, entry: Bytes) -> Result<Offset, LogError> {
        let mut file = self.file.lock().await;

        // Get the current offset
        let offset = file
            .seek(io::SeekFrom::End(0))
            .await
            .map_err(LogError::IoError)?;

        // Create a new record
        let record = Record {
            value_length: entry.len() as u64,
            value: entry,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_millis(),
        };

        file.write(&record.value_length.to_be_bytes())
            .await
            .map_err(LogError::IoError)?;

        file.write(&record.value).await.map_err(LogError::IoError)?;

        file.write(&record.timestamp.to_be_bytes())
            .await
            .map_err(LogError::IoError)?;

        Ok(offset)
    }

    async fn read(&self, offset: Offset) -> Result<Bytes, LogError> {
        let mut file = self.file.lock().await;

        // Seek to the offset
        file.seek(io::SeekFrom::Start(offset))
            .await
            .map_err(LogError::IoError)?;

        let mut length_buf = [0u8; 8];
        file.read_exact(&mut length_buf)
            .await
            .map_err(LogError::IoError)?;
        let value_length = u64::from_be_bytes(length_buf);

        let mut value_buf = vec![0u8; value_length as usize];
        file.read_exact(&mut value_buf)
            .await
            .map_err(LogError::IoError)?;

        let mut timestamp_buf = [0u8; 16];
        file.read_exact(&mut timestamp_buf)
            .await
            .map_err(LogError::IoError)?;
        let _timestamp = u128::from_be_bytes(timestamp_buf);

        Ok(value_buf)
    }
}

impl From<SystemTimeError> for LogError {
    fn from(error: SystemTimeError) -> Self {
        LogError::TimeError(error)
    }
}

impl From<io::Error> for LogError {
    fn from(error: io::Error) -> Self {
        LogError::IoError(error)
    }
}
