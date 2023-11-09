use std::sync::Arc;

use async_trait::async_trait;
use tokio::fs::{File, OpenOptions};
use tokio::io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, MutexGuard};

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

    pub fn new_with_file(file: File) -> Self {
        FileLog {
            file: Arc::new(Mutex::new(file)),
        }
    }

    /// Reads a single record from the log file.
    async fn read_one_record(&self, offset: Offset, file: &mut MutexGuard<'_, File>) -> Result<(Record, Offset), LogError> {
        // Read the length of the value
        let mut length_buf = [0u8; 8];
        file.read_exact(&mut length_buf)
            .await
            .map_err(LogError::IoError)?;
        let value_length = u64::from_be_bytes(length_buf);

        // Read the value
        let mut value_buf = vec![0u8; value_length as usize];
        file.read_exact(&mut value_buf)
            .await
            .map_err(LogError::IoError)?;

        // Calculate the next offset
        let next_offset = offset + std::mem::size_of::<u64>() as u64 + value_length;

        Ok((Record {
            value_length,
            value: value_buf,
        }, next_offset))
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
        };

        file.write(&record.value_length.to_be_bytes())
            .await
            .map_err(LogError::IoError)?;

        file.write(&record.value).await.map_err(LogError::IoError)?;


        Ok(offset)
    }

    async fn read(&self, offset: Offset) -> Result<(Bytes, Offset), LogError> {
        let mut file = self.file.lock().await;

        let file_size = file
            .metadata()
            .await
            .map_err(LogError::IoError)?
            .len();

        if offset >= file_size {
            return Err(LogError::IndexOutOfBounds);
        }

        // Seek to the offset
        file.seek(io::SeekFrom::Start(offset))
            .await
            .map_err(LogError::IoError)?;

        let (record, next_offset) = self.read_one_record(offset, &mut file).await?;

        Ok((record.value, next_offset))
    }

    async fn batch_read(&self, offset: Offset, max_records: usize) -> Result<(Vec<Bytes>, Offset), LogError> {
        let mut file = self.file.lock().await;

        let file_size = file
            .metadata()
            .await
            .map_err(LogError::IoError)?
            .len();

        if offset >= file_size {
            return Err(LogError::IndexOutOfBounds);
        }

        file.seek(io::SeekFrom::Start(offset))
            .await
            .map_err(LogError::IoError)?;

        let mut records = Vec::new();
        let mut next_offset = offset;

        while next_offset < file_size && records.len() < max_records {
            let (record, offset) = self.read_one_record(next_offset, &mut file).await?;
            records.push(record.value);
            next_offset = offset;
        }

        Ok((records, next_offset))
    }
}

impl From<io::Error> for LogError {
    fn from(error: io::Error) -> Self {
        LogError::IoError(error)
    }
}
