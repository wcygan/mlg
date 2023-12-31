use async_trait::async_trait;
use std::fmt::{Display, Formatter};

mod file_log;

pub use file_log::FileLog;

/// The position of a record within a log, represented as an offset from the beginning of the log.
pub type Offset = u64;

/// A byte vector, used to represent binary data.
pub type Bytes = Vec<u8>;

/// Represents a single record within a log, consisting of a key, a value
struct Record {
    /// The length of the value of the log entry (in bytes).
    value_length: u64,

    /// The value of the log entry.
    value: Bytes,
}

#[async_trait]
pub trait Log {
    /// Appends a new record to the log, returning the offset of the newly appended record.
    async fn append(&self, entry: Bytes) -> Result<Offset, LogError>;

    /// Reads the record at the given offset, returning the record
    /// and the offset of the next record.
    async fn read(&self, offset: Offset) -> Result<(Bytes, Offset), LogError>;

    /// Reads records starting at the given offset, returning up to 'max_records' records,
    /// and the offset of the record after the last record read.
    async fn batch_read(
        &self,
        offset: Offset,
        max_records: usize,
    ) -> Result<(Vec<Bytes>, Offset), LogError>;
}

/// An enumeration of possible errors that can occur during log operations.
#[derive(Debug)]
pub enum LogError {
    /// Represents an I/O error, typically encountered during file operations (read/write).
    IoError(std::io::Error),

    /// Represents an error encountered while attempting to read a record from the log
    /// that does not exist.
    IndexOutOfBounds,
}

impl Display for LogError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LogError::IoError(e) => write!(f, "I/O error: {}", e),
            LogError::IndexOutOfBounds => write!(f, "Index out of bounds"),
        }
    }
}

impl std::error::Error for LogError {}
