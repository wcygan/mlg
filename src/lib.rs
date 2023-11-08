use async_trait::async_trait;

use serde::{Deserialize, Serialize};

mod file_log;

/// The position of a record within a log, represented as an offset from the beginning of the log.
type Offset = u64;

/// A byte vector, used to represent binary data.
type Bytes = Vec<u8>;

/// Represents a single record within a log, consisting of a key, a value, and a timestamp.
#[derive(Serialize, Deserialize)]
struct Record {
    /// The length of the value of the log entry (in bytes).
    value_length: u64,

    /// The value of the log entry.
    value: Bytes,

    /// The timestamp when the log entry was created, represented as a Unix timestamp in milliseconds.
    timestamp: u128,
}

/// An enumeration of possible errors that can occur during log operations.
#[derive(Debug)]
pub enum LogError {
    /// Represents an I/O error, typically encountered during file operations (read/write).
    IoError(std::io::Error),

    /// Represents a system time error, typically encountered when converting between system time and Unix time.
    TimeError(std::time::SystemTimeError),
    
    /// Represents a serialization error, typically encountered when serializing or deserializing a record.
    SerializationError(bincode::Error),
}

#[async_trait]
pub trait Log {
    /// Appends a new record to the log, returning the offset of the newly appended record.
    async fn append(&self, entry: Bytes) -> Result<Offset, LogError>;

    /// Reads the record at the given offset, returning the record.
    async fn read(&self, offset: Offset) -> Result<Bytes, LogError>;
}
