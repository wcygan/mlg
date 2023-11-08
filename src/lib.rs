use async_trait::async_trait;

mod file_log;

/// The position of a record within a log, represented as an offset from the beginning of the log.
type Offset = u64;

/// A byte vector, used to represent binary data.
type Bytes = Vec<u8>;

/// Represents a single record within a log, consisting of a key, a value, and a timestamp.
struct Record {
    /// The length of the log entry, in bytes.
    length: u64,

    /// The key of the log entry, which can be used for partitioning or identifying the record.
    /// Stored as a byte vector to support binary key data.
    key: Bytes,

    /// The value of the log entry. The actual data payload as a byte vector, supporting binary content.
    value: Bytes,

    /// The timestamp when the log entry was created, represented as a Unix timestamp in milliseconds.
    timestamp: u64,
}

/// An enumeration of possible errors that can occur during log operations.
#[derive(Debug)]
pub enum LogError {
    /// Represents an I/O error, typically encountered during file operations (read/write).
    IoError(std::io::Error),
}

#[async_trait]
pub trait Log {
    /// Appends a new record to the log, returning the offset of the newly appended record.
    async fn append(&self, entry: Bytes) -> Result<Offset, LogError>;

    /// Reads the record at the given offset, returning the record.
    async fn read(&self, offset: Offset) -> Result<Bytes, LogError>;
}
