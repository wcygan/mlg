use std::sync::Arc;

use tempfile::tempfile;

use mlg::{FileLog, Log, Offset};
use mlg::LogError::IndexOutOfBounds;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PersonalInfo {
    name: String,
    age: u8,
}

/// Creates a new log for testing.
async fn new_log() -> FileLog {
    let temp_file = tempfile().expect("Failed to create a temp file");
    let temp_file = tokio::fs::File::from_std(temp_file);
    FileLog::new_with_file(temp_file)
}

/// Appends a record to the log.
async fn append_record(log: &FileLog, name: &str, age: u8) -> Offset {
    let data = PersonalInfo {
        name: name.to_string(),
        age,
    };
    let serialized_data = bincode::serialize(&data).expect("Failed to serialize data");
    let offset = log.append(serialized_data).await.expect("Failed to append data");
    offset
}

/// Appends a record to the log and returns the offset.
#[tokio::test]
async fn test_append() {
    let log = new_log().await;

    // Create a record.
    let data = PersonalInfo {
        name: "John Doe".to_string(),
        age: 42,
    };

    // Serialize the record.
    let data = bincode::serialize(&data).expect("Failed to serialize data");

    // Append the record to the log & assert no error
    assert!(log.append(data).await.is_ok());
}

/// Reads a record from the log.
#[tokio::test]
async fn test_read() {
    let log = new_log().await;

    // Create a record.
    let data = PersonalInfo {
        name: "John Doe".to_string(),
        age: 42,
    };

    // Serialize the record.
    let data = bincode::serialize(&data).expect("Failed to serialize data");

    // Append the record to the log.
    let offset = log.append(data).await.expect("Failed to append data");

    // Read the record from the log.
    let read_data = log
        .read(offset)
        .await
        .expect("Failed to read data from log");

    // Deserialize the record.
    let read_data: PersonalInfo =
        bincode::deserialize(&read_data.0).expect("Failed to deserialize data");

    // Assert that the record is deserialized correctly.
    assert_eq!(read_data.name, "John Doe");
    assert_eq!(read_data.age, 42);
}

/// Reads two records from the log.
#[tokio::test]
async fn test_read_two_records() {
    // A test that creates records for john & jane doe, and asserts that both records are deserialized correctly.
    let log = new_log().await;

    // Create two records.
    let john_doe = PersonalInfo {
        name: "John Doe".to_string(),
        age: 42,
    };

    let jane_doe = PersonalInfo {
        name: "Jane Doe".to_string(),
        age: 43,
    };

    // Serialize the records.
    let john_doe = bincode::serialize(&john_doe).expect("Failed to serialize data");
    let jane_doe = bincode::serialize(&jane_doe).expect("Failed to serialize data");

    // Append the records to the log.
    let john_offset = log.append(john_doe).await.expect("Failed to append data");
    let jane_offset = log.append(jane_doe).await.expect("Failed to append data");

    // Read the records from the log.
    let read_john_doe = log
        .read(john_offset)
        .await
        .expect("Failed to read data from log");

    let read_jane_doe = log
        .read(jane_offset)
        .await
        .expect("Failed to read data from log");

    // Deserialize the records.
    let read_john_doe: PersonalInfo =
        bincode::deserialize(&read_john_doe.0).expect("Failed to deserialize data");
    let read_jane_doe: PersonalInfo =
        bincode::deserialize(&read_jane_doe.0).expect("Failed to deserialize data");

    // Assert that the records are deserialized correctly.
    assert_eq!(read_john_doe.name, "John Doe");
    assert_eq!(read_jane_doe.name, "Jane Doe");
    assert_eq!(read_john_doe.age, 42);
    assert_eq!(read_jane_doe.age, 43);
    assert_ne!(read_john_doe.name, read_jane_doe.name);
    assert_ne!(read_john_doe.age, read_jane_doe.age);
}

/// Tests that the next offset is returned correctly.
#[tokio::test]
async fn the_next_offset_is_returned_correctly() {
    let log = new_log().await;

    // Create a record.
    let data = PersonalInfo {
        name: "John Doe".to_string(),
        age: 42,
    };

    // Serialize the record.
    let data = bincode::serialize(&data).expect("Failed to serialize data");

    // Append the record to the log.
    let offset = log.append(data.clone()).await.expect("Failed to append data");

    // Read the record from the log.
    let read_data = log
        .read(offset)
        .await
        .expect("Failed to read data from log");

    // Get the next offset. This is calculated by adding the size of the record to the current offset.
    let expected_next_offset = offset + std::mem::size_of::<u64>() as u64 + data.len() as u64;

    // Assert that the next offset is correct.
    assert_eq!(read_data.1, expected_next_offset);
}

/// Tests that batch reading from the log works correctly when there are fewer records than requested.
#[tokio::test]
async fn test_batch_read_less_records_than_requested() {
    let log = new_log().await;

    // Append two records.
    append_record(&log, "John Doe", 30).await;
    append_record(&log, "Jane Doe", 31).await;

    // Try to read 5 records, expecting only 2 to be returned.
    let (records, _) = log.batch_read(0, 5).await.expect("Failed to batch read from log");

    assert_eq!(records.len(), 2, "Should have read 2 records, but got {}", records.len());
}

/// Tests that batch reading from the log works correctly when there the same number of records as requested.
#[tokio::test]
async fn test_batch_read_exact_number_of_requested_records() {
    let log = new_log().await;

    // Append five records.
    for i in 0..5 {
        let _ = append_record(&log, &format!("User{}", i), 20 + i as u8).await;
    }

    // Try to read 5 records, expecting exactly 5 to be returned.
    let (records, _) = log.batch_read(0, 5).await.expect("Failed to batch read from log");
    assert_eq!(records.len(), 5, "Should have read 5 records, but got {}", records.len());
}

/// Tests that batch reading from the log works correctly when there are more records than requested.
#[tokio::test]
async fn test_batch_read_when_more_records_exist_than_requested() {
    let log = new_log().await;

    // Append ten records.
    for i in 0..10 {
        let _ = append_record(&log, &format!("User{}", i), 20 + i as u8).await;
    }

    // Try to read 5 records, expecting exactly 5 to be returned even though there are more records.
    let (records, _) = log.batch_read(0, 5).await.expect("Failed to batch read from log");
    assert_eq!(records.len(), 5, "Should have read 5 records, but got {}", records.len());
}

#[tokio::test]
async fn test_batch_read_when_no_records_exist_returns_error() {
    let log = new_log().await;

    // Try to read 5 records, expecting 0 to be returned since the log is empty.
    let res = log.batch_read(0, 5).await;

    // Assert that the result is an error.
    assert!(res.is_err());

    // Assert that the error is IndexOutOfBounds.
    match res.unwrap_err() {
        IndexOutOfBounds => {}
        err => panic!("Expected IndexOutOfBounds error, but got {:?}", err),
    }
}

#[tokio::test]
async fn test_batch_read_data_integrity_and_order() {
    let log = new_log().await;

    // Append several records with unique data.
    let mut expected_records = Vec::new();
    for i in 0..10 {
        let name = format!("User{}", i);
        let age = 20 + i as u8;
        append_record(&log, &name, age).await;
        expected_records.push(PersonalInfo { name, age });
    }

    // Perform a batch read to get the same number of records as appended.
    let (records, _) = log.batch_read(0, expected_records.len()).await.expect("Failed to batch read from log");

    // Check that the number of records read matches the number appended.
    assert_eq!(records.len(), expected_records.len(), "Expected {} records, got {}", expected_records.len(), records.len());

    // Check each record for integrity and order.
    for (index, record_bytes) in records.iter().enumerate() {
        let record: PersonalInfo = bincode::deserialize(record_bytes).expect("Failed to deserialize data");
        let expected_record = &expected_records[index];
        assert_eq!(record.name, expected_record.name, "Expected name {}, got {}", expected_record.name, record.name);
        assert_eq!(record.age, expected_record.age, "Expected age {}, got {}", expected_record.age, record.age);
    }
}

/// This test will append two records concurrently and ensure that both writes succeed
/// and that the log contains both entries, no matter the order in which they are written.
#[tokio::test]
async fn test_concurrent_writes() {
    let log = Arc::new(new_log().await);

    // Concurrently append two different records.
    let handle1 =
        tokio::spawn(
            {
                let log = log.clone();
                async move {
                    append_record(&log, "ConcurrentUser1", 25).await;
                }
            });

    let handle2 =
        tokio::spawn(
            {
                let log = log.clone();
                async move {
                    append_record(&log, "ConcurrentUser2", 26).await;
                }
            });

    // Await both handles to finish.
    let _ = tokio::try_join!(handle1, handle2);

    // Read back the log to ensure both records were written.
    let (records, _) = log.batch_read(0, 2).await.expect("Failed to batch read from log");
    assert_eq!(records.len(), 2, "Both records should have been written");

    let record1: PersonalInfo = bincode::deserialize(&records[0]).expect("Failed to deserialize data");
    let record2: PersonalInfo = bincode::deserialize(&records[1]).expect("Failed to deserialize data");

    assert!(record1.name == "ConcurrentUser1" || record1.name == "ConcurrentUser2");
    assert!(record2.name == "ConcurrentUser1" || record2.name == "ConcurrentUser2");
    assert_ne!(record1.name, record2.name);
    assert!(record1.age == 25 || record1.age == 26);
    assert!(record2.age == 25 || record2.age == 26);
    assert_ne!(record1.age, record2.age);
}

/// This test will write a record to the log and then concurrently
/// read it from multiple tasks to ensure that they all see the same value.
#[tokio::test]
async fn test_concurrent_reads_after_write() {
    let log = Arc::new(new_log().await);

    // Append a record.
    let offset = append_record(&log, "User", 30).await;

    // Concurrently read the record from multiple tasks.
    let mut handles = Vec::new();
    for _ in 0..10 {
        let log_clone = log.clone();
        handles.push(tokio::spawn(async move {
            log_clone.read(offset).await.expect("Failed to read data from log")
        }));
    }

    // Collect the results.
    let results = futures::future::join_all(handles).await;

    assert_eq!(results.len(), 10, "Expected 10 results, got {}", results.len());

    // Check that all tasks read the same value.
    for res in results {
        let (bytes, _) = res.expect("Task failed");
        let record: PersonalInfo = bincode::deserialize(&bytes).expect("Failed to deserialize data");
        assert_eq!(record.name, "User");
        assert_eq!(record.age, 30);
    }
}
