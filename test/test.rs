use tempfile::tempfile;

use mlg::{FileLog, Log};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PersonalInfo {
    name: String,
    age: u8,
}

async fn new_log() -> FileLog {
    let temp_file = tempfile().expect("Failed to create a temp file");
    let temp_file = tokio::fs::File::from_std(temp_file);
    FileLog::new_with_file(temp_file)
        .await
        .expect("Failed to create FileLog")
}

#[tokio::test]
async fn test_append() {
    let log = new_log().await;

    let data = PersonalInfo {
        name: "John Doe".to_string(),
        age: 42,
    };

    let data = bincode::serialize(&data).expect("Failed to serialize data");

    log.append(data).await.expect("Failed to append data");
}

#[tokio::test]
async fn test_read() {
    let log = new_log().await;

    let data = PersonalInfo {
        name: "John Doe".to_string(),
        age: 42,
    };

    let data = bincode::serialize(&data).expect("Failed to serialize data");

    let offset = log.append(data).await.expect("Failed to append data");

    let read_data = log
        .read(offset)
        .await
        .expect("Failed to read data from log");

    let read_data: PersonalInfo =
        bincode::deserialize(&read_data).expect("Failed to deserialize data");

    assert_eq!(read_data.name, "John Doe");
    assert_eq!(read_data.age, 42);
}

#[tokio::test]
async fn test_read_two_records() {
    // A test that creates records for john & jane doe, and asserts that both records are deserialized correctly.
    let log = new_log().await;

    let john_doe = PersonalInfo {
        name: "John Doe".to_string(),
        age: 42,
    };

    let john_doe = bincode::serialize(&john_doe).expect("Failed to serialize data");

    let jane_doe = PersonalInfo {
        name: "Jane Doe".to_string(),
        age: 43,
    };

    let jane_doe = bincode::serialize(&jane_doe).expect("Failed to serialize data");

    let john_offset = log.append(john_doe).await.expect("Failed to append data");

    let jane_offset = log.append(jane_doe).await.expect("Failed to append data");

    let read_john_doe = log
        .read(john_offset)
        .await
        .expect("Failed to read data from log");

    let read_john_doe: PersonalInfo =
        bincode::deserialize(&read_john_doe).expect("Failed to deserialize data");

    let read_jane_doe = log
        .read(jane_offset)
        .await
        .expect("Failed to read data from log");

    let read_jane_doe: PersonalInfo =
        bincode::deserialize(&read_jane_doe).expect("Failed to deserialize data");

    assert_eq!(read_john_doe.name, "John Doe");
    assert_eq!(read_jane_doe.name, "Jane Doe");
    assert_eq!(read_john_doe.age, 42);
    assert_eq!(read_jane_doe.age, 43);
    assert_ne!(read_john_doe.name, read_jane_doe.name);
    assert_ne!(read_john_doe.age, read_jane_doe.age);
}
