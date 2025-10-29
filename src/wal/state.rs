use std::path::Path;

use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::{
    errors,
    wal::{WAL_STATE_PATH, record_id::WALRecordID, segment_id::WALSegmentID},
};

#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct WALGlobalState {
    pub last_record_id: WALRecordID,
    pub last_checkpoint_record_id: WALRecordID,
    pub last_segment_id: WALSegmentID,
    pub last_checkpoint_segment_id: WALSegmentID,
    pub last_segment_file_offset: usize,
}

impl WALGlobalState {
    // Load the WAL global state from a file (if exists)
    pub async fn load(base_path: &Path) -> errors::Result<Self> {
        let wal_state_path = base_path.join(WAL_STATE_PATH);

        if !wal_state_path.exists() {
            return Err(errors::Errors::new(errors::ErrorCodes::WALStateReadError)
                .with_message("WAL state file does not exist".to_string()));
        }

        let data = std::fs::read(wal_state_path).map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::WALStateReadError).with_message(e.to_string())
        })?;
        let state = serde_json::from_slice(&data).map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::WALStateDecodeError).with_message(e.to_string())
        })?;

        Ok(state)
    }

    pub async fn get_file_init_handle(&self, base_path: &Path) -> errors::Result<tokio::fs::File> {
        let wal_state_path = base_path.join(WAL_STATE_PATH);

        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(wal_state_path)
            .await
            .map_err(|e| {
                errors::Errors::new(errors::ErrorCodes::WALStateWriteError)
                    .with_message(e.to_string())
            })?;

        Ok(file)
    }

    pub async fn get_file_handle(&self, base_path: &Path) -> errors::Result<tokio::fs::File> {
        let wal_state_path = base_path.join(WAL_STATE_PATH);

        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(wal_state_path)
            .await
            .map_err(|e| {
                errors::Errors::new(errors::ErrorCodes::WALStateWriteError)
                    .with_message(e.to_string())
            })?;

        Ok(file)
    }

    // Save the WAL global state to a file
    pub async fn save(&self, file_handle: &mut tokio::fs::File) -> errors::Result<()> {
        let data = serde_json::to_vec(self).map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::WALRecordEncodeError)
                .with_message(e.to_string())
        })?;

        file_handle
            .seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|e| {
                errors::Errors::new(errors::ErrorCodes::WALStateWriteError)
                    .with_message(e.to_string())
            })?;

        file_handle.write_all(&data).await.map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::WALStateWriteError).with_message(e.to_string())
        })?;

        file_handle.set_len(data.len() as u64).await.map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::WALStateWriteError).with_message(e.to_string())
        })?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct WALStateWriteHandles {
    pub(crate) state_file: Option<tokio::fs::File>,
}
