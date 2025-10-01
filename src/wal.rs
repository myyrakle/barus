use std::{io::Write, path::Path};

use crate::errors;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WalRecord {
    pub record_id: u64,
    pub record_type: RecordType,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum RecordType {
    #[serde(rename = "put")]
    Put,
    #[serde(rename = "delete")]
    Delete,
}

pub trait WalRecordCodec {
    fn encode(&self, record: &WalRecord) -> errors::Result<Vec<u8>>;
    fn decode(&self, data: &[u8]) -> errors::Result<WalRecord>;
}

pub struct WalRecordJsonCodec;

impl WalRecordCodec for WalRecordJsonCodec {
    fn encode(&self, record: &WalRecord) -> errors::Result<Vec<u8>> {
        serde_json::to_vec(record).map_err(|e| errors::Errors::WalRecordEncodeError(e.to_string()))
    }

    fn decode(&self, data: &[u8]) -> errors::Result<WalRecord> {
        serde_json::from_slice(data)
            .map_err(|e| errors::Errors::WalRecordDecodeError(e.to_string()))
    }
}

pub const WAL_SEGMENT_SIZE: usize = 1024 * 1024 * 16; // 16MB

pub const WAL_DIRECTORY: &str = "wal";

pub struct WALManager {
    codec: Box<dyn WalRecordCodec + Send + Sync>,
}

impl WALManager {
    pub fn new(codec: Box<dyn WalRecordCodec + Send + Sync>) -> Self {
        Self { codec }
    }

    async fn get_current_segment_file_name(&self) -> errors::Result<String> {
        unimplemented!("<Get last WAL segment file name logic>");
    }

    async fn get_current_segment_file_size(&self) -> errors::Result<usize> {
        unimplemented!("<Get last WAL segment file size logic>");
    }

    async fn new_segment_file(&self) -> errors::Result<()> {
        unimplemented!("<Create new WAL segment file name logic>");
    }

    pub async fn append(&self, record: &WalRecord) -> errors::Result<()> {
        let encoded = self.codec.encode(record)?;

        let current_segment_size = self.get_current_segment_file_size().await?;

        if current_segment_size + encoded.len() > WAL_SEGMENT_SIZE {
            self.new_segment_file().await?;
        }

        let segment_file_name = self.get_current_segment_file_name().await?;
        let segment_file_path = Path::new(WAL_DIRECTORY).join(segment_file_name);

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(segment_file_path)
            .map_err(|e| errors::Errors::WalRecordWriteError(e.to_string()))?;

        file.write_all(&encoded)
            .map_err(|e| errors::Errors::WalRecordWriteError(e.to_string()))?;
        file.write_all(b"\n")
            .map_err(|e| errors::Errors::WalRecordWriteError(e.to_string()))?;

        // fsync
        file.sync_all()
            .map_err(|e| errors::Errors::WalRecordWriteError(e.to_string()))?;

        Ok(())
    }

    pub fn checkpoint(&self) -> errors::Result<()> {
        unimplemented!("<Checkpoint logic>");
    }
}
