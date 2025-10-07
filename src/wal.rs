use std::{io::Write, path::Path};

use crate::errors;

pub const WAL_SEGMENT_SIZE: usize = 1024 * 1024 * 16; // 16MB

pub const WAL_DIRECTORY: &str = "wal";
pub const WAL_STATE_PATH: &str = "wal/state.json";

#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct WalGlobalState {
    pub last_record_id: u64,
    pub last_segment_id: u64,
    pub last_checkpoint_segment_id: u64,
    pub last_checkpoint_record_id: u64,
}

impl WalGlobalState {
    // Load the WAL global state from a file (if exists)
    pub async fn load(base_path: &Path) -> errors::Result<Self> {
        let wal_state_path = base_path.join(WAL_STATE_PATH);

        if !wal_state_path.exists() {
            return Err(errors::Errors::WalStateReadError(
                "WAL state file does not exist".to_string(),
            ));
        }

        let data = std::fs::read(wal_state_path)
            .map_err(|e| errors::Errors::WalStateReadError(e.to_string()))?;
        let state = serde_json::from_slice(&data)
            .map_err(|e| errors::Errors::WalStateDecodeError(e.to_string()))?;

        Ok(state)
    }

    // Save the WAL global state to a file
    pub async fn save(&self, base_path: &Path) -> errors::Result<()> {
        let wal_state_path = base_path.join(WAL_STATE_PATH);
        let data = serde_json::to_vec_pretty(self)
            .map_err(|e| errors::Errors::WalStateDecodeError(e.to_string()))?;

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(wal_state_path)
            .map_err(|e| errors::Errors::WalStateReadError(e.to_string()))?;

        file.write_all(&data)
            .map_err(|e| errors::Errors::WalStateWriteError(e.to_string()))?;
        file.sync_all()
            .map_err(|e| errors::Errors::WalStateWriteError(e.to_string()))?;

        Ok(())
    }
}

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

pub struct WALManager {
    codec: Box<dyn WalRecordCodec + Send + Sync>,
    state: WalGlobalState,
}

impl WALManager {
    pub fn new(codec: Box<dyn WalRecordCodec + Send + Sync>) -> Self {
        Self {
            codec,
            state: Default::default(),
        }
    }

    pub async fn initialize(&self) -> errors::Result<()> {
        let state = if Path::new(WAL_STATE_PATH).exists() {
            let data = std::fs::read(WAL_STATE_PATH)
                .map_err(|e| errors::Errors::WalStateReadError(e.to_string()))?;
            serde_json::from_slice(&data)
                .map_err(|e| errors::Errors::WalStateDecodeError(e.to_string()))?
        } else {
            WalGlobalState {
                last_record_id: 0,
                last_segment_id: 0,
                last_checkpoint_segment_id: 0,
                last_checkpoint_record_id: 0,
            }
        };

        unimplemented!("<WAL Initialization logic>");
    }

    // Append a new record to the WAL
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

    // read records from the WAL
    // read range: last_checkpoint_record_id < records... <= last_record_id
    pub async fn read_records(&self) -> errors::Result<Vec<WalRecord>> {
        unimplemented!("<WAL Read records logic>");
    }

    pub fn checkpoint(&self) -> errors::Result<()> {
        unimplemented!("<Checkpoint logic>");
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
}
