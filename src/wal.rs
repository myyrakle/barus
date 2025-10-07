use std::{
    io::Write,
    path::{Path, PathBuf},
};

use crate::errors;

pub const WAL_SEGMENT_SIZE: usize = 1024 * 1024 * 16; // 16MB

pub const WAL_DIRECTORY: &str = "wal";
pub const WAL_STATE_PATH: &str = "wal/state.json";

#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct WalGlobalState {
    pub last_record_id: u64,
    pub last_checkpoint_record_id: u64,
    pub last_segment_id: u32,
    pub last_checkpoint_segment_id: u32,
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
            .map_err(|e| errors::Errors::WalRecordEncodeError(e.to_string()))?;

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(wal_state_path)
            .map_err(|e| errors::Errors::WalStateWriteError(e.to_string()))?;

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
    base_path: PathBuf,
    state: WalGlobalState,
}

impl WALManager {
    pub fn new(codec: Box<dyn WalRecordCodec + Send + Sync>, base_path: PathBuf) -> Self {
        Self {
            codec,
            base_path,
            state: Default::default(),
        }
    }

    // Initialize the WAL system (create directories, load state, etc.)
    pub async fn initialize(&self) -> errors::Result<()> {
        // 1. create WAL directory if not exists
        let wal_dir_path = self.base_path.join(WAL_DIRECTORY);
        if !wal_dir_path.exists() {
            std::fs::create_dir_all(&wal_dir_path)
                .map_err(|e| errors::Errors::WalInitializationError(e.to_string()))?;
        }

        // 2. create WAL state file if not exists
        let wal_state_path = self.base_path.join(WAL_STATE_PATH);
        if !wal_state_path.exists() {
            let initial_state = WalGlobalState::default();
            initial_state.save(&self.base_path).await?;
        }

        Ok(())
    }

    // Load WAL states from the state file
    pub async fn load(&mut self) -> errors::Result<()> {
        // Load the WAL global state from the state file
        self.state = WalGlobalState::load(&self.base_path).await?;

        Ok(())
    }

    // Append a new record to the WAL
    pub async fn append(&mut self, record: &WalRecord) -> errors::Result<()> {
        let encoded = self.codec.encode(record)?;

        let current_segment_size = self.get_current_segment_file_size().await?;

        if current_segment_size + encoded.len() > WAL_SEGMENT_SIZE {
            self.new_segment_file().await?;
        }

        let segment_file_name = self.get_current_segment_file_name().await?;
        let segment_file_path = self.base_path.join(WAL_DIRECTORY).join(segment_file_name);

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
    pub async fn read_records(
        &self,
        _start_segment_id: u32, // need state.last_checkpoint_segment_id
        _end_segment_id: u32,   // need state.last_segment_id
        _start_record_id: u64,  // need state.last_checkpoint_record_id
        _end_record_id: u64,    // need state.last_record_id
    ) -> errors::Result<Vec<WalRecord>> {
        unimplemented!("<WAL Read records logic>");
    }

    // Move the checkpoint to the specified segment and record ID
    pub async fn move_checkpoint(&mut self, segment_id: u32, record_id: u64) -> errors::Result<()> {
        self.state.last_checkpoint_segment_id = segment_id;
        self.state.last_checkpoint_record_id = record_id;
        self.state.save(&self.base_path).await?;

        Ok(())
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
