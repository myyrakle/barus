use std::{
    fmt::Debug,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

use crate::errors;

pub const WAL_SEGMENT_SIZE: usize = 1024 * 1024 * 16; // 16MB

pub const WAL_DIRECTORY: &str = "wal";
pub const WAL_STATE_PATH: &str = "wal_state.json";

// 24 length hex ID (ex 000000010000000D000000EA)
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct WalSegmentID(u128);

impl std::ops::Add<u128> for WalSegmentID {
    type Output = WalSegmentID;

    fn add(self, rhs: u128) -> Self::Output {
        WalSegmentID(self.0 + rhs)
    }
}

impl WalSegmentID {
    pub fn new(id: u128) -> Self {
        WalSegmentID(id)
    }

    pub fn increment(&mut self) {
        self.0 += 1;
    }
}

impl From<WalSegmentID> for u128 {
    fn from(val: WalSegmentID) -> Self {
        val.0
    }
}

impl From<&WalSegmentID> for String {
    fn from(val: &WalSegmentID) -> Self {
        format!("{:024X}", val.0)
    }
}

impl TryFrom<&str> for WalSegmentID {
    type Error = errors::Errors;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() != 24 {
            return Err(errors::Errors::WalSegmentIDParseError(
                "Invalid segment ID length".to_string(),
            ));
        }

        let id = u128::from_str_radix(value, 16).map_err(|e| {
            errors::Errors::WalSegmentIDParseError(format!("Failed to parse segment ID: {}", e))
        })?;

        Ok(WalSegmentID(id))
    }
}

#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct WalRecordID(u64);

#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct WalGlobalState {
    pub last_record_id: u64,
    pub last_checkpoint_record_id: u64,
    pub last_segment_id: WalSegmentID,
    pub last_checkpoint_segment_id: WalSegmentID,
    pub last_segment_file_offset: u64,
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
    current_segment_file: Option<std::fs::File>,
}

impl Debug for WALManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WALManager")
            .field("base_path", &self.base_path)
            .field("state", &self.state)
            .finish()
    }
}

impl WALManager {
    pub fn new(codec: Box<dyn WalRecordCodec + Send + Sync>, base_path: PathBuf) -> Self {
        Self {
            codec,
            base_path,
            state: Default::default(),
            current_segment_file: None,
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

        // 3. create initial segment file if wal directory is empty
        let entries = std::fs::read_dir(&wal_dir_path)
            .map_err(|e| errors::Errors::WalInitializationError(e.to_string()))?;

        if entries.count() == 0 {
            let segment_file_name = format!("{:024X}", 0u128);
            let segment_file_path = wal_dir_path.join(segment_file_name);

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&segment_file_path)
                .map_err(|e| {
                    errors::Errors::WalSegmentFileOpenError(format!(
                        "Failed to open WAL segment file: {}",
                        e
                    ))
                })?;

            file.set_len(WAL_SEGMENT_SIZE as u64).map_err(|e| {
                errors::Errors::WalSegmentFileOpenError(format!(
                    "Failed to set length for initial WAL segment file: {}",
                    e
                ))
            })?;
        }

        Ok(())
    }

    // Load WAL states from the state file
    pub async fn load(&mut self) -> errors::Result<()> {
        // Load the WAL global state from the state file
        self.state = WalGlobalState::load(&self.base_path).await?;

        // Load file stream for the current segment
        let segment_file_name = self.get_current_segment_file_name()?;
        let segment_file_path = self.base_path.join(WAL_DIRECTORY).join(segment_file_name);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&segment_file_path)
            .map_err(|e| {
                errors::Errors::WalSegmentFileOpenError(format!(
                    "Failed to open WAL segment file: {}",
                    e
                ))
            })?;

        self.current_segment_file = Some(file);

        Ok(())
    }

    // Append a new record to the WAL
    pub async fn append(&mut self, record: &WalRecord) -> errors::Result<()> {
        let encoded = self.codec.encode(record)?;

        let current_segment_size = self.get_current_segment_file_size().await?;

        if current_segment_size + encoded.len() > WAL_SEGMENT_SIZE {
            self.new_segment_file().await?;
        }

        let segment_file_name = self.get_current_segment_file_name()?;
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

        // TODO: record ID management

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
    pub async fn move_checkpoint(
        &mut self,
        segment_id: u128,
        record_id: u64,
    ) -> errors::Result<()> {
        self.state.last_checkpoint_segment_id = WalSegmentID::new(segment_id);
        self.state.last_checkpoint_record_id = record_id;
        self.state.save(&self.base_path).await?;

        Ok(())
    }

    fn get_current_segment_file_name(&self) -> errors::Result<String> {
        let segment_id_str: String = (&self.state.last_segment_id).into();
        Ok(segment_id_str)
    }

    async fn get_current_segment_file_size(&self) -> errors::Result<usize> {
        unimplemented!("<Get last WAL segment file size logic>");
    }

    async fn new_segment_file(&mut self) -> errors::Result<()> {
        self.state.last_segment_id.increment();

        let new_segment_id = &self.state.last_segment_id;
        let new_segment_id_str: String = new_segment_id.into();

        let new_segment_file_path = self.base_path.join(WAL_DIRECTORY).join(new_segment_id_str);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&new_segment_file_path)
            .map_err(|e| {
                errors::Errors::WalSegmentFileOpenError(format!(
                    "Failed to create new WAL segment file: {}",
                    e
                ))
            })?;

        file.set_len(WAL_SEGMENT_SIZE as u64).map_err(|e| {
            errors::Errors::WalSegmentFileOpenError(format!(
                "Failed to set length for new WAL segment file: {}",
                e
            ))
        })?;
        self.state.last_segment_file_offset = 0;

        self.state.save(&self.base_path).await?;

        self.current_segment_file = Some(file);

        Ok(())
    }
}
