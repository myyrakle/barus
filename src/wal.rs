use std::{
    fmt::Debug,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use tokio::{fs::OpenOptions, sync::Mutex};

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

    pub async fn get_file_init_handle(&self, base_path: &Path) -> errors::Result<tokio::fs::File> {
        let wal_state_path = base_path.join(WAL_STATE_PATH);

        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(wal_state_path)
            .await
            .map_err(|e| errors::Errors::WalStateWriteError(e.to_string()))?;

        Ok(file)
    }

    pub async fn get_file_handle(&self, base_path: &Path) -> errors::Result<tokio::fs::File> {
        let wal_state_path = base_path.join(WAL_STATE_PATH);

        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(wal_state_path)
            .await
            .map_err(|e| errors::Errors::WalStateWriteError(e.to_string()))?;

        Ok(file)
    }

    // Save the WAL global state to a file
    pub async fn save(&self, file_handle: &mut tokio::fs::File) -> errors::Result<()> {
        let data = serde_json::to_vec_pretty(self)
            .map_err(|e| errors::Errors::WalRecordEncodeError(e.to_string()))?;

        file_handle
            .seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|e| errors::Errors::WalStateWriteError(e.to_string()))?;

        file_handle
            .write_all(&data)
            .await
            .map_err(|e| errors::Errors::WalStateWriteError(e.to_string()))?;

        file_handle
            .set_len(data.len() as u64)
            .await
            .map_err(|e| errors::Errors::WalStateWriteError(e.to_string()))?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WalRecord {
    pub record_id: u64,
    pub record_type: RecordType,
    pub data: String,
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
    background_fsync_duration: Option<std::time::Duration>,
    always_use_fsync: bool,
    wal_write_handles: Arc<Mutex<WALWriteHandles>>,
    wal_state_write_handles: Arc<Mutex<WALStateWriteHandles>>,
}

pub struct WALWriteHandles {
    current_segment_file: Option<tokio::fs::File>,
}

pub struct WALStateWriteHandles {
    state_file: Option<tokio::fs::File>,
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
            wal_write_handles: Arc::new(Mutex::new(WALWriteHandles {
                current_segment_file: None,
            })),
            wal_state_write_handles: Arc::new(Mutex::new(WALStateWriteHandles {
                state_file: None,
            })),
            always_use_fsync: false,
            background_fsync_duration: Some(std::time::Duration::from_secs(10)),
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

            let mut file_handle = initial_state.get_file_init_handle(&self.base_path).await?;
            initial_state.save(&mut file_handle).await?;
        }

        // 3. create initial segment file if wal directory is empty
        let entries = std::fs::read_dir(&wal_dir_path)
            .map_err(|e| errors::Errors::WalInitializationError(e.to_string()))?;

        if entries.count() == 0 {
            let segment_file_name = format!("{:024X}", 0u128);
            let segment_file_path = wal_dir_path.join(segment_file_name);

            let file = OpenOptions::new()
                .read(true)
                .create(true)
                .truncate(true)
                .write(true)
                .open(&segment_file_path)
                .await
                .map_err(|e| {
                    errors::Errors::WalSegmentFileOpenError(format!(
                        "Failed to open WAL segment file: {}",
                        e
                    ))
                })?;

            file.set_len(WAL_SEGMENT_SIZE as u64).await.map_err(|e| {
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
        {
            self.wal_state_write_handles.lock().await.state_file =
                Some(self.state.get_file_handle(&self.base_path).await?);
        }

        // TODO: recovery last_record_id & last_segment_file_offset by scanning the last segment file

        // Load file stream for the current segment
        let segment_file_name = self.get_current_segment_file_name()?;
        let segment_file_path = self.base_path.join(WAL_DIRECTORY).join(segment_file_name);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&segment_file_path)
            .await
            .map_err(|e| {
                errors::Errors::WalSegmentFileOpenError(format!(
                    "Failed to open WAL segment file: {}",
                    e
                ))
            })?;

        self.wal_write_handles.lock().await.current_segment_file = Some(file);

        Ok(())
    }

    pub fn start_background(&self) -> errors::Result<()> {
        if let Some(duration) = self.background_fsync_duration {
            let write_state = self.wal_write_handles.clone();

            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(duration).await;

                    let state = write_state.lock().await;

                    // fsync current segment file
                    #[allow(clippy::collapsible_if)]
                    if let Some(file) = &state.current_segment_file {
                        if let Err(e) = file.sync_all().await {
                            eprintln!("Failed to fsync WAL segment file: {}", e);
                            // Handle error (e.g., retry, log, etc.)
                        }
                    }
                }
            });
        }

        Ok(())
    }

    // Append a new record to the WAL
    pub async fn append(&mut self, mut record: WalRecord) -> errors::Result<()> {
        // 1. Serialize the record (thread free)
        let new_record_id = self.state.last_record_id + 1;
        record.record_id = new_record_id;
        let encoded = self.codec.encode(&record)?;

        let payload_size = encoded.len();
        let header_bytes = (payload_size as u32).to_be_bytes();

        let total_bytes = payload_size + header_bytes.len();

        // 2. Get Write Lock
        let write_mutex = self.wal_write_handles.clone();

        let mut write_state = write_mutex.lock().await;

        // 3. Check if need to new segment file.
        // If current segment file size + new record size > WAL_SEGMENT_SIZE, create new segment file
        let current_segment_size = self.get_current_segment_file_size()?;

        if current_segment_size + encoded.len() > WAL_SEGMENT_SIZE {
            write_state.current_segment_file = Some(self.new_segment_file().await?);
        }

        let file = write_state.current_segment_file.as_mut().ok_or_else(|| {
            errors::Errors::WalSegmentFileOpenError(
                "Current WAL segment file is not opened".to_string(),
            )
        })?;

        // 4. Write the record (header + payload)
        file.write_all(&header_bytes)
            .await
            .map_err(|e| errors::Errors::WalRecordWriteError(e.to_string()))?;
        file.write_all(&encoded)
            .await
            .map_err(|e| errors::Errors::WalRecordWriteError(e.to_string()))?;

        // 5. Update WAL state
        self.state.last_segment_file_offset += total_bytes as u64;
        self.state.last_record_id = new_record_id;

        self.save_state().await?;

        // 5. fsync (Optional)
        if self.always_use_fsync {
            file.sync_all()
                .await
                .map_err(|e| errors::Errors::WalRecordWriteError(e.to_string()))?;
        }

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
        self.save_state().await?;

        Ok(())
    }

    fn get_current_segment_file_name(&self) -> errors::Result<String> {
        let segment_id_str: String = (&self.state.last_segment_id).into();
        Ok(segment_id_str)
    }

    fn get_current_segment_file_size(&self) -> errors::Result<usize> {
        self.state.last_segment_file_offset.try_into().map_err(|e| {
            errors::Errors::WalSegmentFileOpenError(format!(
                "Failed to get current segment file size: {}",
                e
            ))
        })
    }

    async fn new_segment_file(&mut self) -> errors::Result<tokio::fs::File> {
        self.state.last_segment_id.increment();

        let new_segment_id = &self.state.last_segment_id;
        let new_segment_id_str: String = new_segment_id.into();

        let new_segment_file_path = self.base_path.join(WAL_DIRECTORY).join(new_segment_id_str);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&new_segment_file_path)
            .await
            .map_err(|e| {
                errors::Errors::WalSegmentFileOpenError(format!(
                    "Failed to create new WAL segment file: {}",
                    e
                ))
            })?;

        file.set_len(WAL_SEGMENT_SIZE as u64).await.map_err(|e| {
            errors::Errors::WalSegmentFileOpenError(format!(
                "Failed to set length for new WAL segment file: {}",
                e
            ))
        })?;
        self.state.last_segment_file_offset = 0;

        self.save_state().await?;

        Ok(file)
    }

    async fn save_state(&self) -> errors::Result<()> {
        let mut state_handles = self.wal_state_write_handles.lock().await;

        let file = state_handles.state_file.as_mut().ok_or_else(|| {
            errors::Errors::WalStateWriteError("WAL state file is not opened".to_string())
        })?;

        self.state.save(file).await
    }
}
