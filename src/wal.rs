use bincode::config::{Configuration, Fixint, LittleEndian, NoLimit};
use std::{
    fmt::Debug,
    path::{Path, PathBuf},
    sync::Arc,
    vec,
};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use tokio::{fs::OpenOptions, sync::Mutex};

use crate::errors;

pub const WAL_SEGMENT_SIZE: usize = 1024 * 1024 * 32; // 32MB
pub const WAL_ZERO_CHUNK: [u8; WAL_SEGMENT_SIZE] = [0u8; WAL_SEGMENT_SIZE];
pub const WAL_WRITE_BUFFER_SIZE: usize = WAL_RECORD_HEADER_SIZE + 10 * 1024 * 1024; // 최대 10MB 레코드 지원

pub const WAL_DIRECTORY: &str = "wal";
pub const WAL_STATE_PATH: &str = "wal_state.json";
pub const WAL_RECORD_HEADER_SIZE: usize = 4; // 4 bytes for record length

// 24 length hex ID (ex 0000000D000000EA)
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct WalSegmentID(u64);

impl std::ops::Add<u64> for WalSegmentID {
    type Output = WalSegmentID;

    fn add(self, rhs: u64) -> Self::Output {
        WalSegmentID(self.0 + rhs)
    }
}

impl WalSegmentID {
    pub fn new(id: u64) -> Self {
        WalSegmentID(id)
    }

    pub fn increment(&mut self) {
        self.0 += 1;
    }
}

impl From<WalSegmentID> for u64 {
    fn from(val: WalSegmentID) -> Self {
        val.0
    }
}

impl From<&WalSegmentID> for String {
    fn from(val: &WalSegmentID) -> Self {
        format!("{:016X}", val.0)
    }
}

impl TryFrom<&str> for WalSegmentID {
    type Error = errors::Errors;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() != 16 {
            return Err(errors::Errors::WalSegmentIDParseError(
                "Invalid segment ID length".to_string(),
            ));
        }

        let id = u64::from_str_radix(value, 16).map_err(|e| {
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
        let data = serde_json::to_vec(self)
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

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub struct WalPayload {
    pub table: String,
    pub key: String,
    pub value: Option<String>,
}

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub struct WalRecord {
    pub record_id: u64,
    pub record_type: RecordType,
    pub data: WalPayload,
}

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub enum RecordType {
    #[serde(rename = "put")]
    Put,
    #[serde(rename = "delete")]
    Delete,
}

pub trait WalRecordCodec {
    fn encode(&self, record: &WalRecord, buf: &mut [u8]) -> errors::Result<usize>;
    fn decode(&self, data: &[u8]) -> errors::Result<WalRecord>;
}

pub struct WalRecordBincodeCodec;

impl WalRecordBincodeCodec {
    const CONFIG: Configuration<LittleEndian, Fixint, NoLimit> = bincode::config::standard()
        .with_fixed_int_encoding()
        .with_little_endian()
        .with_no_limit();
}

impl WalRecordCodec for WalRecordBincodeCodec {
    fn encode(&self, record: &WalRecord, buf: &mut [u8]) -> errors::Result<usize> {
        // bincode 2.x uses encode_to_vec with config
        bincode::encode_into_slice(record, buf, Self::CONFIG)
            .map_err(|e| errors::Errors::WalRecordEncodeError(e.to_string()))
    }

    fn decode(&self, data: &[u8]) -> errors::Result<WalRecord> {
        // bincode 2.x uses decode_from_slice with config
        let (decoded, _len): (WalRecord, usize) = bincode::decode_from_slice(data, Self::CONFIG)
            .map_err(|e| errors::Errors::WalRecordDecodeError(e.to_string()))?;
        Ok(decoded)
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
    wal_write_buffer: Vec<u8>,
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
        let wal_write_buffer = vec![0u8; WAL_WRITE_BUFFER_SIZE];

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
            wal_write_buffer,
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
            let segment_file_name: String = (&WalSegmentID::new(0u64)).into();
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

        // last_record_id & last_segment_file_offset by scanning the last segment file
        self.do_recover().await?;

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

    async fn do_recover(&mut self) -> errors::Result<()> {
        let segment_files = self.list_segment_files().await?;

        if segment_files.is_empty() {
            return Ok(());
        }

        if let Some(last_segment_file) = segment_files.last() {
            let (records, offset) = self.scan_records(last_segment_file).await?;

            self.state.last_segment_file_offset = offset as u64;

            if let Some(last_record) = records.last() {
                self.state.last_record_id = last_record.record_id;
            }

            self.save_state().await?;
        }

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
                        if let Err(e) = file.sync_data().await {
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

        let payload_size = self.codec.encode(
            &record,
            &mut self.wal_write_buffer[WAL_RECORD_HEADER_SIZE..],
        )?;

        let header_bytes = (payload_size as u32).to_be_bytes();
        self.wal_write_buffer[0..WAL_RECORD_HEADER_SIZE].copy_from_slice(&header_bytes);

        let total_bytes = payload_size + header_bytes.len();

        // 2. Get Write Lock
        let write_mutex = self.wal_write_handles.clone();

        let mut write_state = write_mutex.lock().await;

        // 3. Check if need to new segment file.
        // If current segment file size + new record size > WAL_SEGMENT_SIZE, create new segment file
        let current_segment_size = self.get_current_segment_file_size()?;

        if current_segment_size + total_bytes > WAL_SEGMENT_SIZE {
            write_state.current_segment_file = Some(self.new_segment_file().await?);
        }

        let file = write_state.current_segment_file.as_mut().ok_or_else(|| {
            errors::Errors::WalSegmentFileOpenError(
                "Current WAL segment file is not opened".to_string(),
            )
        })?;

        // 4. Write the record (header + payload)
        file.write_all(&self.wal_write_buffer[..total_bytes])
            .await
            .map_err(|e| {
                errors::Errors::WalRecordWriteError(format!("Failed to write WAL record: {}", e))
            })?;

        // 5. datasync (Optional)
        if self.always_use_fsync {
            file.sync_data()
                .await
                .map_err(|e| errors::Errors::WalRecordWriteError(e.to_string()))?;
        }

        self.state.last_record_id = new_record_id;
        self.state.last_segment_file_offset += total_bytes as u64;

        Ok(())
    }

    // listup WAL segment files
    pub async fn list_segment_files(&self) -> errors::Result<Vec<String>> {
        let wal_dir = self.base_path.join(WAL_DIRECTORY);

        // 1. 모든 세그먼트 파일 읽기 (파일만 필터링해서 파일명 반환)
        let mut segment_files: Vec<_> = std::fs::read_dir(&wal_dir)
            .map_err(|e| {
                errors::Errors::WalSegmentFileOpenError(format!(
                    "Failed to read WAL directory: {}",
                    e
                ))
            })?
            .filter_map(|entry| {
                entry.ok().and_then(|e| {
                    let path = e.path();
                    if path.is_file() {
                        path.file_name()
                            .and_then(|name| name.to_str().map(|s| s.to_string()))
                    } else {
                        None
                    }
                })
            })
            .collect();

        // 2. 파일명 기준 정렬
        segment_files.sort();

        Ok(segment_files)
    }

    // read records from the WAL
    pub async fn scan_records(
        &self,
        segment_file: &str,
    ) -> errors::Result<(Vec<WalRecord>, usize)> {
        let segment_file_path = self.base_path.join(WAL_DIRECTORY).join(segment_file);

        let bytes = tokio::fs::read(&segment_file_path).await.map_err(|e| {
            errors::Errors::WalSegmentFileOpenError(format!(
                "Failed to read WAL segment file: {}",
                e
            ))
        })?;

        let mut records = vec![];

        let mut offset = 0;
        while offset + WAL_RECORD_HEADER_SIZE <= bytes.len() {
            let header_bytes = &bytes[offset..offset + WAL_RECORD_HEADER_SIZE];
            let payload_size = u32::from_be_bytes(header_bytes.try_into().unwrap()) as usize;

            if payload_size == 0 {
                break; // No more valid records
            }

            offset += WAL_RECORD_HEADER_SIZE;

            if offset + payload_size > bytes.len() {
                break; // Incomplete record, stop processing
            }

            let payload_bytes = &bytes[offset..offset + payload_size];

            let record = self.codec.decode(payload_bytes)?;

            records.push(record);
            offset += payload_size;
        }

        Ok((records, offset))
    }

    // Move the checkpoint to the specified segment and record ID
    pub async fn move_checkpoint(&mut self, segment_id: u64, record_id: u64) -> errors::Result<()> {
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

        #[cfg(target_os = "linux")]
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&new_segment_file_path)
                .await
                .map_err(|e| {
                    errors::Errors::WalSegmentFileOpenError(format!(
                        "Failed to create new WAL segment file: {}",
                        e
                    ))
                })?;

            use std::os::fd::{AsFd, AsRawFd};

            let fd = file.as_fd().as_raw_fd();
            let result = unsafe {
                libc::fallocate(
                    fd,
                    0, // flags = 0 은 공간만 할당 (초기화 안됨)
                    0,
                    WAL_SEGMENT_SIZE as i64,
                )
            };

            if result != 0 {
                return Err(errors::Errors::WalSegmentFileOpenError(format!(
                    "Failed to allocate space for new WAL segment file: {}",
                    std::io::Error::last_os_error()
                )));
            }

            let result = unsafe {
                libc::fallocate(
                    fd,
                    libc::FALLOC_FL_ZERO_RANGE, // 0으로 채우기
                    0,
                    WAL_SEGMENT_SIZE as i64,
                )
            };

            if result != 0 {
                return Err(errors::Errors::WalSegmentFileOpenError(format!(
                    "Failed to zero-fill new WAL segment file: {}",
                    std::io::Error::last_os_error()
                )));
            }

            self.state.last_segment_file_offset = 0;

            Ok(file)
        }

        #[cfg(not(target_os = "linux"))]
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
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

            file.write_all(&WAL_ZERO_CHUNK).await.map_err(|e| {
                errors::Errors::WalSegmentFileOpenError(format!(
                    "Failed to zero-fill new WAL segment file: {}",
                    e
                ))
            })?;

            file.seek(std::io::SeekFrom::Start(0))
                .await
                .map_err(|e| errors::Errors::WalSegmentFileOpenError(e.to_string()))?;

            self.state.last_segment_file_offset = 0;

            Ok(file)
        }
    }

    async fn save_state(&self) -> errors::Result<()> {
        let mut state_handles = self.wal_state_write_handles.lock().await;

        let file = state_handles.state_file.as_mut().ok_or_else(|| {
            errors::Errors::WalStateWriteError("WAL state file is not opened".to_string())
        })?;

        self.state.save(file).await
    }
}
