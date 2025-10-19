use std::{fmt::Debug, path::PathBuf, sync::Arc, vec};
use tokio::{fs::OpenOptions, sync::Mutex};

use crate::{
    errors,
    wal::{
        encode::WalRecordCodec,
        record::WalRecord,
        segment::{WALSegmentWriteHandle, WalSegmentID},
        state::{WALStateWriteHandles, WalGlobalState},
    },
};

pub mod encode;
pub mod record;
pub mod segment;
pub mod state;

pub const WAL_SEGMENT_SIZE: usize = 1024 * 1024 * 32; // 32MB
#[cfg(not(target_os = "linux"))]
pub static WAL_ZERO_CHUNK: [u8; WAL_SEGMENT_SIZE] = [0u8; WAL_SEGMENT_SIZE];

pub const WAL_DIRECTORY: &str = "wal";
pub const WAL_STATE_PATH: &str = "wal_state.json";
pub const WAL_RECORD_HEADER_SIZE: usize = 4; // 4 bytes for record length

pub struct WALManager {
    codec: Box<dyn WalRecordCodec + Send + Sync>,
    base_path: PathBuf,
    state: WalGlobalState,
    background_fsync_duration: Option<std::time::Duration>,
    wal_write_handles: Arc<Mutex<WALSegmentWriteHandle>>,
    wal_state_write_handles: Arc<Mutex<WALStateWriteHandles>>,
}

impl WALManager {
    pub fn new(codec: Box<dyn WalRecordCodec + Send + Sync>, base_path: PathBuf) -> Self {
        Self {
            codec,
            base_path,
            state: Default::default(),
            wal_write_handles: Arc::new(Mutex::new(WALSegmentWriteHandle::empty())),
            wal_state_write_handles: Arc::new(Mutex::new(WALStateWriteHandles {
                state_file: None,
            })),
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

        *self.wal_write_handles.lock().await = WALSegmentWriteHandle::new(file).await?;

        Ok(())
    }

    async fn do_recover(&mut self) -> errors::Result<()> {
        let segment_files = self.list_segment_files().await?;

        if segment_files.is_empty() {
            return Ok(());
        }

        if let Some(last_segment_file) = segment_files.last() {
            let (records, offset) = self.scan_records(last_segment_file).await?;

            self.state.last_segment_file_offset = offset;

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
                    if !state.is_empty() {
                        if let Err(e) = state.flush() {
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
        // 1. Get Write Lock
        let write_mutex = self.wal_write_handles.clone();

        let mut write_state = write_mutex.lock().await;

        if write_state.is_empty() {
            return Err(errors::Errors::WalSegmentFileOpenError(
                "Current WAL segment file is not opened".to_string(),
            ));
        }

        // 2. Check if need to new segment file.
        // If current segment file size + new record size > WAL_SEGMENT_SIZE, create new segment file
        let current_segment_size = self.state.last_segment_file_offset;

        if current_segment_size + record.size() > WAL_SEGMENT_SIZE {
            *write_state = self.new_segment_file().await?;
        }

        // 3. Serialize the record and write (zero copy)
        let payload_start_offset = self.state.last_segment_file_offset + WAL_RECORD_HEADER_SIZE;

        let new_record_id = self.state.last_record_id + 1;
        record.record_id = new_record_id;

        let payload_size = self
            .codec
            .encode(&record, &mut write_state.mmap[payload_start_offset..])?;

        // 4. Set the header value
        let header_start_offset = self.state.last_segment_file_offset;
        let header_end_offset = self.state.last_segment_file_offset + WAL_RECORD_HEADER_SIZE;

        let header = (payload_size as u32).to_be_bytes();
        write_state.mmap[header_start_offset..header_end_offset].copy_from_slice(&header);

        let total_bytes = payload_size + WAL_RECORD_HEADER_SIZE;

        self.state.last_record_id = new_record_id;
        self.state.last_segment_file_offset += total_bytes;

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

    async fn new_segment_file(&mut self) -> errors::Result<WALSegmentWriteHandle> {
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

            use crate::wal::segment::WALSegmentWriteHandle;

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

            WALSegmentWriteHandle::new(file).await
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

            Ok(WALSegmentWriteHandle::new(file).await?)
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

impl Debug for WALManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WALManager")
            .field("base_path", &self.base_path)
            .field("state", &self.state)
            .finish()
    }
}
