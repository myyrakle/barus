use std::{collections::HashMap, fmt::Debug, io::SeekFrom, path::PathBuf, sync::Arc};

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, RwLock},
};

use crate::{
    config::{
        DISKTABLE_PAGE_SIZE, DISKTABLE_SEGMENT_SIZE, TABLE_SEGMENT_RECORD_HEADER_SIZE,
        TABLES_DIRECTORY, TABLES_SEGMENT_DIRECTORY,
    },
    disktable::segment::{
        encode::{TableRecordBincodeCodec, TableRecordCodec},
        position::TableRecordPosition,
        record::{RecordStateFlags, TableSegmentPayload},
        segment_id::TableSegmentID,
        state::TableSegmentState,
    },
    errors,
    os::file_resize_and_set_zero,
};

pub mod encode;
pub mod position;
pub mod record;
pub mod segment_id;
pub mod state;

#[derive(Debug)]
pub struct TableSegmentManager {
    codec: Box<dyn TableRecordCodec + Send + Sync>,
    base_path: PathBuf,
    tables_map: Arc<Mutex<HashMap<String, TableSegmentState>>>,
    file_rw_lock: Arc<Mutex<HashMap<String, Arc<RwLock<()>>>>>,
}

impl TableSegmentManager {
    pub fn new(base_path: PathBuf) -> Self {
        Self {
            base_path,
            tables_map: Arc::new(Mutex::new(HashMap::new())),
            file_rw_lock: Arc::new(Mutex::new(HashMap::new())),
            codec: Box::new(TableRecordBincodeCodec {}),
        }
    }

    // Table Initialization
    pub async fn initialize_table(&self, table_name: &str) -> errors::Result<()> {
        let mut tables_map = self.tables_map.lock().await;
        let _ = tables_map
            .entry(table_name.to_owned())
            .or_insert_with(TableSegmentState::default);

        Ok(())
    }

    // Truncate table (delete all segment files and recreate)
    pub async fn truncate_table(&self, table_name: &str) -> errors::Result<()> {
        let segments_directory = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(table_name)
            .join(TABLES_SEGMENT_DIRECTORY);

        // 1. remove all segment files
        tokio::fs::remove_dir_all(&segments_directory)
            .await
            .or_else(|e| {
                if e.kind() != std::io::ErrorKind::NotFound {
                    Err(errors::Errors::new(errors::ErrorCodes::FileDeleteError)
                        .with_message(format!("Failed to delete segment files: {}", e)))
                } else {
                    Ok(())
                }
            })?;

        // 2. recreate segments directory
        tokio::fs::create_dir_all(&segments_directory)
            .await
            .or_else(|e| {
                if e.kind() != std::io::ErrorKind::AlreadyExists {
                    Err(
                        errors::Errors::new(errors::ErrorCodes::FileDeleteError).with_message(
                            format!(
                                "Failed to recreate segments directory '{}': {}",
                                segments_directory.display(),
                                e
                            ),
                        ),
                    )
                } else {
                    Ok(())
                }
            })?;

        // 3. reset table state
        let mut tables_map = self.tables_map.lock().await;
        let _ = tables_map.remove(table_name);

        Ok(())
    }

    pub async fn list_segment_files(
        &self,
        table_name: &str,
    ) -> errors::Result<Vec<ListSegmentFileResultItem>> {
        let table_directory = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(table_name)
            .join(TABLES_SEGMENT_DIRECTORY);

        // 1. 모든 세그먼트 파일 읽기 (파일만 필터링해서 파일명 반환)
        let mut segment_files: Vec<_> = std::fs::read_dir(&table_directory)
            .map_err(|e| {
                errors::Errors::new(errors::ErrorCodes::WALSegmentFileOpenError)
                    .with_message(format!("Failed to read Table Segment directory: {}", e))
            })?
            .filter_map(|entry| {
                entry.ok().and_then(|e| {
                    let path = e.path();
                    if path.is_file() {
                        let file_name = path
                            .file_name()
                            .and_then(|name| name.to_str().map(|s| s.to_string()))
                            .unwrap_or_default();

                        let Ok(file_size) = e.metadata().map(|meta| meta.len() as u32) else {
                            return None;
                        };

                        Some(ListSegmentFileResultItem {
                            file_name,
                            file_size,
                        })
                    } else {
                        None
                    }
                })
            })
            .collect();

        // 2. 파일명 기준 정렬
        segment_files
            .sort_by(|file1, file2| file1.file_name.as_str().cmp(file2.file_name.as_str()));

        Ok(segment_files)
    }

    pub async fn scan_segment_file(
        &self,
        table_name: &str,
        segment_file_name: &str,
    ) -> errors::Result<Vec<ScanSegmentFileResult>> {
        let file_path = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(table_name)
            .join(TABLES_SEGMENT_DIRECTORY)
            .join(segment_file_name);

        let mut file = File::open(&file_path).await.map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::FileOpenError).with_message(format!(
                "Failed to open file '{}': {}",
                file_path.display(),
                e
            ))
        })?;
        let metadata = file.metadata().await.map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::FileMetadataError).with_message(format!(
                "Failed to get metadata for file '{}': {}",
                file_path.display(),
                e
            ))
        })?;
        let file_size = metadata.len() as u32;

        let total_page_number = file_size / DISKTABLE_PAGE_SIZE;

        let mut scan_items = Vec::new();

        let mut page_buffer = vec![0u8; DISKTABLE_PAGE_SIZE as usize];

        for page_index in 0..total_page_number {
            file.read_exact(&mut page_buffer).await.map_err(|e| {
                errors::Errors::new(errors::ErrorCodes::FileReadError).with_message(format!(
                    "Failed to read page {} in file '{}': {}",
                    page_index,
                    file_path.display(),
                    e
                ))
            })?;

            let mut page_offset = 0_usize;

            while page_offset < DISKTABLE_PAGE_SIZE as usize {
                let page_start_offset = page_index * DISKTABLE_PAGE_SIZE;
                let real_offset = page_start_offset + page_offset as u32;

                // read from header byte
                let flag_header = page_buffer[page_offset].into();
                page_offset += 1;

                // process header byte
                match flag_header {
                    RecordStateFlags::Nothing => {
                        // end of data
                        break;
                    }
                    RecordStateFlags::Alive | RecordStateFlags::Deleted => {}
                    RecordStateFlags::Unknown => {
                        return Err(errors::Errors::new(
                            errors::ErrorCodes::UnknownTableRecordHeaderFlag,
                        ));
                    }
                }

                let size_header_bytes = [
                    page_buffer[page_offset],
                    page_buffer[page_offset + 1],
                    page_buffer[page_offset + 2],
                    page_buffer[page_offset + 3],
                ];
                let size_header = u32::from_be_bytes(size_header_bytes);
                page_offset += 4;

                let payload = &page_buffer[page_offset..page_offset + size_header as usize];
                page_offset += size_header as usize;

                let record = self.codec.decode(payload)?;

                scan_items.push(ScanSegmentFileResult {
                    state_flags: flag_header,
                    position: TableRecordPosition {
                        segment_id: TableSegmentID::try_from(segment_file_name).unwrap_or_default(),
                        offset: real_offset,
                    },
                    payload: record,
                });
            }
        }

        Ok(scan_items)
    }

    pub async fn set_table_names(&self, table_names: Vec<String>) -> errors::Result<()> {
        for table_name in table_names {
            let segment_files = self.list_segment_files(&table_name).await?;

            let last_segment_id = match segment_files.last() {
                Some(file) => TableSegmentID::try_from(file.file_name.as_str())
                    .unwrap_or(TableSegmentID::new(0)),
                None => TableSegmentID::new(0),
            };

            match last_segment_id.0 {
                0 => {
                    let mut table_map = self.tables_map.lock().await;
                    table_map.insert(table_name, TableSegmentState::default());
                }
                _ => {
                    let describe_result = self
                        .describe_segment_file(&table_name, &last_segment_id)
                        .await?;

                    let mut table_map = self.tables_map.lock().await;
                    table_map.insert(table_name, describe_result);
                }
            }
        }

        Ok(())
    }

    pub async fn describe_segment_file(
        &self,
        table_name: &str,
        segment_id: &TableSegmentID,
    ) -> errors::Result<TableSegmentState> {
        let file_name: String = segment_id.into();

        let file_path = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(table_name)
            .join(TABLES_SEGMENT_DIRECTORY)
            .join(file_name);

        let mut file = File::open(&file_path).await.map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::FileOpenError).with_message(format!(
                "Failed to open file '{}': {}",
                file_path.display(),
                e
            ))
        })?;
        let metadata = file.metadata().await.map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::FileMetadataError).with_message(format!(
                "Failed to get metadata for file '{}': {}",
                file_path.display(),
                e
            ))
        })?;
        let file_size = metadata.len() as u32;

        let total_page_number = file_size / DISKTABLE_PAGE_SIZE;
        let current_page_index = total_page_number - 1;

        let mut offset = file_size - DISKTABLE_PAGE_SIZE;

        while offset < file_size {
            let flag_header_offset = offset as u64;

            file.seek(SeekFrom::Start(flag_header_offset))
                .await
                .map_err(|e| {
                    errors::Errors::new(errors::ErrorCodes::FileSeekError).with_message(format!(
                        "Failed to seek to offset {} in file '{}': {}",
                        flag_header_offset,
                        file_path.display(),
                        e
                    ))
                })?;

            // read from header byte
            let flag_header = file
                .read_u8()
                .await
                .map_err(|e| {
                    errors::Errors::new(errors::ErrorCodes::FileReadError).with_message(format!(
                        "Failed to read header byte at offset {} in file '{}': {}",
                        flag_header_offset,
                        file_path.display(),
                        e
                    ))
                })?
                .into();

            // process header byte
            match flag_header {
                RecordStateFlags::Nothing => {
                    // end of data
                    break;
                }
                RecordStateFlags::Alive | RecordStateFlags::Deleted => {}
                RecordStateFlags::Unknown => {
                    return Err(errors::Errors::new(
                        errors::ErrorCodes::UnknownTableRecordHeaderFlag,
                    ));
                }
            }

            let size_header = file.read_u32().await.map_err(|e| {
                errors::Errors::new(errors::ErrorCodes::FileReadError).with_message(format!(
                    "Failed to read size header at offset {} in file '{}': {}",
                    flag_header_offset + 1,
                    file_path.display(),
                    e
                ))
            })?;

            offset += TABLE_SEGMENT_RECORD_HEADER_SIZE + size_header;
        }

        Ok(TableSegmentState {
            last_segment_id: segment_id.clone(),
            segment_file_size: file_size,
            current_page_index,
            current_page_offset: offset,
        })
    }

    pub async fn get_segment_file(
        &self,
        table_name: &str,
        segment_id: &TableSegmentID,
    ) -> errors::Result<File> {
        // 2. get segment file
        let segment_filename: String = segment_id.into();

        let new_segment_file_path = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(table_name)
            .join(TABLES_SEGMENT_DIRECTORY)
            .join(segment_filename);

        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .open(new_segment_file_path)
            .await
            .map_err(|err| {
                errors::Errors::new(errors::ErrorCodes::TableSegmentFileOpenError)
                    .with_message(err.to_string())
            })?;

        Ok(file)
    }

    // new segment file (DISKTABLE_PAGE_SIZE start)
    pub async fn create_segment(
        &self,
        table_name: &str,
        table_state: &mut TableSegmentState,
        size: u32,
    ) -> errors::Result<File> {
        // 2. Create new segment file
        table_state.last_segment_id.increment();
        table_state.current_page_index = 0;
        table_state.current_page_offset = 0;
        table_state.segment_file_size = size;

        let segment_filename: String = (&table_state.last_segment_id).into();

        let new_segment_file_path = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(table_name)
            .join(TABLES_SEGMENT_DIRECTORY)
            .join(segment_filename);

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(new_segment_file_path)
            .await
            .map_err(|err| {
                errors::Errors::new(errors::ErrorCodes::TableSegmentFileCreateError)
                    .with_message(err.to_string())
            })?;

        file_resize_and_set_zero(&mut file, size).await?;

        Ok(file)
    }

    // increase size of segment file
    pub async fn increase_segment(
        &self,
        table_name: &str,
        table_state: &mut TableSegmentState,
        size: u32,
    ) -> errors::Result<File> {
        let mut file = self
            .get_segment_file(table_name, &table_state.last_segment_id)
            .await?;

        // 3. Expand segment file
        file_resize_and_set_zero(&mut file, size).await?;

        table_state.current_page_offset = table_state.segment_file_size;
        table_state.current_page_index += 1;
        table_state.segment_file_size += size;

        Ok(file)
    }

    // Provides protection for segment areas that have already been created
    // (`append` is excluded from the effect).
    async fn lock_segment_file(
        &self,
        table_name: &str,
        segment_id: &TableSegmentID,
    ) -> Arc<RwLock<()>> {
        let file_key = format!("{}/{}", table_name, segment_id.0);

        {
            let mut locks_map = self.file_rw_lock.lock().await;

            locks_map
                .entry(file_key)
                .or_insert(Arc::new(RwLock::new(())))
                .clone()
        }
    }

    // Appends a record to the segment file.
    pub async fn append_record(
        &self,
        table_name: &str,
        record: TableSegmentPayload,
    ) -> errors::Result<TableRecordPosition> {
        // 1. Payload Prepare
        let encoded_bytes = self.codec.encode(&record)?;

        let state_byte = RecordStateFlags::Alive;
        let record_size = encoded_bytes.len() as u32;
        let record_size_bytes = record_size.to_be_bytes();
        assert!(record_size_bytes.len() == 4);

        let header: [u8; 5] = [
            state_byte as u8,
            record_size_bytes[0],
            record_size_bytes[1],
            record_size_bytes[2],
            record_size_bytes[3],
        ];

        let total_bytes = header.len() as u32 + encoded_bytes.len() as u32;
        let mut write_buffer = Vec::with_capacity(total_bytes as usize);
        write_buffer.extend_from_slice(&header);
        write_buffer.extend_from_slice(&encoded_bytes);

        let mut tables_map = self.tables_map.lock().await;
        let table = tables_map
            .entry(table_name.to_owned())
            .or_insert_with(TableSegmentState::default);

        // 2. If the current page is full, create new page or new segment.
        if table.current_page_offset + total_bytes > table.segment_file_size {
            // 3-a. If the segment size reaches its maximum size (or not exist), a new segment is created.
            // 3-b. If the segment size not reaches its maximum size, segment size grows. (new page)
            if table.segment_file_size == 0
                || table.segment_file_size + total_bytes > DISKTABLE_SEGMENT_SIZE
            {
                self.create_segment(table_name, table, DISKTABLE_PAGE_SIZE)
                    .await?;
            } else {
                self.increase_segment(table_name, table, DISKTABLE_PAGE_SIZE)
                    .await?;
            }
        }

        // 4. If there is enough space, write the data immediately.
        // TODO: managing file handler pool for I/O performance
        let mut file = self
            .get_segment_file(table_name, &table.last_segment_id)
            .await?;

        file.seek(SeekFrom::Start(table.current_page_offset as u64))
            .await
            .map_err(|e| {
                errors::Errors::new(errors::ErrorCodes::FileSeekError)
                    .with_message(format!("Failed to seek file: {}", e))
            })?;
        file.write_all(&write_buffer).await.map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::TableSegmentFileWriteError)
                .with_message(format!("Failed to write data: {}", e))
        })?;

        let position = TableRecordPosition {
            segment_id: table.last_segment_id.clone(),
            offset: table.current_page_offset,
        };

        table.current_page_offset += total_bytes;

        Ok(position)
    }

    // Finds a record in the segment file.
    pub async fn find_record(
        &self,
        table_name: &str,
        position: TableRecordPosition,
    ) -> errors::Result<(RecordStateFlags, TableSegmentPayload)> {
        let segment_file_lock = self
            .lock_segment_file(table_name, &position.segment_id)
            .await;
        let read_lock = segment_file_lock.read().await;

        let mut file = self
            .get_segment_file(table_name, &position.segment_id)
            .await?;

        file.seek(SeekFrom::Start(position.offset as u64))
            .await
            .map_err(|e| {
                errors::Errors::new(errors::ErrorCodes::FileSeekError)
                    .with_message(format!("Failed to seek file: {}", e))
            })?;

        let flag_byte = file.read_u8().await.map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::FileReadError)
                .with_message(format!("Failed to read flag byte: {}", e))
        })?;
        let flag = RecordStateFlags::from(flag_byte);

        let size_header = file.read_u32().await.map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::FileReadError)
                .with_message(format!("Failed to read size header: {}", e))
        })?;

        let mut buffer = vec![0; size_header as usize];
        file.read_exact(&mut buffer).await.map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::FileReadError)
                .with_message(format!("Failed to read data: {}", e))
        })?;

        drop(read_lock);

        let record = self.codec.decode(&buffer)?;

        Ok((flag, record))
    }

    /// Marks a record as deleted in the segment file. (not real delete)
    pub async fn mark_deleted_record(
        &self,
        table_name: &str,
        position: TableRecordPosition,
    ) -> errors::Result<()> {
        let segment_file_lock = self
            .lock_segment_file(table_name, &position.segment_id)
            .await;
        let _read_lock = segment_file_lock.read().await;

        let mut file = self
            .get_segment_file(table_name, &position.segment_id)
            .await?;

        file.seek(SeekFrom::Start(position.offset as u64))
            .await
            .map_err(|e| {
                errors::Errors::new(errors::ErrorCodes::FileSeekError)
                    .with_message(format!("Failed to seek file: {}", e))
            })?;

        let delete_flag = RecordStateFlags::Deleted as u8;

        file.write_u8(delete_flag).await.map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::FileWriteError)
                .with_message(format!("Failed to write delete flag: {}", e))
        })?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct ListSegmentFileResultItem {
    pub file_name: String,
    pub file_size: u32,
}

#[derive(Debug)]
pub struct ScanSegmentFileResult {
    pub state_flags: RecordStateFlags,
    pub position: TableRecordPosition,
    pub payload: TableSegmentPayload,
}
