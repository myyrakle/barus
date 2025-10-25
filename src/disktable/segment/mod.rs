use std::{collections::HashMap, path::PathBuf, sync::Arc};

use nix::NixPath;
use tokio::{
    fs::{File, OpenOptions},
    sync::Mutex,
};

use crate::{
    config::TABLES_DIRECTORY,
    disktable::segment::id::TableSegmentID,
    errors::{self, Errors},
    os::file_resize_and_set_zero,
};

pub mod id;
pub mod record;

#[derive(Debug, Clone)]
pub struct TableSegmentManager {
    base_path: PathBuf,
    tables_map: Arc<Mutex<HashMap<String, TableSegmentStatusPerTable>>>,
}

pub struct ListSegmentFileItem {
    pub file_name: String,
    pub file_size: u64,
}

impl TableSegmentManager {
    pub fn new(base_path: PathBuf) -> Self {
        Self {
            base_path,
            tables_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn list_segment_files(
        &self,
        table_name: &str,
    ) -> errors::Result<Vec<ListSegmentFileItem>> {
        let table_directory = self.base_path.join(TABLES_DIRECTORY).join(table_name);

        // 1. 모든 세그먼트 파일 읽기 (파일만 필터링해서 파일명 반환)
        let mut segment_files: Vec<_> = std::fs::read_dir(&table_directory)
            .map_err(|e| {
                errors::Errors::WALSegmentFileOpenError(format!(
                    "Failed to read Table Segment directory: {}",
                    e
                ))
            })?
            .filter_map(|entry| {
                entry.ok().and_then(|e| {
                    let path = e.path();
                    if path.is_file() {
                        let file_name = path
                            .file_name()
                            .and_then(|name| name.to_str().map(|s| s.to_string()))
                            .unwrap_or_default();

                        let file_size = path.len() as u64;

                        Some(ListSegmentFileItem {
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

    pub async fn set_table_names(&self, table_names: Vec<String>) -> errors::Result<()> {
        for table_name in table_names {
            let segment_files = self.list_segment_files(&table_name).await?;

            let last_segment_id = match segment_files.last() {
                Some(file) => TableSegmentID::try_from(file.file_name.as_str())
                    .unwrap_or(TableSegmentID::new(0)),
                None => TableSegmentID::new(0),
            };

            let file_size = match segment_files.last() {
                Some(file) => file.file_size,
                None => 0,
            };

            let mut table_map = self.tables_map.lock().await;
            table_map.insert(
                table_name,
                TableSegmentStatusPerTable {
                    last_segment_id,
                    file_size,
                },
            );
        }

        Ok(())
    }

    // new segment file (DISKTABLE_PAGE_SIZE start)
    pub async fn create_segment(&self, table_name: &str, size: u64) -> errors::Result<File> {
        // 1. Check if table exists
        let mut table_map = self.tables_map.lock().await;

        let table_status = table_map
            .get_mut(table_name)
            .ok_or(Errors::TableNotFound(format!(
                "Table '{}' not found",
                table_name
            )))?;

        // 2. Create new segment file
        table_status.last_segment_id.increment();
        let segment_filename: String = (&table_status.last_segment_id).into();

        let new_segment_file_path = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(table_name)
            .join(segment_filename);

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(new_segment_file_path)
            .await
            .map_err(|err| Errors::TableSegmentFileCreateError(err.to_string()))?;

        file_resize_and_set_zero(&mut file, size).await?;

        Ok(file)
    }

    // increase size of segment file
    pub async fn increase_segment(&self, table_name: &str, size: u64) -> errors::Result<File> {
        // 1. Check if table exists
        let mut table_map = self.tables_map.lock().await;

        let table_status = table_map
            .get_mut(table_name)
            .ok_or(Errors::TableNotFound(format!(
                "Table '{}' not found",
                table_name
            )))?;

        // 2. get segment file
        let segment_filename: String = (&table_status.last_segment_id).into();

        let new_segment_file_path = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(table_name)
            .join(segment_filename);

        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .open(new_segment_file_path)
            .await
            .map_err(|err| Errors::TableSegmentFileCreateError(err.to_string()))?;

        // 3. Expand segment file
        file_resize_and_set_zero(&mut file, size).await?;

        Ok(file)
    }

    pub async fn write_records(
        &self,
        _table_name: &str,
        _records: Vec<Vec<u8>>,
    ) -> Result<(), Errors> {
        // Implementation goes here
        unimplemented!()
    }

    pub async fn mark_deleted(&self, _table_name: &str, _offset: u64) -> Result<(), Errors> {
        // Implementation goes here
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct TableSegmentStatusPerTable {
    last_segment_id: TableSegmentID,
    file_size: u64,
}
