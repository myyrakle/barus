use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    config::TABLES_DIRECTORY,
    disktable::{segment::TableRecordPayload, table::TableInfo},
    errors::{self, Errors},
    memtable::HashMemtable,
    wal::state::{WALGlobalState, WALStateWriteHandles},
};

pub mod index;
pub mod segment;
pub mod table;

#[derive(Debug, Clone)]
pub struct DiskTableManager {
    #[allow(dead_code)]
    base_path: std::path::PathBuf,
    #[allow(dead_code)]
    index_manager: index::IndexManager,
    #[allow(dead_code)]
    segment_manager: segment::TableSegmentManager,
}

impl DiskTableManager {
    pub fn new(base_path: std::path::PathBuf) -> Self {
        Self {
            base_path: base_path.clone(),
            index_manager: index::IndexManager::new(),
            segment_manager: segment::TableSegmentManager::new(base_path),
        }
    }

    pub async fn initialize(&self) -> errors::Result<()> {
        // 1. Initialize Table Directory
        let tables_path = self.base_path.join(TABLES_DIRECTORY);

        if !tables_path.exists() {
            tokio::fs::create_dir_all(self.base_path.join(TABLES_DIRECTORY))
                .await
                .map_err(|e| {
                    errors::Errors::TableCreationError(format!(
                        "Failed to create tables directory: {}",
                        e
                    ))
                })?;
        }

        // 2. Set Table Names
        let table_names = self.list_tables().await?;
        self.segment_manager.set_table_names(table_names).await?;

        Ok(())
    }

    pub async fn list_tables(&self) -> errors::Result<Vec<String>> {
        let mut table_names = Vec::new();

        let tables_path = self.base_path.join(TABLES_DIRECTORY);
        let mut dir_entries = tokio::fs::read_dir(tables_path).await.map_err(|e| {
            errors::Errors::TableListFailed(format!("Failed to read tables directory: {}", e))
        })?;

        while let Some(entry) = dir_entries.next_entry().await.map_err(|e| {
            errors::Errors::TableListFailed(format!("Failed to read table entry: {}", e))
        })? {
            let file_name = entry.file_name();
            if let Some(name_str) = file_name.to_str()
                && name_str.ends_with(".json")
            {
                table_names.push(name_str.trim_end_matches(".json").to_string());
            }
        }

        Ok(table_names)
    }

    pub async fn get_table(&self, table: &str) -> errors::Result<TableInfo> {
        let table_path = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(format!("{}.json", table));

        let table_info_bytes = tokio::fs::read(table_path).await.map_err(|e| {
            errors::Errors::TableNotFound(format!("Failed to read table file: {}", e))
        })?;

        let table_info = serde_json::from_slice(&table_info_bytes).map_err(|e| {
            errors::Errors::TableGetFailed(format!("Failed to deserialize table info: {}", e))
        })?;

        Ok(table_info)
    }

    pub async fn create_table(&self, table: &str) -> errors::Result<()> {
        // 1. Create table info file
        let table_info_path = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(format!("{}.json", table));

        if table_info_path.exists() {
            return Err(errors::Errors::TableAlreadyExists(format!(
                "Table '{}' already exists",
                table
            )));
        }

        let table_info = table::TableInfo {
            name: table.to_string(),
        };

        let table_info_json = serde_json::to_string_pretty(&table_info).map_err(|e| {
            errors::Errors::TableCreationError(format!(
                "Failed to serialize table info to JSON: {}",
                e
            ))
        })?;

        tokio::fs::write(&table_info_path, table_info_json)
            .await
            .map_err(|e| {
                errors::Errors::TableCreationError(format!(
                    "Failed to write table info to file: {}",
                    e
                ))
            })?;

        // 2. Create table segment directory
        let table_segment_directory = self.base_path.join(TABLES_DIRECTORY).join(table);
        if !table_segment_directory.exists() {
            tokio::fs::create_dir_all(&table_segment_directory)
                .await
                .map_err(|e| {
                    errors::Errors::TableCreationError(format!(
                        "Failed to create table segment directory: {}",
                        e
                    ))
                })?;
        }

        Ok(())
    }

    // delete all table datas.
    // No error occurs if db file not exists
    pub async fn delete_table(&self, table: &str) -> errors::Result<()> {
        // 1. Table info file 삭제
        let table_info_path = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(format!("{}.json", table));

        if table_info_path.exists() {
            tokio::fs::remove_file(&table_info_path)
                .await
                .map_err(|e| {
                    errors::Errors::TableCreationError(format!(
                        "Failed to delete table info file: {}",
                        e
                    ))
                })?;
        }

        // 2. Disktable 세그먼트 파일 전체 삭제
        let table_segment_directory = self.base_path.join(TABLES_DIRECTORY).join(table);
        if table_segment_directory.exists() {
            tokio::fs::remove_dir_all(&table_segment_directory)
                .await
                .map_err(|e| {
                    errors::Errors::TableCreationError(format!(
                        "Failed to delete table segment directory: {}",
                        e
                    ))
                })?;
        }

        Ok(())
    }

    pub async fn get_value(&self, _table: &str, _key: &str) -> errors::Result<DisktableGetResult> {
        Ok(DisktableGetResult::Found("disk value".to_string()))
    }

    pub async fn put_value(
        &self,
        _table: String,
        _key: String,
        _value: String,
    ) -> errors::Result<()> {
        Ok(())
    }

    pub async fn delete_value(&self, _table: String, _key: String) -> errors::Result<()> {
        Ok(())
    }

    pub async fn write_memtable(
        &self,
        memtable: HashMap<String, Arc<Mutex<HashMemtable>>>,
        wal_state: Arc<Mutex<WALGlobalState>>,
        wal_state_write_handles: Arc<Mutex<WALStateWriteHandles>>,
    ) -> errors::Result<()> {
        // 1. write memtable to disk
        for (table_name, memtable) in memtable {
            let mut memtable = memtable.lock().await;

            for (key, memtable_entry) in memtable.table.iter() {
                match &memtable_entry.value {
                    // Insert/Update Process
                    Some(value) => {
                        // delete old data if exists
                        let old_position = self
                            .index_manager
                            .find_record(table_name.as_str(), key.as_str())
                            .await?;

                        if let Some(old_position) = old_position {
                            self.segment_manager
                                .mark_deleted_record(table_name.as_str(), old_position)
                                .await?;
                        }

                        // insert new data
                        let position = self
                            .segment_manager
                            .append_record(
                                table_name.as_str(),
                                TableRecordPayload {
                                    key: key.clone(),
                                    value: value.clone(),
                                },
                            )
                            .await?;

                        self.index_manager
                            .add_record(table_name.as_str(), key.as_str(), &position)
                            .await?;
                    }
                    // Delete Process
                    None => {
                        let old_position = self
                            .index_manager
                            .find_record(table_name.as_str(), key.as_str())
                            .await?;

                        if let Some(old_position) = old_position {
                            self.segment_manager
                                .mark_deleted_record(table_name.as_str(), old_position)
                                .await?;
                        }
                    }
                };
            }

            // 1.3. destroy memtable. now, we can find data in disk
            memtable.table.clear();
        }

        // 2. move WAL checkpoint
        {
            let mut wal_state = wal_state.lock().await;

            wal_state.last_checkpoint_record_id = wal_state.last_record_id;
            wal_state.last_checkpoint_segment_id = wal_state.last_segment_id.clone();
            let mut write_handle = wal_state_write_handles.lock().await;

            if let Some(ref mut file) = write_handle.state_file {
                wal_state.save(file).await?;
            } else {
                return Err(Errors::WALStateFileHandleNotFound);
            }
        }
        Ok(())
    }
}

pub enum DisktableGetResult {
    Found(String),
    NotFound,
    Deleted,
}
