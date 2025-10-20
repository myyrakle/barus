use crate::{config::TABLES_DIRECTORY, errors};

pub mod record;
pub mod table;

#[derive(Debug, Clone)]
pub struct DiskTableManager {
    #[allow(dead_code)]
    base_path: std::path::PathBuf,
}

impl DiskTableManager {
    pub fn new(base_path: std::path::PathBuf) -> Self {
        Self { base_path }
    }

    pub async fn initialize(&self) -> errors::Result<()> {
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

        Ok(())
    }

    pub async fn create_table(&self, table: &str) -> errors::Result<()> {
        // 1. Create table info file
        let table_info_path = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(format!("{}.json", table));

        if table_info_path.exists() {
            return Ok(());
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

    pub async fn drop_table(&self, table: &str) -> errors::Result<()> {
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

    pub async fn get(&self, _table: &str, _key: &str) -> errors::Result<DisktableGetResult> {
        Ok(DisktableGetResult::Found("disk value".to_string()))
    }

    pub async fn put(&self, _table: String, _key: String, _value: String) -> errors::Result<()> {
        Ok(())
    }

    pub async fn delete(&self, _table: String, _key: String) -> errors::Result<()> {
        Ok(())
    }
}

pub enum DisktableGetResult {
    Found(String),
    NotFound,
    Deleted,
}
