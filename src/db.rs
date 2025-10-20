use std::{path::PathBuf, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    disktable::{DiskTableManager, DisktableGetResult},
    errors,
    memtable::{MemtableGetResult, MemtableManager},
    system::{SystemInfo, get_system_info},
    wal::{
        self, WALManager,
        encode::WalRecordBincodeCodec,
        record::{WalPayload, WalRecord},
    },
};

#[derive(Debug, Clone)] // Clone 추가
pub struct DBEngine {
    #[allow(dead_code)]
    system_info: SystemInfo,
    #[allow(dead_code)]
    base_path: PathBuf,
    wal_manager: Arc<Mutex<WALManager>>,
    memtable_manager: Arc<MemtableManager>,
    disktable_manager: Arc<DiskTableManager>,
}

pub struct GetResponse {
    pub value: String,
}

impl DBEngine {
    pub async fn initialize(base_path: PathBuf) -> errors::Result<Self> {
        // 1. Load System Info
        let system_info = get_system_info();

        // 2. Initialize the database directory
        // Create DB directory if not exists
        std::fs::create_dir_all(&base_path).or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(errors::Errors::WalInitializationError(format!(
                    "Failed to create database directory: {}",
                    e
                )))
            }
        })?;

        // 3. TODO: Global Setting Init

        // 4. Initialize and load the WAL manager
        let wal_manager = {
            let mut wal_manager =
                WALManager::new(Box::new(WalRecordBincodeCodec {}), base_path.clone());

            wal_manager.initialize().await?;
            wal_manager.load().await?;
            wal_manager.start_background()?;

            Arc::new(Mutex::new(wal_manager))
        };

        // 5. Memtable Load
        let memtable_manager = {
            let memtable_manager = Arc::new(MemtableManager::new(&system_info));

            memtable_manager
        };

        // TODO: Basic Table Setting Init

        // 6. Disktable Load
        let disktable_manager = {
            let disktable_manager = Arc::new(DiskTableManager::new(base_path.clone()));

            disktable_manager.initialize().await?;

            disktable_manager
        };

        let manager = Self {
            system_info,
            base_path: base_path.clone(),
            wal_manager,
            memtable_manager,
            disktable_manager,
        };

        Ok(manager)
    }

    pub async fn get(&self, table: &str, key: &str) -> errors::Result<GetResponse> {
        // 1. Try to get from Memtable
        let memtable_result = self.memtable_manager.get(table, key).await?;

        match memtable_result {
            MemtableGetResult::Deleted => {
                return Err(errors::Errors::ValueNotFound(format!(
                    "Key not found (deleted): {}",
                    key
                )));
            }
            MemtableGetResult::Found(value) => {
                return Ok(GetResponse { value });
            }
            MemtableGetResult::NotFound => {}
        }

        // 2. Try to get from disk area (not implemented yet)
        {
            let disktable_result = self.disktable_manager.get(table, key).await?;

            match disktable_result {
                DisktableGetResult::Found(value) => {
                    return Ok(GetResponse { value });
                }
                _ => {
                    return Err(errors::Errors::ValueNotFound(format!(
                        "Key not found: {}",
                        key
                    )));
                }
            }
        }
    }

    pub async fn put(&self, table: String, key: String, value: String) -> errors::Result<()> {
        let wal_record = WalRecord {
            record_id: 0,
            record_type: wal::record::RecordType::Put,
            data: WalPayload {
                table: table.clone(),
                key: key.clone(),
                value: Some(value.clone()),
            },
        };

        // 1. WAL write
        {
            self.wal_manager.lock().await.append(wal_record).await?;
        }

        // 2. Memtable update
        {
            self.memtable_manager.put(table, key, value).await?;
        }

        Ok(())
    }

    pub async fn delete(&self, table: String, key: String) -> errors::Result<()> {
        let wal_record = WalRecord {
            record_id: 0,
            record_type: wal::record::RecordType::Delete,
            data: WalPayload {
                table: table.to_string(),
                key: key.to_string(),
                value: None,
            },
        };

        // 1. WAL write
        {
            self.wal_manager.lock().await.append(wal_record).await?;
        }

        // 2. Memtable update
        {
            self.memtable_manager.delete(table, key).await?;
        }

        Ok(())
    }
}
