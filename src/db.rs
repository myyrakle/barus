use std::{path::PathBuf, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    errors,
    memtable::{MemtableGetResult, MemtableManager},
    wal::{
        self, WALManager,
        encode::WalRecordBincodeCodec,
        record::{WalPayload, WalRecord},
    },
};

#[derive(Debug, Clone)] // Clone 추가
pub struct DBEngine {
    base_path: PathBuf,
    wal_manager: Arc<Mutex<WALManager>>,
    memtable_manager: Arc<MemtableManager>,
}

pub struct GetResponse {
    pub value: Vec<u8>,
}

impl DBEngine {
    pub fn new(base_path: PathBuf) -> Self {
        Self {
            base_path: base_path.clone(),
            wal_manager: Arc::new(Mutex::new(WALManager::new(
                Box::new(WalRecordBincodeCodec {}),
                base_path,
            ))),
            memtable_manager: Arc::new(MemtableManager::new()),
        }
    }

    pub async fn initialize(&mut self) -> errors::Result<()> {
        // 1. Initialize the database directory
        // Create DB directory if not exists
        std::fs::create_dir_all(&self.base_path).or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(errors::Errors::WalInitializationError(format!(
                    "Failed to create database directory: {}",
                    e
                )))
            }
        })?;

        // 2. TODO: Global Setting Init

        // 3. Initialize and load the WAL manager
        let mut wal = self.wal_manager.lock().await;
        wal.initialize().await?;
        wal.load().await?;
        wal.start_background()?;
        drop(wal); // 명시적으로 Lock 해제

        // 4. TODO: Basic Table Setting Init

        Ok(())
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
                return Ok(GetResponse {
                    value: value.into_bytes(),
                });
            }
            MemtableGetResult::NotFound => {}
        }

        // 2. Try to get from disk area (not implemented yet)
        let response = GetResponse { value: vec![] };

        Ok(response)
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

        // unimplemented!()

        // TODO: 메인 테이블 데이터 및 Tree 인덱스 업데이트

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

        // TODO: 메인 테이블 데이터 및 Tree 인덱스 업데이트

        Ok(())
    }
}
