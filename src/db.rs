use std::path::PathBuf;

use tokio::sync::Mutex;

use crate::{
    errors,
    wal::{self, WALManager, WalPayload, WalRecord, WalRecordBincodeCodec},
};

#[derive(Debug)]
pub struct DBEngine {
    base_path: PathBuf,
    wal_manager: Mutex<WALManager>,
}

pub struct GetResponse {
    pub value: Vec<u8>,
}

impl DBEngine {
    pub fn new(base_path: PathBuf) -> Self {
        Self {
            base_path: base_path.clone(),
            wal_manager: Mutex::new(WALManager::new(
                Box::new(WalRecordBincodeCodec {}),
                base_path,
            )),
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
        self.wal_manager.lock().await.initialize().await?;
        self.wal_manager.lock().await.load().await?;
        self.wal_manager.lock().await.start_background()?;

        // 4. TODO: Basic Table Setting Init

        Ok(())
    }

    pub async fn get(&self, _table: &str, _key: &str) -> errors::Result<GetResponse> {
        unimplemented!()
    }

    pub async fn put(&self, table: String, key: String, value: String) -> errors::Result<()> {
        let wal_record = WalRecord {
            record_id: 0,
            record_type: wal::RecordType::Put,
            data: WalPayload {
                table: table,
                key: key,
                value: Some(value),
            },
        };

        {
            self.wal_manager.lock().await.append(wal_record).await?;
        }

        // unimplemented!()

        // TODO: 메인 테이블 데이터 및 Tree 인덱스 업데이트

        Ok(())
    }

    pub async fn delete(&self, table: String, key: String) -> errors::Result<()> {
        let wal_record = WalRecord {
            record_id: 0,
            record_type: wal::RecordType::Delete,
            data: WalPayload {
                table: table.to_string(),
                key: key.to_string(),
                value: None,
            },
        };

        {
            self.wal_manager.lock().await.append(wal_record).await?;
        }

        // TODO: 메인 테이블 데이터 및 Tree 인덱스 업데이트

        Ok(())
    }
}
