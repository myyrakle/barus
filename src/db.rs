use std::path::PathBuf;

use tokio::sync::Mutex;

use crate::{
    errors,
    wal::{self, WALManager, WalRecord, WalRecordJsonCodec},
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
            wal_manager: Mutex::new(WALManager::new(Box::new(WalRecordJsonCodec {}), base_path)),
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

    pub async fn get(&self, _key: &str) -> errors::Result<GetResponse> {
        unimplemented!()
    }

    pub async fn put(&self, key: &str, value: &str) -> errors::Result<()> {
        let payload = format!(r#"{{"key":"{key}","value":"{value}"}}"#);

        let wal_record = WalRecord {
            record_id: 0,
            record_type: wal::RecordType::Put,
            data: payload,
        };

        {
            self.wal_manager.lock().await.append(wal_record).await?;
        }

        // unimplemented!()

        Ok(())
    }

    pub async fn delete(&self, _key: &str) -> errors::Result<()> {
        unimplemented!()
    }
}
