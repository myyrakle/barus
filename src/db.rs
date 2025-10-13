use std::path::PathBuf;

use crate::{
    errors,
    wal::{WALManager, WalRecordJsonCodec},
};

#[derive(Debug)]
pub struct DBEngine {
    base_path: PathBuf,
    wal_manager: WALManager,
}

pub struct GetResponse {
    pub value: Vec<u8>,
}

impl DBEngine {
    pub fn new(base_path: PathBuf) -> Self {
        Self {
            base_path: base_path.clone(),
            wal_manager: WALManager::new(Box::new(WalRecordJsonCodec {}), base_path),
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
        self.wal_manager.initialize().await?;
        self.wal_manager.load().await?;
        self.wal_manager.start_background()?;

        // 4. TODO: Basic Table Setting Init

        Ok(())
    }

    pub async fn get(&self, _key: &str) -> errors::Result<GetResponse> {
        unimplemented!()
    }

    pub async fn put(&self, _key: &str, _value: &[u8]) -> errors::Result<()> {
        unimplemented!()
    }

    pub async fn delete(&self, _key: &str) -> errors::Result<()> {
        unimplemented!()
    }
}
