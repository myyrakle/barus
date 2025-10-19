use crate::errors;

pub mod record;

#[derive(Debug, Clone)]
pub struct DiskTableManager {
    #[allow(dead_code)]
    base_path: std::path::PathBuf,
}

impl DiskTableManager {
    pub fn new(base_path: std::path::PathBuf) -> Self {
        Self { base_path }
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
