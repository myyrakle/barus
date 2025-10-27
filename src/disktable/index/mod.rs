use std::{collections::HashMap, path::PathBuf, sync::Arc};

use tokio::sync::Mutex;

use crate::{config::TABLES_DIRECTORY, disktable::segment::TableRecordPosition, errors::Errors};

pub mod btree;

#[derive(Debug, Clone)]
pub struct IndexManager {
    base_path: PathBuf,
    indices: Arc<Mutex<HashMap<String, Arc<btree::BTreeIndex>>>>,
}

impl IndexManager {
    pub fn new(base_path: PathBuf) -> Self {
        Self {
            base_path,
            indices: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // delete index file and remove from in-memory map
    async fn delete_index(&self, table_name: &str) -> Result<(), Errors> {
        // 1. remove all file
        let index_path = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(table_name)
            .join("indices");

        tokio::fs::remove_dir_all(index_path).await.or_else(|e| {
            if e.kind() != std::io::ErrorKind::NotFound {
                Err(Errors::FileDeleteError(format!(
                    "Failed to delete index files: {}",
                    e
                )))
            } else {
                Ok(())
            }
        })?;

        // 2. remove from in-memory map
        let mut indices = self.indices.lock().await;
        indices.remove(table_name);

        Ok(())
    }

    /// 테이블의 인덱스 가져오기 또는 생성
    async fn get_or_create_index(
        &self,
        table_name: &str,
    ) -> Result<Arc<btree::BTreeIndex>, Errors> {
        let mut indices = self.indices.lock().await;

        if let Some(index) = indices.get(table_name) {
            return Ok(index.clone());
        }

        // 새 인덱스 생성 및 초기화
        let index = Arc::new(btree::BTreeIndex::new(
            self.base_path.clone(),
            table_name.to_string(),
        ));
        index.initialize().await?;

        indices.insert(table_name.to_string(), index.clone());

        Ok(index)
    }

    pub async fn add_record(
        &self,
        table_name: &str,
        key: &str,
        position: &TableRecordPosition,
    ) -> Result<(), Errors> {
        let index = self.get_or_create_index(table_name).await?;
        index.insert(key.to_string(), position.clone()).await
    }

    pub async fn delete_record(&self, table_name: &str, key: &str) -> Result<(), Errors> {
        let index = self.get_or_create_index(table_name).await?;
        index.delete(key).await
    }

    pub async fn update_record(
        &self,
        table_name: &str,
        key: &str,
        position: &TableRecordPosition,
    ) -> Result<(), Errors> {
        let index = self.get_or_create_index(table_name).await?;
        index.update(key.to_string(), position.clone()).await
    }

    pub async fn find_record(
        &self,
        table_name: &str,
        key: &str,
    ) -> Result<Option<TableRecordPosition>, Errors> {
        let index = self.get_or_create_index(table_name).await?;
        index.find(key).await
    }
}
