use std::{collections::HashMap, path::PathBuf, sync::Arc};

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

impl TableSegmentManager {
    pub fn new(base_path: PathBuf) -> Self {
        Self {
            base_path,
            tables_map: Arc::new(Mutex::new(HashMap::new())),
        }
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
        table_status.last_segment_id += 1;
        let segment_filename: String = (&TableSegmentID::new(table_status.last_segment_id)).into();

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
        let segment_filename: String = (&TableSegmentID::new(table_status.last_segment_id)).into();

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
    last_segment_id: u64,
}
