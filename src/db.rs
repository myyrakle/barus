use std::{path::PathBuf, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    compaction::CompactionManager,
    disktable::{DiskTableManager, DisktableGetResult, table::TableInfo},
    errors,
    memtable::{MemtableGetResult, MemtableManager},
    os::handle_shutdown,
    system::{SystemInfo, get_system_info},
    validate::{validate_key, validate_table_name, validate_value},
    wal::{
        self, WALManager,
        encode::WALRecordBincodeCodec,
        record::{WALPayload, WALRecord},
        segment::WALSegmentID,
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
    compaction_manager: Arc<Mutex<CompactionManager>>,
}

pub struct GetResponse {
    pub value: String,
}

pub struct ListTablesResponse {
    pub tables: Vec<ListTablesResponseItem>,
}

pub struct ListTablesResponseItem {
    pub table_name: String,
}

pub struct DBStatusResponse {
    pub memtable_size: u64,
    pub table_count: usize,
    pub wal_total_size: u64,
}

impl DBEngine {
    /// Initializes the DBEngine with the given base path.
    pub async fn initialize(base_path: PathBuf) -> errors::Result<Self> {
        // 1. Load System Info
        let system_info = get_system_info();

        log::info!("System Info Loaded: {:?}", system_info);

        // 2. Initialize the database directory
        // Create DB directory if not exists
        log::info!("Initializing database directory...");
        std::fs::create_dir_all(&base_path).or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(errors::Errors::WALInitializationError(format!(
                    "Failed to create database directory: {}",
                    e
                )))
            }
        })?;

        // 3. Initialize and load the WAL manager
        log::info!("Initializing WAL manager...");
        let wal_manager = {
            let mut wal_manager =
                WALManager::new(Box::new(WALRecordBincodeCodec {}), base_path.clone());

            wal_manager.initialize().await?;
            wal_manager.load().await?;

            wal_manager
        };

        // 4. Memtable Load
        log::info!("Initializing memtable manager...");
        let mut memtable_manager = MemtableManager::new(&system_info, &wal_manager);

        // 5. Disktable Load
        log::info!("Initializing disktable manager...");
        let disktable_manager = {
            let disktable_manager = Arc::new(DiskTableManager::new(base_path.clone()));

            disktable_manager.initialize().await?;

            disktable_manager
        };

        // 6. compaction manager load
        log::info!("Initializing compaction manager...");
        let compaction_manager = CompactionManager::new(
            &wal_manager,
            &mut memtable_manager,
            disktable_manager.clone(),
        );

        // 7. Load table list
        log::info!("Loading table list...");
        {
            let table_list = disktable_manager.list_tables().await?;
            memtable_manager.load_table_list(table_list).await?;
        }

        // 8. Load WAL Records
        log::info!("WAL Records Loading...");
        {
            let segment_files = wal_manager.list_segment_files().await?;

            let state = { wal_manager.state.lock().await.clone() };

            let last_checkpoint_segment = state.last_checkpoint_segment_id.clone();
            let last_checkpoint_record_id = state.last_checkpoint_record_id;

            for segment_file in segment_files {
                let current_segment_id = WALSegmentID::try_from(segment_file.as_str())
                    .expect("Failed to parse WAL segment ID");

                if current_segment_id < last_checkpoint_segment {
                    continue;
                }

                let (records, _) = wal_manager.scan_records(segment_file.as_str()).await?;

                let filtered_records = records
                    .into_iter()
                    .filter(|record| record.record_id > last_checkpoint_record_id)
                    .collect();

                memtable_manager.load_wal_records(filtered_records).await?;
            }
        }

        let mut manager = Self {
            system_info,
            base_path: base_path.clone(),
            wal_manager: Arc::new(Mutex::new(wal_manager)),
            memtable_manager: Arc::new(memtable_manager),
            disktable_manager,
            compaction_manager: Arc::new(Mutex::new(compaction_manager)),
        };

        log::info!("Starting Background Workers...");
        manager.start_background().await?;

        log::info!("DB Engine Initialized");

        Ok(manager)
    }

    async fn start_background(&mut self) -> errors::Result<()> {
        {
            self.wal_manager.lock().await.start_background()?;
        }

        {
            self.compaction_manager.lock().await.start_background()?;
        }

        {
            let wal_manager = self.wal_manager.clone();

            tokio::spawn(async move {
                handle_shutdown().await;
                log::info!("Graceful shutdown started");

                if let Err(error) = wal_manager.lock().await.flush_wal().await {
                    log::error!("Failed to flush WAL: {}", error);
                }
                log::info!("WAL flushed");

                log::info!("Graceful shutdown completed");
                std::process::exit(0);
            });
        }

        Ok(())
    }

    pub async fn get_db_status(&self) -> errors::Result<DBStatusResponse> {
        let table_count = self.disktable_manager.list_tables().await?.len();
        let memtable_size = self.memtable_manager.get_memtable_current_size()?;
        let wal_total_size = self.wal_manager.lock().await.total_file_size().await?;

        let status = DBStatusResponse {
            table_count,
            memtable_size,
            wal_total_size,
        };

        Ok(status)
    }

    /// List all table names
    pub async fn list_tables(&self) -> errors::Result<ListTablesResponse> {
        let table_names = self.memtable_manager.list_tables().await?;

        let tables = table_names
            .into_iter()
            .map(|name| ListTablesResponseItem { table_name: name })
            .collect();

        Ok(ListTablesResponse { tables })
    }

    /// get table information
    pub async fn get_table(&self, table: &str) -> errors::Result<TableInfo> {
        // 1. Validation
        validate_table_name(table)?;

        // 2. Get table information from Disktable Manager
        let table_info = self.disktable_manager.get_table(table).await?;

        Ok(table_info)
    }

    /// Create Table
    /// Error occurs if table already exists
    pub async fn create_table(&self, table: &str) -> errors::Result<()> {
        // 1. Validation
        validate_table_name(table)?;

        // 2. Create table in Disktable Manager
        self.disktable_manager.create_table(table).await?;

        // 3. Create table in Memtable Manager
        self.memtable_manager.create_table(table).await?;

        Ok(())
    }

    /// Delete Table
    /// No error occurs if table does not exist
    pub async fn delete_table(&self, table: &str) -> errors::Result<()> {
        // 1. Validation
        validate_table_name(table)?;

        // 2. Delete table in Disktable Manager
        self.disktable_manager.delete_table(table).await?;

        // 3. Delete table in Memtable Manager
        self.memtable_manager.delete_table(table).await?;

        Ok(())
    }

    /// Gets the value for the given table and key.
    pub async fn get_value(&self, table: &str, key: &str) -> errors::Result<GetResponse> {
        // 1. Validation
        validate_table_name(table)?;
        validate_key(key)?;

        // 2. Try to get from Memtable
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

        let memtable_result = self.memtable_manager.get_from_flushing(table, key).await?;

        // 2.
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

        // 4. Try to get from disk area (not implemented yet)
        {
            let disktable_result = self.disktable_manager.get(table, key).await?;

            match disktable_result {
                DisktableGetResult::Found(value) => Ok(GetResponse { value }),
                _ => Err(errors::Errors::ValueNotFound(format!(
                    "Key not found: {}",
                    key
                ))),
            }
        }
    }

    /// Puts the given key-value pair into the specified table.
    pub async fn put_value(&self, table: String, key: String, value: String) -> errors::Result<()> {
        // 1. Validation
        validate_table_name(&table)?;
        validate_key(&key)?;
        validate_value(&value)?;

        let wal_record = WALRecord {
            record_id: 0,
            record_type: wal::record::RecordType::Put,
            data: WALPayload {
                table: table.clone(),
                key: key.clone(),
                value: Some(value.clone()),
            },
        };

        // 2. WAL write
        {
            self.wal_manager.lock().await.append(wal_record).await?;
        }

        // 3. Memtable update
        {
            self.memtable_manager.put(table, key, value).await?;
        }

        Ok(())
    }

    /// Deletes the given key from the specified table.
    pub async fn delete_value(&self, table: String, key: String) -> errors::Result<()> {
        // 1 Validation
        validate_table_name(&table)?;
        validate_key(&key)?;

        let wal_record = WALRecord {
            record_id: 0,
            record_type: wal::record::RecordType::Delete,
            data: WALPayload {
                table: table.to_string(),
                key: key.to_string(),
                value: None,
            },
        };

        // 2. WAL write
        {
            self.wal_manager.lock().await.append(wal_record).await?;
        }

        // 3. Memtable update
        {
            self.memtable_manager.delete(table, key).await?;
        }

        Ok(())
    }

    /// Flushes the WAL to disk.
    pub async fn flush_wal(&self) -> errors::Result<()> {
        self.wal_manager.lock().await.flush_wal().await?;

        Ok(())
    }

    /// Trigger memtable flush
    pub async fn trigger_memtable_flush(&self) -> errors::Result<()> {
        self.memtable_manager.trigger_flush().await?;

        Ok(())
    }
}
