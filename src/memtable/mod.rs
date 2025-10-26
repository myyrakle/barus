use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use tokio::sync::{Mutex, RwLock};

use crate::{
    compaction::MemtableFlushEvent,
    errors::{self, Errors},
    system::SystemInfo,
    wal::{
        WALManager,
        record::{RecordType, WALRecord},
        state::WALGlobalState,
    },
};

#[derive(Debug)]
pub struct MemtableManager {
    pub(crate) memtable_map: Arc<RwLock<HashMap<String, Arc<Mutex<HashMemtable>>>>>,
    pub(crate) memtable_current_size: Arc<AtomicU64>,
    pub(crate) flushing_memtable_map: Arc<RwLock<HashMap<String, Arc<Mutex<HashMemtable>>>>>,
    pub(crate) block_write: Arc<AtomicBool>,
    #[allow(dead_code)]
    memtable_size_soft_limit: usize,
    memtable_size_hard_limit: usize,
    pub(crate) memtable_flush_sender: tokio::sync::mpsc::Sender<MemtableFlushEvent>,

    // borrowed from WALManager
    pub(crate) wal_state: Arc<Mutex<WALGlobalState>>,
}

impl MemtableManager {
    pub fn new(system_info: &SystemInfo, wal_manager: &WALManager) -> Self {
        let total_memory = system_info.total_memory;

        let memtable_size_soft_limit =
            (total_memory as f64 * crate::config::MEMTABLE_SIZE_SOFT_LIMIT_RATE) as usize;

        let memtable_size_hard_limit =
            (total_memory as f64 * crate::config::MEMTABLE_SIZE_HARD_LIMIT_RATE) as usize;

        let (fake_sender, _) = tokio::sync::mpsc::channel(1);

        Self {
            memtable_map: Arc::new(RwLock::new(HashMap::new())),
            flushing_memtable_map: Arc::new(RwLock::new(HashMap::new())),
            memtable_current_size: Arc::new(AtomicU64::new(0)),
            block_write: Arc::new(AtomicBool::new(false)),
            memtable_size_soft_limit,
            memtable_size_hard_limit,
            memtable_flush_sender: fake_sender,
            wal_state: wal_manager.wal_state.clone(),
        }
    }

    pub fn get_memtable_current_size(&self) -> errors::Result<u64> {
        let memtable_current_size = self.memtable_current_size.load(Ordering::Relaxed);

        Ok(memtable_current_size)
    }

    pub async fn load_table_list(&self, table_list: Vec<String>) -> errors::Result<()> {
        for table in table_list {
            self.create_table(&table).await?;
        }

        Ok(())
    }

    pub async fn load_wal_records(&self, records: Vec<WALRecord>) -> errors::Result<()> {
        for record in records {
            match record.record_type {
                RecordType::Put => {
                    let payload = record.data;

                    self.put(
                        payload.table,
                        payload.key,
                        payload.value.unwrap_or_default(),
                    )
                    .await?;
                }
                RecordType::Delete => {
                    let payload = record.data;

                    match self.delete(payload.table, payload.key).await {
                        Ok(_) => (),
                        Err(error) => {
                            match error {
                                Errors::TableNotFound(_) | Errors::ValueNotFound(_) => {
                                    // 로그로 남기고 무시
                                    log::debug!("WAL replay delete failed but ignored: {}", error);
                                }
                                _ => {
                                    return Err(error);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn list_tables(&self) -> errors::Result<Vec<String>> {
        let memtable_map = self.memtable_map.read().await;

        let table_names = memtable_map.keys().cloned().collect();

        Ok(table_names)
    }

    pub async fn create_table(&self, table: &str) -> errors::Result<()> {
        let mut memtable_map = self.memtable_map.write().await;

        if !memtable_map.contains_key(table) {
            let memtable = Arc::new(Mutex::new(HashMemtable::new()));
            memtable_map.insert(table.to_string(), memtable);
        }

        Ok(())
    }

    pub async fn delete_table(&self, table: &str) -> errors::Result<()> {
        // 1. Delete the table from the map
        let delete_result = {
            let mut memtable_map = self.memtable_map.write().await;

            memtable_map.remove(table)
        };

        // 2. Decrement the current size
        if let Some(deleted_table) = delete_result {
            let reclaimed: u64 = deleted_table
                .lock()
                .await
                .table
                .values()
                .filter_map(|e| e.value.as_ref().map(|v| v.len() as u64))
                .sum();

            if reclaimed > 0 {
                self.memtable_current_size
                    .fetch_sub(reclaimed, Ordering::SeqCst);
            }
        }

        Ok(())
    }

    pub async fn trigger_flush(&self) -> errors::Result<()> {
        if self
            .block_write
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.memtable_current_size.store(0, Ordering::SeqCst);

            {
                let mut memtable_map = self.memtable_map.write().await;

                let mut flushing_memtable = self.flushing_memtable_map.write().await;
                for table in memtable_map.keys() {
                    flushing_memtable
                        .insert(table.clone(), Arc::new(Mutex::new(HashMemtable::new())));
                }

                std::mem::swap(&mut *memtable_map, &mut *flushing_memtable);
            }

            let mut flushing_memtable = self.flushing_memtable_map.write().await;
            let flushing_memtable = std::mem::take(&mut *flushing_memtable);

            let _ = self
                .memtable_flush_sender
                .send(MemtableFlushEvent {
                    memtable: flushing_memtable,
                    wal_state: self.wal_state.clone(),
                })
                .await;

            self.block_write.store(false, Ordering::SeqCst);
        } else {
            return Err(errors::Errors::MemtableFlushAlreadyInProgress);
        }

        Ok(())
    }

    pub async fn put(&self, table: String, key: String, value: String) -> errors::Result<()> {
        let bytes = key.len() + value.len();

        // 1. increment the current size, and check if it exceeds the hard limit
        // send a flush event if it exceeds the hard limit
        loop {
            let is_blocked = self.block_write.load(Ordering::Relaxed);
            if is_blocked {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }

            let current_memtable_size = self.memtable_current_size.load(Ordering::SeqCst);

            let new_size_value = current_memtable_size + (bytes as u64);

            if new_size_value > self.memtable_size_hard_limit as u64 {
                self.trigger_flush().await?;

                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }

            let cas_result = self.memtable_current_size.compare_exchange(
                current_memtable_size,
                new_size_value,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );

            match cas_result {
                Ok(_) => {
                    // CAS succeeded. Break the loop.
                    break;
                }
                Err(_) => {
                    // CAS failed. Retry the operation.)
                    continue;
                }
            }
        }

        // 2. get memtable for the table
        let memtable = {
            let memtable_map = self.memtable_map.read().await;

            match memtable_map.get(&table) {
                Some(memtable) => memtable.clone(),
                None => return Err(errors::Errors::TableNotFound(table.to_string())),
            }
        };

        // 3. put the key-value into the memtable
        let mut memtable_lock = memtable.lock().await;
        let old_value_size = memtable_lock.put(key, value);

        // 4. adjust current size if there was an old value
        if let Some(old_size) = old_value_size {
            self.memtable_current_size
                .fetch_sub(old_size as u64, Ordering::SeqCst);
        }

        Ok(())
    }

    pub async fn get(&self, table: &str, key: &str) -> errors::Result<MemtableGetResult> {
        let memtable_map = self.memtable_map.read().await;

        match memtable_map.get(table) {
            Some(memtable) => {
                let memtable_lock = memtable.lock().await;

                Ok(memtable_lock.get(key))
            }
            None => Ok(MemtableGetResult::NotFound),
        }
    }

    pub async fn get_from_flushing(
        &self,
        table: &str,
        key: &str,
    ) -> errors::Result<MemtableGetResult> {
        let memtable_map = self.flushing_memtable_map.read().await;

        match memtable_map.get(table) {
            Some(memtable) => {
                let memtable_lock = memtable.lock().await;

                Ok(memtable_lock.get(key))
            }
            None => Ok(MemtableGetResult::NotFound),
        }
    }

    pub async fn delete(&self, table: String, key: String) -> errors::Result<()> {
        // 1. check if the write is blocked
        loop {
            let is_blocked = self.block_write.load(Ordering::Relaxed);

            if is_blocked {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }

            break;
        }

        // 2. check if the memtable exists
        let memtable_map = self.memtable_map.read().await;

        match memtable_map.get(&table) {
            Some(memtable) => {
                let mut memtable_lock = memtable.lock().await;

                let _ = memtable_lock.delete(&key);

                Ok(())
            }
            None => Err(Errors::TableNotFound(format!("Table not found: {}", table))),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MemtableEntry {
    pub value: Option<String>,
}

#[derive(Debug)]
pub struct HashMemtable {
    pub(crate) table: HashMap<String, MemtableEntry>,
}

pub const MEMTABLE_CAPACITY: usize = 100000;

pub enum MemtableGetResult {
    Found(String),
    NotFound,
    Deleted,
}

impl Default for HashMemtable {
    fn default() -> Self {
        Self::new()
    }
}

impl HashMemtable {
    pub fn new() -> Self {
        Self {
            table: HashMap::with_capacity(MEMTABLE_CAPACITY),
        }
    }

    // Returns previous value size if key existed
    pub fn put(&mut self, key: String, value: String) -> Option<usize> {
        match self.table.get_mut(&key) {
            Some(entry) => {
                let prev = entry.value.as_ref().map(|v| v.len()).unwrap_or(0);
                entry.value = Some(value);
                Some(prev)
            }
            None => {
                self.table.insert(key, MemtableEntry { value: Some(value) });
                None
            }
        }
    }

    pub fn get(&self, key: &str) -> MemtableGetResult {
        match self.table.get(key) {
            Some(entry) => match &entry.value {
                Some(value) => MemtableGetResult::Found(value.clone()),
                None => MemtableGetResult::Deleted,
            },
            None => MemtableGetResult::NotFound,
        }
    }

    pub fn delete(&mut self, key: &str) -> Option<usize> {
        if let Some(entry) = self.table.get_mut(key) {
            let old_size = entry.value.as_ref().map(|v| v.len()).unwrap_or(0);

            entry.value = None;
            Some(old_size)
        } else {
            self.table
                .insert(key.to_string(), MemtableEntry { value: None });

            None
        }
    }
}
