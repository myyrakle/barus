use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::sync::{Mutex, RwLock};

use crate::{
    errors::{self, Errors},
    system::SystemInfo,
};

#[derive(Debug)]
pub struct MemtableManager {
    memtable_map: Arc<RwLock<HashMap<String, Arc<Mutex<HashMemtable>>>>>,
    memtable_current_size: Arc<AtomicU64>,
    #[allow(dead_code)]
    memtable_size_soft_limit: usize,
    memtable_size_hard_limit: usize,
}

impl MemtableManager {
    pub fn new(system_info: &SystemInfo) -> Self {
        let total_memory = system_info.total_memory;

        let memtable_size_soft_limit =
            (total_memory as f64 * crate::config::MEMTABLE_SIZE_SOFT_LIMIT_RATE) as usize;

        let memtable_size_hard_limit =
            (total_memory as f64 * crate::config::MEMTABLE_SIZE_HARD_LIMIT_RATE) as usize;

        Self {
            memtable_map: Arc::new(RwLock::new(HashMap::new())),
            memtable_current_size: Arc::new(AtomicU64::new(0)),
            memtable_size_soft_limit,
            memtable_size_hard_limit,
        }
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
        let mut memtable_map = self.memtable_map.write().await;

        if memtable_map.contains_key(table) {
            memtable_map.remove(table);
        }

        Ok(())
    }

    pub async fn put(&self, table: String, key: String, value: String) -> errors::Result<()> {
        let bytes = key.len() + value.len();

        // 1. blocking until enough space is available
        loop {
            let current = self.memtable_current_size.load(Ordering::SeqCst);
            if current + (bytes as u64) <= self.memtable_size_hard_limit as u64 {
                if self
                    .memtable_current_size
                    .compare_exchange(
                        current,
                        current + (bytes as u64),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    break;
                }
                // CAS 경쟁: 재시도
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
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
            None => Err(Errors::TableNotFound(format!("Table not found: {}", table))),
        }
    }

    pub async fn delete(&self, table: String, key: String) -> errors::Result<()> {
        let memtable_map = self.memtable_map.read().await;

        match memtable_map.get(&table) {
            Some(memtable) => {
                let mut memtable_lock = memtable.lock().await;

                match memtable_lock.delete(&key) {
                    Some(_) => (),
                    None => return Err(Errors::ValueNotFound(format!("Key not found: {}", key))),
                }

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
    table: HashMap<String, MemtableEntry>,
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
            None
        }
    }
}
