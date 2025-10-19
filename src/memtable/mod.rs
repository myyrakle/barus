use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
};

use tokio::sync::{Mutex, RwLock};

use crate::errors::{self, Errors};

pub const MEMTABLE_SIZE_SOFT_LIMIT: usize = 512 * 1024 * 1024; // 512MB
pub const MEMTABLE_SIZE_HARD_LIMIT: usize = 1024 * 1024 * 1024; // 1024MB

#[derive(Debug)]
pub struct MemtableManager {
    memtable_map: Arc<RwLock<HashMap<String, Arc<Mutex<HashMemtable>>>>>,
    memtable_current_size: Arc<AtomicU64>,
}

impl MemtableManager {
    pub fn new() -> Self {
        Self {
            memtable_map: Arc::new(RwLock::new(HashMap::new())),
            memtable_current_size: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn put(&self, table: String, key: String, value: String) -> errors::Result<()> {
        let bytes = key.len() + value.len();
        let currnet_size = self
            .memtable_current_size
            .fetch_add(bytes as u64, std::sync::atomic::Ordering::SeqCst);

        if key == "f1c44c34-d646-4740-9307-7d9e7719e295" {
            println!(
                "memtable_current_size after adding key {}: {}",
                key,
                currnet_size + (bytes as u64)
            );
        }

        // blocking write if limit exceeded
        if currnet_size + (bytes as u64) > MEMTABLE_SIZE_HARD_LIMIT as u64 {
            println!(
                "Memtable size limit exceeded ({} bytes). Blocking write...",
                MEMTABLE_SIZE_HARD_LIMIT
            );

            loop {
                let current_size = self
                    .memtable_current_size
                    .load(std::sync::atomic::Ordering::SeqCst);

                if current_size + (bytes as u64) < MEMTABLE_SIZE_HARD_LIMIT as u64 {
                    break;
                }

                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }

        let memtable = {
            let memtable_map = self.memtable_map.read().await;

            match memtable_map.get(&table) {
                Some(memtable) => memtable.clone(),
                None => {
                    drop(memtable_map); // Release read lock before acquiring write lock

                    let new_memtable = Arc::new(Mutex::new(HashMemtable::new()));
                    let mut memtable_map = self.memtable_map.write().await;
                    memtable_map.insert(table.to_string(), new_memtable.clone());
                    new_memtable
                }
            }
        };

        let mut memtable_lock = memtable.lock().await;
        memtable_lock.put(key, value);

        Ok(())
    }

    pub async fn get(&self, table: &str, key: &str) -> errors::Result<String> {
        println!(
            "current size of memtable: {}",
            self.memtable_current_size
                .load(std::sync::atomic::Ordering::SeqCst)
        );

        let memtable_map = self.memtable_map.read().await;

        match memtable_map.get(table) {
            Some(memtable) => {
                let memtable_lock = memtable.lock().await;
                match memtable_lock.get(key) {
                    Some(value) => Ok(value.clone()),
                    None => Err(Errors::ValueNotFound(format!("Key not found: {}", key))),
                }
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

impl HashMemtable {
    pub fn new() -> Self {
        Self {
            table: HashMap::with_capacity(MEMTABLE_CAPACITY),
        }
    }

    pub fn put(&mut self, key: String, value: String) {
        self.table.insert(key, MemtableEntry { value: Some(value) });
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.table.get(key).and_then(|entry| entry.value.as_ref())
    }

    pub fn delete(&mut self, key: &str) -> Option<()> {
        if let Some(entry) = self.table.get_mut(key) {
            entry.value = None;
            Some(())
        } else {
            None
        }
    }
}
