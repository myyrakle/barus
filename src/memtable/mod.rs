use std::{collections::HashMap, sync::Arc};

use tokio::sync::{Mutex, RwLock};

use crate::errors::{self, Errors};

#[derive(Debug)]
pub struct MemtableManager {
    memtable_map: Arc<RwLock<HashMap<String, Arc<Mutex<HashMemtable>>>>>,
}

impl MemtableManager {
    pub fn new() -> Self {
        Self {
            memtable_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn put(&self, table: &str, key: String, value: String) -> errors::Result<()> {
        let memtable = {
            let memtable_map = self.memtable_map.read().await;

            match memtable_map.get(table) {
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

    pub fn delete(&mut self, table: &str, key: &str) -> errors::Result<()> {
        let memtable_map = self.memtable_map.blocking_read();

        match memtable_map.get(table) {
            Some(memtable) => {
                let mut memtable_lock = memtable.blocking_lock();
                memtable_lock.delete(key);
                Ok(())
            }
            None => Err(Errors::TableNotFound(format!("Table not found: {}", table))),
        }
    }
}

#[derive(Debug)]
pub struct HashMemtable {
    table: HashMap<String, String>,
}

pub const MEMTABLE_CAPACITY: usize = 100000;

impl HashMemtable {
    pub fn new() -> Self {
        Self {
            table: HashMap::with_capacity(MEMTABLE_CAPACITY),
        }
    }

    pub fn put(&mut self, key: String, value: String) {
        self.table.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.table.get(key)
    }

    pub fn delete(&mut self, key: &str) {
        self.table.remove(key);
    }
}
