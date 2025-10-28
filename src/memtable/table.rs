use std::collections::HashMap;

pub const MEMTABLE_DEFAULT_CAPACITY: usize = 100000;

// Value type stored in the Memtable
#[derive(Clone, Debug)]
pub struct MemtableValue {
    pub value: Option<String>,
}

// In-memory key-value store
#[derive(Debug)]
pub struct Memtable {
    pub(crate) kv_map: HashMap<String, MemtableValue>,
}

impl Memtable {
    // Clear all entries in the Memtable
    pub fn clear(&mut self) {
        self.kv_map.clear();
    }
}

// Result of a get operation from Memtable
pub enum MemtableGetValueResult {
    Found(String),
    NotFound,
    Deleted,
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

impl Memtable {
    // Create a new empty Memtable
    pub fn new() -> Self {
        Self {
            kv_map: HashMap::with_capacity(MEMTABLE_DEFAULT_CAPACITY),
        }
    }

    // Returns previous value size if key existed
    pub fn put(&mut self, key: String, value: String) -> Option<usize> {
        match self.kv_map.get_mut(&key) {
            Some(entry) => {
                let prev = entry.value.as_ref().map(|v| v.len()).unwrap_or(0);
                entry.value = Some(value);
                Some(prev)
            }
            None => {
                self.kv_map
                    .insert(key, MemtableValue { value: Some(value) });
                None
            }
        }
    }

    // Get value for a key
    pub fn get(&self, key: &str) -> MemtableGetValueResult {
        match self.kv_map.get(key) {
            Some(entry) => match &entry.value {
                Some(value) => MemtableGetValueResult::Found(value.clone()),
                None => MemtableGetValueResult::Deleted,
            },
            None => MemtableGetValueResult::NotFound,
        }
    }

    // Delete a key, returning previous value size if existed
    pub fn delete(&mut self, key: &str) -> Option<usize> {
        if let Some(entry) = self.kv_map.get_mut(key) {
            let old_size = entry.value.as_ref().map(|v| v.len()).unwrap_or(0);

            entry.value = None;
            Some(old_size)
        } else {
            self.kv_map
                .insert(key.to_string(), MemtableValue { value: None });

            None
        }
    }
}
