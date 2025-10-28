use std::{collections::HashMap, sync::Arc};

use tokio::sync::{Mutex, RwLock};

use crate::{memtable::table::Memtable, wal::state::WALGlobalState};

#[derive(Default)]
pub struct MemtableFlushEvent {
    pub memtable: Arc<RwLock<HashMap<String, Arc<RwLock<Memtable>>>>>,
    pub wal_state: Arc<Mutex<WALGlobalState>>,
}
