use crate::{memtable::MemtableMap, wal::SharedWALState};

#[derive(Default)]
pub struct MemtableFlushEvent {
    pub memtable: MemtableMap,
    pub wal_state: SharedWALState,
}

impl MemtableFlushEvent {
    pub fn make_channel() -> (MemtableFlushEventSender, MemtableFlushEventReceiver) {
        tokio::sync::mpsc::channel(1)
    }
}

pub type MemtableFlushEventSender = tokio::sync::mpsc::Sender<MemtableFlushEvent>;
pub type MemtableFlushEventReceiver = tokio::sync::mpsc::Receiver<MemtableFlushEvent>;
