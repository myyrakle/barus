use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
};

use tokio::sync::{
    Mutex, RwLock,
    mpsc::{Receiver, Sender},
};

use crate::{
    disktable::DiskTableManager,
    errors,
    memtable::{HashMemtable, MemtableManager},
};

#[derive(Default)]
pub struct MemtableFlushEvent;

#[derive(Debug)]
pub struct CompactionManager {
    memtable_flush_receiver: Receiver<MemtableFlushEvent>,
    memtable_flush_sender: Sender<MemtableFlushEvent>,

    // borrowed from memtable manager
    memtable_map: Arc<RwLock<HashMap<String, Arc<Mutex<HashMemtable>>>>>,
    memtable_current_size: Arc<AtomicU64>,

    // borrowed from disktable manager
    disktable_manager: Arc<DiskTableManager>,
}

impl CompactionManager {
    pub fn new(
        memtable_manager: &mut MemtableManager,
        disktable_manager: Arc<DiskTableManager>,
    ) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);

        CompactionManager {
            memtable_flush_receiver: receiver,
            memtable_flush_sender: sender,
            memtable_map: memtable_manager.memtable_map.clone(),
            memtable_current_size: memtable_manager.memtable_current_size.clone(),
            disktable_manager: disktable_manager.clone(),
        }
    }

    pub fn start_background(&mut self) -> errors::Result<()> {
        self.start_memtable_flush_task();
        Ok(())
    }

    fn start_memtable_flush_task(&mut self) {
        let (_, fake_receiver) = tokio::sync::mpsc::channel(1);

        let mut memtable_flush_receiver =
            std::mem::replace(&mut self.memtable_flush_receiver, fake_receiver);

        tokio::spawn(async move {
            while let Some(_event) = memtable_flush_receiver.recv().await {
                // Handle memtable flush event
                log::debug!("Memtable flush event received");
            }
        });
    }
}
