use std::{collections::HashMap, sync::Arc};

use tokio::sync::{Mutex, mpsc::Receiver};

use crate::{
    disktable::DiskTableManager,
    errors,
    memtable::{HashMemtable, MemtableManager},
    wal::state::WALGlobalState,
};

#[derive(Default)]
pub struct MemtableFlushEvent {
    pub memtable: HashMap<String, Arc<Mutex<HashMemtable>>>,
    pub wal_state: WALGlobalState,
}

#[derive(Debug)]
pub struct CompactionManager {
    memtable_flush_receiver: Receiver<MemtableFlushEvent>,

    // borrowed from disktable manager
    disktable_manager: Arc<DiskTableManager>,
}

impl CompactionManager {
    pub fn new(
        memtable_manager: &mut MemtableManager,
        disktable_manager: Arc<DiskTableManager>,
    ) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);

        memtable_manager.memtable_flush_sender = sender;

        CompactionManager {
            memtable_flush_receiver: receiver,
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

        let disk_manager = self.disktable_manager.clone();

        tokio::spawn(async move {
            while let Some(event) = memtable_flush_receiver.recv().await {
                // Handle memtable flush event
                log::info!("Memtable flush event received");

                if let Err(error) = disk_manager
                    .write_memtable(event.memtable, event.wal_state)
                    .await
                {
                    log::error!("Failed to write memtable: {}", error);
                }
            }
        });
    }
}
