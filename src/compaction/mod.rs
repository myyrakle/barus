use std::{collections::HashMap, sync::Arc};

use tokio::sync::{Mutex, RwLock, mpsc::Receiver};

use crate::{
    disktable::DiskTableManager,
    errors,
    memtable::{HashMemtable, MemtableManager},
    wal::{
        WALManager,
        state::{WALGlobalState, WALStateWriteHandles},
    },
};

#[derive(Default)]
pub struct MemtableFlushEvent {
    pub memtable: Arc<RwLock<HashMap<String, Arc<RwLock<HashMemtable>>>>>,
    pub wal_state: Arc<Mutex<WALGlobalState>>,
}

#[derive(Debug)]
pub struct CompactionManager {
    memtable_flush_receiver: Receiver<MemtableFlushEvent>,

    // borrowed from disktable manager
    disktable_manager: Arc<DiskTableManager>,

    // borrowed from wal manager
    wal_state_write_handles: Arc<Mutex<WALStateWriteHandles>>,

    wal_manager: Arc<WALManager>,
}

impl CompactionManager {
    pub fn new(
        wal_manager: Arc<WALManager>,
        memtable_manager: &mut MemtableManager,
        disktable_manager: Arc<DiskTableManager>,
    ) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);

        memtable_manager.memtable_flush_sender = sender;

        CompactionManager {
            wal_state_write_handles: wal_manager.wal_state_write_handles.clone(),
            memtable_flush_receiver: receiver,
            disktable_manager: disktable_manager.clone(),
            wal_manager,
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
        let wal_manager = self.wal_manager.clone();
        let wal_state_write_handles = self.wal_state_write_handles.clone();

        tokio::spawn(async move {
            while let Some(event) = memtable_flush_receiver.recv().await {
                // Handle memtable flush event
                log::info!("Memtable flush event received");

                if let Err(error) = disk_manager
                    .write_memtable(
                        event.memtable,
                        event.wal_state,
                        wal_state_write_handles.clone(),
                    )
                    .await
                {
                    log::error!("Failed to write memtable: {}", error);
                }

                if let Err(error) = wal_manager.remove_old_wal_segments().await {
                    log::error!("Failed to remove old WAL segments: {}", error);
                }
            }
        });
    }
}
