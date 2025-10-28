use std::sync::Arc;

use tokio::sync::mpsc::Receiver;

use crate::{
    bridge::event::MemtableFlushEvent, disktable::DiskTableManager, errors,
    memtable::MemtableManager, wal::WALManager,
};

pub mod event;

// Mediates mutual calls between different layers.
#[derive(Debug)]
pub struct BridgeController {
    memtable_flush_receiver: Receiver<MemtableFlushEvent>,

    disktable_manager: Arc<DiskTableManager>,
    wal_manager: Arc<WALManager>,
}

impl BridgeController {
    pub fn new(
        wal_manager: Arc<WALManager>,
        memtable_manager: &mut MemtableManager,
        disktable_manager: Arc<DiskTableManager>,
    ) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);

        memtable_manager.memtable_flush_sender = sender;

        BridgeController {
            memtable_flush_receiver: receiver,
            disktable_manager: disktable_manager.clone(),
            wal_manager,
        }
    }

    // Start background tasks for the bridge controller.
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
        let wal_state_write_handles = self.wal_manager.wal_state_write_handles.clone();

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
