use memmap2::MmapMut;

use crate::errors;

pub struct WALSegmentFileWriteHandle {
    pub(crate) mmap: MmapMut,
}

impl WALSegmentFileWriteHandle {
    // empty writer (for initialization)
    pub fn empty() -> Self {
        Self {
            mmap: MmapMut::map_anon(0).unwrap(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.mmap.len() == 0
    }

    pub async fn new(file: tokio::fs::File) -> errors::Result<Self> {
        let mmap = unsafe {
            MmapMut::map_mut(&file).map_err(|e| {
                errors::Errors::WALSegmentFileOpenError(format!(
                    "Failed to mmap WAL segment file: {}",
                    e
                ))
            })?
        };

        Ok(Self { mmap })
    }

    pub fn flush(&self) -> errors::Result<()> {
        self.mmap.flush().map_err(|e| {
            errors::Errors::WALRecordWriteError(format!("Failed to flush WAL segment mmap: {}", e))
        })?;
        Ok(())
    }
}
