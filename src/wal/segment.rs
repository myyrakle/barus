use memmap2::MmapMut;

use crate::errors;

// 16 length hex ID (ex 0000000D000000EA)
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct WalSegmentID(u64);

impl std::ops::Add<u64> for WalSegmentID {
    type Output = WalSegmentID;

    fn add(self, rhs: u64) -> Self::Output {
        WalSegmentID(self.0 + rhs)
    }
}

impl WalSegmentID {
    pub fn new(id: u64) -> Self {
        WalSegmentID(id)
    }

    pub fn increment(&mut self) {
        self.0 += 1;
    }
}

impl From<WalSegmentID> for u64 {
    fn from(val: WalSegmentID) -> Self {
        val.0
    }
}

impl From<&WalSegmentID> for String {
    fn from(val: &WalSegmentID) -> Self {
        format!("{:016X}", val.0)
    }
}

impl TryFrom<&str> for WalSegmentID {
    type Error = errors::Errors;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() != 16 {
            return Err(errors::Errors::WalSegmentIDParseError(
                "Invalid segment ID length".to_string(),
            ));
        }

        let id = u64::from_str_radix(value, 16).map_err(|e| {
            errors::Errors::WalSegmentIDParseError(format!("Failed to parse segment ID: {}", e))
        })?;

        Ok(WalSegmentID(id))
    }
}

pub struct WALSegmentWriteHandle {
    mmap: MmapMut,
    offset: usize,
}

impl WALSegmentWriteHandle {
    // empty writer (for initialization)
    pub fn empty() -> Self {
        Self {
            mmap: MmapMut::map_anon(0).unwrap(),
            offset: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.mmap.len() == 0
    }

    pub async fn new(file: tokio::fs::File, offset: usize) -> errors::Result<Self> {
        let mmap = unsafe {
            MmapMut::map_mut(&file).map_err(|e| {
                errors::Errors::WalSegmentFileOpenError(format!(
                    "Failed to mmap WAL segment file: {}",
                    e
                ))
            })?
        };

        Ok(Self { mmap, offset })
    }

    pub fn write(&mut self, data: &[u8]) -> errors::Result<()> {
        let len = data.len();
        self.mmap[self.offset..self.offset + len].copy_from_slice(data);
        self.offset += len;
        Ok(())
    }

    pub fn flush(&self) -> errors::Result<()> {
        self.mmap.flush().map_err(|e| {
            errors::Errors::WalRecordWriteError(format!("Failed to flush WAL segment mmap: {}", e))
        })?;
        Ok(())
    }
}
