use std::cmp::{self, Ordering};

use memmap2::MmapMut;

use crate::errors;

// 16 length hex ID (ex 0000000D000000EA)
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct WALSegmentID(u64);

impl Ord for WALSegmentID {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for WALSegmentID {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::ops::Add<u64> for WALSegmentID {
    type Output = WALSegmentID;

    fn add(self, rhs: u64) -> Self::Output {
        WALSegmentID(self.0 + rhs)
    }
}

impl WALSegmentID {
    pub fn new(id: u64) -> Self {
        WALSegmentID(id)
    }

    pub fn increment(&mut self) {
        self.0 += 1;
    }
}

impl From<WALSegmentID> for u64 {
    fn from(val: WALSegmentID) -> Self {
        val.0
    }
}

impl From<&WALSegmentID> for String {
    fn from(val: &WALSegmentID) -> Self {
        format!("{:016X}", val.0)
    }
}

impl TryFrom<&str> for WALSegmentID {
    type Error = errors::Errors;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() != 16 {
            return Err(errors::Errors::WALSegmentIDParseError(
                "Invalid segment ID length".to_string(),
            ));
        }

        let id = u64::from_str_radix(value, 16).map_err(|e| {
            errors::Errors::WALSegmentIDParseError(format!("Failed to parse segment ID: {}", e))
        })?;

        Ok(WALSegmentID(id))
    }
}

pub struct WALSegmentWriteHandle {
    pub(crate) mmap: MmapMut,
}

impl WALSegmentWriteHandle {
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

    // pub fn write(&mut self, data: &[u8]) -> errors::Result<()> {
    //     let len = data.len();
    //     self.mmap[self.offset..self.offset + len].copy_from_slice(data);
    //     self.offset += len;
    //     Ok(())
    // }

    pub fn flush(&self) -> errors::Result<()> {
        self.mmap.flush().map_err(|e| {
            errors::Errors::WALRecordWriteError(format!("Failed to flush WAL segment mmap: {}", e))
        })?;
        Ok(())
    }
}
