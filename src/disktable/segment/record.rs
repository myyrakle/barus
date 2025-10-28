// Contents stored in table segments
#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct TableSegmentPayload {
    pub key: String,
    pub value: String,
}

// Determines the validity of records within a segment.
// It is written to the first byte of each Record in the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, bincode::Encode, bincode::Decode)]
#[repr(u8)]
pub enum RecordStateFlags {
    Nothing,
    Alive,
    Deleted,
    Unknown = 255,
}

impl From<u8> for RecordStateFlags {
    fn from(value: u8) -> Self {
        match value {
            0 => RecordStateFlags::Nothing,
            1 => RecordStateFlags::Alive,
            2 => RecordStateFlags::Deleted,
            _ => RecordStateFlags::Unknown,
        }
    }
}

impl RecordStateFlags {
    pub fn is_deleted(&self) -> bool {
        matches!(self, RecordStateFlags::Deleted)
    }
}
