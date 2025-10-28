use crate::disktable::segment::segment_id::TableSegmentID;

// Position information within the Record segment file
#[derive(Debug, Clone, bincode::Decode, bincode::Encode)]
pub struct TableRecordPosition {
    pub segment_id: TableSegmentID,
    pub offset: u32,
}
