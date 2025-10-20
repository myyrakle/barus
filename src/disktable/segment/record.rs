#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct TableSegmentRecord {
    pub record_type: TableSegmentRecordStatus,
    pub data: TableSegmentPayload,
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub enum TableSegmentRecordStatus {
    Alive,
    Deleted,
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct TableSegmentPayload {
    pub key: String,
    pub value: String,
}
