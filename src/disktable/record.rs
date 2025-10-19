#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct DisktableRecord {
    pub record_type: DisktableRecordStatus,
    pub data: DisktablePayload,
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub enum DisktableRecordStatus {
    Alive,
    Deleted,
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct DisktablePayload {
    pub key: String,
    pub value: String,
}
