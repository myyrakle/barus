#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct WalRecordID(u64);

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub struct WalPayload {
    pub table: String,
    pub key: String,
    pub value: Option<String>,
}

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub struct WalRecord {
    pub record_id: u64,
    pub record_type: RecordType,
    pub data: WalPayload,
}

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub enum RecordType {
    #[serde(rename = "put")]
    Put,
    #[serde(rename = "delete")]
    Delete,
}
