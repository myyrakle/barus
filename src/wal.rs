#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WalRecord {
    pub record_id: u64,
    pub record_type: RecordType,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum RecordType {
    #[serde(rename = "put")]
    Put,
    #[serde(rename = "delete")]
    Delete,
}
