use crate::errors;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WalRecord {
    pub record_id: u64,
    pub record_type: RecordType,
    pub data: serde_json::Value,
}

pub trait WalCodec {
    fn encode(&self) -> errors::Result<Vec<u8>>;
    fn decode(data: &[u8]) -> errors::Result<Self>
    where
        Self: Sized;
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum RecordType {
    #[serde(rename = "put")]
    Put,
    #[serde(rename = "delete")]
    Delete,
}

pub const WAL_SEGMENT_SIZE: usize = 1024 * 1024 * 16; // 16MB
