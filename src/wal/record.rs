#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct WALRecordID(u64);

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub struct WALPayload {
    pub table: String,
    pub key: String,
    pub value: Option<String>,
}

impl WALPayload {
    pub fn size(&self) -> usize {
        let table_size = self.table.len();
        let key_size = self.key.len();
        let value_size = match &self.value {
            Some(v) => v.len(),
            None => 0,
        };

        // 8 bytes for table length, 8 bytes for key length, 8 bytes for value length
        8 + table_size + 8 + key_size + 8 + value_size
    }
}

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub struct WALRecord {
    pub record_id: u64,
    pub record_type: RecordType,
    pub data: WALPayload,
}

impl WALRecord {
    pub fn size(&self) -> usize {
        let payload_size = self.data.size();
        // 8 bytes for record_id, 1 byte for record_type
        8 + 1 + payload_size
    }
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
