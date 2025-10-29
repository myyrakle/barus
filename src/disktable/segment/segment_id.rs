use crate::errors;

// 16 length hex ID (ex 0000000D000000EA)
#[derive(
    Debug,
    Clone,
    PartialEq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct TableSegmentID(pub u64);

impl std::ops::Add<u64> for TableSegmentID {
    type Output = TableSegmentID;

    fn add(self, rhs: u64) -> Self::Output {
        TableSegmentID(self.0.saturating_add(rhs))
    }
}

impl TableSegmentID {
    pub fn new(id: u64) -> Self {
        TableSegmentID(id)
    }

    pub fn increment(&mut self) {
        self.0 = self.0.saturating_add(1);
    }
}

impl From<TableSegmentID> for u64 {
    fn from(val: TableSegmentID) -> Self {
        val.0
    }
}

impl From<&TableSegmentID> for String {
    fn from(val: &TableSegmentID) -> Self {
        format!("{:016X}", val.0)
    }
}

impl TryFrom<&str> for TableSegmentID {
    type Error = errors::Errors;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() != 16 {
            return Err(
                errors::Errors::new(errors::ErrorCodes::TableSegmentIDParseError)
                    .with_message("Invalid segment ID length".to_string()),
            );
        }

        let id = u64::from_str_radix(value, 16).map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::TableSegmentIDParseError)
                .with_message(format!("Failed to parse segment ID: {}", e))
        })?;

        Ok(TableSegmentID(id))
    }
}
