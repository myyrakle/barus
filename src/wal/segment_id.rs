use std::cmp::Ordering;

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
