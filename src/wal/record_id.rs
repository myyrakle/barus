use std::cmp;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Default,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct WALRecordID(u64);

impl WALRecordID {
    pub fn new(id: u64) -> Self {
        WALRecordID(id)
    }

    pub fn increment(&mut self) {
        self.0 = self.0.saturating_add(1);
    }

    pub fn add(&self, rhs: u64) -> Self {
        WALRecordID(self.0.saturating_add(rhs))
    }
}

impl cmp::Ord for WALRecordID {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl From<u64> for WALRecordID {
    fn from(val: u64) -> Self {
        WALRecordID(val)
    }
}
