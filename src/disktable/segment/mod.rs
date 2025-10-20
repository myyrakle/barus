pub mod record;

#[derive(Debug, Clone)]
pub struct TableSegmentManager {}

impl Default for TableSegmentManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TableSegmentManager {
    pub fn new() -> Self {
        Self {}
    }
}
