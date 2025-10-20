#[derive(Debug, Clone)]
pub struct IndexManager {}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexManager {
    pub fn new() -> Self {
        Self {}
    }
}
