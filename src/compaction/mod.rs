use crate::errors;

#[derive(Debug, Clone)]
pub struct CompactionManager {}

impl CompactionManager {
    pub fn new() -> Self {
        CompactionManager {}
    }

    pub fn start_background(&self) -> errors::Result<()> {
        Ok(())
    }
}
