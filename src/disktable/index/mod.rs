use crate::{disktable::segment::TableRecordPosition, errors::Errors};

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

    pub async fn add_record(
        &self,
        _table_name: &str,
        _key: &str,
        _position: &TableRecordPosition,
    ) -> Result<(), Errors> {
        // Implementation goes here
        unimplemented!()
    }

    pub async fn delete_record(&self, _table_name: &str, _key: &str) -> Result<(), Errors> {
        // Implementation goes here
        unimplemented!()
    }

    pub async fn update_record(
        &self,
        _table_name: &str,
        _key: &str,
        _position: &TableRecordPosition,
    ) -> Result<(), Errors> {
        // Implementation goes here
        unimplemented!()
    }

    pub async fn find_record(
        &self,
        _table_name: &str,
        _key: &str,
    ) -> Result<Option<TableRecordPosition>, Errors> {
        // Implementation goes here
        unimplemented!()
    }
}
