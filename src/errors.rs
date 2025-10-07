#[derive(Debug)]
pub enum Errors {}

pub type Result<T> = std::result::Result<T, Errors>;
