#[derive(Debug)]
pub enum Errors {
    WalRecordEncodeError(String),
    WalRecordDecodeError(String),
    WalRecordWriteError(String),
    WalStateReadError(String),
    WalStateDecodeError(String),
    WalStateWriteError(String),
}

pub type Result<T> = std::result::Result<T, Errors>;
