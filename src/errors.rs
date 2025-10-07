#[derive(Debug)]
pub enum Errors {
    WalInitializationError(String),
    WalRecordEncodeError(String),
    WalRecordDecodeError(String),
    WalRecordWriteError(String),
    WalStateReadError(String),
    WalStateDecodeError(String),
    WalStateEncodeError(String),
    WalStateWriteError(String),
}

pub type Result<T> = std::result::Result<T, Errors>;
