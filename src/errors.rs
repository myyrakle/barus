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
    WalSegmentIDParseError(String),
    WalSegmentFileOpenError(String),
    TableNotFound(String),
    ValueNotFound(String),
}

impl std::fmt::Display for Errors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Errors::WalInitializationError(msg) => write!(f, "WAL Initialization Error: {}", msg),
            Errors::WalRecordEncodeError(msg) => write!(f, "WAL Record Encode Error: {}", msg),
            Errors::WalRecordDecodeError(msg) => write!(f, "WAL Record Decode Error: {}", msg),
            Errors::WalRecordWriteError(msg) => write!(f, "WAL Record Write Error: {}", msg),
            Errors::WalStateReadError(msg) => write!(f, "WAL State Read Error: {}", msg),
            Errors::WalStateDecodeError(msg) => write!(f, "WAL State Decode Error: {}", msg),
            Errors::WalStateEncodeError(msg) => write!(f, "WAL State Encode Error: {}", msg),
            Errors::WalStateWriteError(msg) => write!(f, "WAL State Write Error: {}", msg),
            Errors::WalSegmentIDParseError(msg) => write!(f, "WAL Segment ID Parse Error: {}", msg),
            Errors::WalSegmentFileOpenError(msg) => {
                write!(f, "WAL Segment File Open Error: {}", msg)
            }
            Errors::TableNotFound(msg) => write!(f, "Table Not Found: {}", msg),
            Errors::ValueNotFound(msg) => write!(f, "Value Not Found: {}", msg),
        }
    }
}

impl std::error::Error for Errors {}

pub type Result<T> = std::result::Result<T, Errors>;
