#[derive(Debug)]
pub enum Errors {
    // WAL related errors
    WALInitializationError(String),
    WALRecordEncodeError(String),
    WALRecordDecodeError(String),
    WALRecordWriteError(String),
    WALStateReadError(String),
    WALStateDecodeError(String),
    WALStateEncodeError(String),
    WALStateWriteError(String),
    WALSegmentIDParseError(String),
    WALSegmentFileOpenError(String),

    // Table related errors
    TableSegmentIDParseError(String),
    TableSegmentFileCreateError(String),
    TableSegmentFileOpenError(String),
    TableRecordDecodeError(String),
    TableRecordEncodeError(String),

    TableCreationError(String),
    FileOpenError(String),
    FileMetadataError(String),
    FileSeekError(String),
    FileReadError(String),
    TableListFailed(String),
    TableGetFailed(String),
    WALStateFileHandleNotFound,
    UnknownTableRecordHeaderFlag,

    // User Bad Request Errors
    TableNotFound(String),
    ValueNotFound(String),
    TableAlreadyExists(String),
    TableNameIsEmpty,
    TableNameTooLong,
    TableNameIsInvalid(String),
    KeyIsEmpty,
    KeySizeTooLarge,
    ValueSizeTooLarge,
    MemtableFlushAlreadyInProgress,
}

impl std::fmt::Display for Errors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Errors::WALInitializationError(msg) => write!(f, "WAL Initialization Error: {}", msg),
            Errors::WALRecordEncodeError(msg) => write!(f, "WAL Record Encode Error: {}", msg),
            Errors::WALRecordDecodeError(msg) => write!(f, "WAL Record Decode Error: {}", msg),
            Errors::WALRecordWriteError(msg) => write!(f, "WAL Record Write Error: {}", msg),
            Errors::WALStateReadError(msg) => write!(f, "WAL State Read Error: {}", msg),
            Errors::WALStateDecodeError(msg) => write!(f, "WAL State Decode Error: {}", msg),
            Errors::WALStateEncodeError(msg) => write!(f, "WAL State Encode Error: {}", msg),
            Errors::WALStateWriteError(msg) => write!(f, "WAL State Write Error: {}", msg),
            Errors::WALSegmentIDParseError(msg) => write!(f, "WAL Segment ID Parse Error: {}", msg),
            Errors::WALSegmentFileOpenError(msg) => {
                write!(f, "WAL Segment File Open Error: {}", msg)
            }
            Errors::TableSegmentIDParseError(msg) => {
                write!(f, "Table Segment ID Parse Error: {}", msg)
            }
            Errors::TableSegmentFileCreateError(msg) => {
                write!(f, "Table Segment File Create Error: {}", msg)
            }
            Errors::TableNotFound(msg) => write!(f, "Table Not Found: {}", msg),
            Errors::ValueNotFound(msg) => write!(f, "Value Not Found: {}", msg),
            Errors::TableAlreadyExists(msg) => write!(f, "Table Already Exists: {}", msg),
            Errors::TableCreationError(msg) => write!(f, "Table Creation Error: {}", msg),
            Errors::TableNameIsEmpty => write!(f, "Table Name Is Empty"),
            Errors::TableNameIsInvalid(msg) => write!(f, "Table Name Is Invalid: {}", msg),
            Errors::TableNameTooLong => write!(f, "Table Name Too Long"),
            Errors::TableGetFailed(msg) => write!(f, "Table Get Failed: {}", msg),
            Errors::TableListFailed(msg) => write!(f, "Table List Failed: {}", msg),
            Errors::KeySizeTooLarge => write!(f, "Key Size Too Large"),
            Errors::KeyIsEmpty => write!(f, "Key Is Empty"),
            Errors::ValueSizeTooLarge => write!(f, "Value Size Too Large"),
            Errors::FileOpenError(msg) => write!(f, "File Open Error: {}", msg),
            Errors::FileMetadataError(msg) => write!(f, "File Metadata Error: {}", msg),
            Errors::FileSeekError(msg) => write!(f, "File Seek Error: {}", msg),
            Errors::FileReadError(msg) => write!(f, "File Read Error: {}", msg),
            Errors::MemtableFlushAlreadyInProgress => {
                write!(f, "Memtable Flush Already In Progress")
            }
            Errors::TableSegmentFileOpenError(msg) => {
                write!(f, "Table Segment File Open Error: {}", msg)
            }
            Errors::WALStateFileHandleNotFound => {
                write!(f, "WAL State File Handle Not Found")
            }
            Errors::TableRecordDecodeError(msg) => {
                write!(f, "Table Record Decode Error: {}", msg)
            }
            Errors::TableRecordEncodeError(msg) => {
                write!(f, "Table Record Encode Error: {}", msg)
            }
            Errors::UnknownTableRecordHeaderFlag => {
                write!(f, "Unknown Table Record Header Flag")
            }
        }
    }
}

impl std::error::Error for Errors {}

pub type Result<T> = std::result::Result<T, Errors>;
