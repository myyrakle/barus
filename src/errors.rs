use std::backtrace::Backtrace;

#[derive(Debug)]
pub struct Errors {
    pub error_code: ErrorCodes,
    pub backtrace: Backtrace,
    pub message: Option<String>,
}

impl Errors {
    pub fn new(error_code: ErrorCodes) -> Self {
        Errors {
            error_code,
            backtrace: Backtrace::capture(),
            message: None,
        }
    }

    pub fn with_message(mut self, message: String) -> Self {
        self.message = Some(message);
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ErrorCodes {
    // WAL related errors
    WALInitializationError,
    WALRecordEncodeError,
    WALRecordDecodeError,
    WALRecordWriteError,
    WALStateReadError,
    WALStateDecodeError,
    WALStateEncodeError,
    WALStateWriteError,
    WALSegmentIDParseError,
    WALSegmentFileOpenError,
    WALSegmentFileDeleteError,

    // Table related errors
    TableSegmentIDParseError,
    TableSegmentFileCreateError,
    TableSegmentFileOpenError,
    TableSegmentFileWriteError,
    TableRecordDecodeError,
    TableRecordEncodeError,
    TableCreationError,

    // General Errors
    FileOpenError,
    FileMetadataError,
    FileSeekError,
    FileReadError,
    FileWriteError,
    FileDeleteError,

    // User Bad Request Errors
    TableNotFound,
    ValueNotFound,
    TableAlreadyExists,
    TableNameIsEmpty,
    TableNameTooLong,
    TableNameIsInvalid,
    KeyIsEmpty,
    KeySizeTooLarge,
    ValueSizeTooLarge,
    MemtableFlushAlreadyInProgress,

    // Internal Errors
    TableListFailed,
    TableGetFailed,
    WALStateFileHandleNotFound,
    UnknownTableRecordHeaderFlag,
}

impl std::fmt::Display for ErrorCodes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCodes::WALInitializationError => write!(f, "WAL Initialization Error"),
            ErrorCodes::WALRecordEncodeError => write!(f, "WAL Record Encode Error"),
            ErrorCodes::WALRecordDecodeError => write!(f, "WAL Record Decode Error"),
            ErrorCodes::WALRecordWriteError => write!(f, "WAL Record Write Error"),
            ErrorCodes::WALStateReadError => write!(f, "WAL State Read Error"),
            ErrorCodes::WALStateDecodeError => write!(f, "WAL State Decode Error"),
            ErrorCodes::WALStateEncodeError => write!(f, "WAL State Encode Error"),
            ErrorCodes::WALStateWriteError => write!(f, "WAL State Write Error"),
            ErrorCodes::WALSegmentIDParseError => write!(f, "WAL Segment ID Parse Error"),
            ErrorCodes::WALSegmentFileOpenError => write!(f, "WAL Segment File Open Error"),
            ErrorCodes::WALSegmentFileDeleteError => write!(f, "WAL Segment File Delete Error"),
            ErrorCodes::TableSegmentIDParseError => write!(f, "Table Segment ID Parse Error"),
            ErrorCodes::TableSegmentFileCreateError => write!(f, "Table Segment File Create Error"),
            ErrorCodes::TableSegmentFileWriteError => write!(f, "Table Segment File Write Error"),
            ErrorCodes::TableNotFound => write!(f, "Table Not Found"),
            ErrorCodes::ValueNotFound => write!(f, "Value Not Found"),
            ErrorCodes::TableAlreadyExists => write!(f, "Table Already Exists"),
            ErrorCodes::TableCreationError => write!(f, "Table Creation Error"),
            ErrorCodes::TableNameIsEmpty => write!(f, "Table Name Is Empty"),
            ErrorCodes::TableNameIsInvalid => write!(f, "Table Name Is Invalid"),
            ErrorCodes::TableNameTooLong => write!(f, "Table Name Too Long"),
            ErrorCodes::TableGetFailed => write!(f, "Table Get Failed"),
            ErrorCodes::TableListFailed => write!(f, "Table List Failed"),
            ErrorCodes::KeySizeTooLarge => write!(f, "Key Size Too Large"),
            ErrorCodes::KeyIsEmpty => write!(f, "Key Is Empty"),
            ErrorCodes::ValueSizeTooLarge => write!(f, "Value Size Too Large"),
            ErrorCodes::FileOpenError => write!(f, "File Open Error"),
            ErrorCodes::FileMetadataError => write!(f, "File Metadata Error"),
            ErrorCodes::FileSeekError => write!(f, "File Seek Error"),
            ErrorCodes::FileReadError => write!(f, "File Read Error"),
            ErrorCodes::FileWriteError => write!(f, "File Write Error"),
            ErrorCodes::FileDeleteError => write!(f, "File Delete Error"),
            ErrorCodes::MemtableFlushAlreadyInProgress => {
                write!(f, "Memtable Flush Already In Progress")
            }
            ErrorCodes::TableSegmentFileOpenError => write!(f, "Table Segment File Open Error"),
            ErrorCodes::WALStateFileHandleNotFound => write!(f, "WAL State File Handle Not Found"),
            ErrorCodes::TableRecordDecodeError => write!(f, "Table Record Decode Error"),
            ErrorCodes::TableRecordEncodeError => write!(f, "Table Record Encode Error"),
            ErrorCodes::UnknownTableRecordHeaderFlag => {
                write!(f, "Unknown Table Record Header Flag")
            }
        }
    }
}

impl std::fmt::Display for Errors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(msg) = &self.message {
            write!(f, "{}: {}", self.error_code, msg)
        } else {
            write!(f, "{}", self.error_code)
        }
    }
}

impl std::error::Error for ErrorCodes {}
impl std::error::Error for Errors {}

impl From<ErrorCodes> for Errors {
    fn from(error_code: ErrorCodes) -> Self {
        Errors::new(error_code)
    }
}

pub type Result<T> = std::result::Result<T, Errors>;
