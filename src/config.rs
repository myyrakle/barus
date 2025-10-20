use std::sync::LazyLock;

pub const HTTP_DEFAULT_PORT: u16 = 53000;
pub const GRPC_DEFAULT_PORT: u16 = 53001;

pub static HTTP_PORT: LazyLock<u16> = LazyLock::new(|| {
    std::env::var("BARUS_HTTP_PORT")
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(HTTP_DEFAULT_PORT)
});
pub static GRPC_PORT: LazyLock<u16> = LazyLock::new(|| {
    std::env::var("BARUS_GRPC_PORT")
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(GRPC_DEFAULT_PORT)
});

pub const WAL_SEGMENT_SIZE: usize = 1024 * 1024 * 32; // 32MB
pub const WAL_DIRECTORY: &str = "wal";
pub const WAL_STATE_PATH: &str = "wal_state.json";
pub const WAL_RECORD_HEADER_SIZE: usize = 4; // 4 bytes for record length

pub const MEMTABLE_SIZE_SOFT_LIMIT_RATE: f64 = 0.3; // 시스템 메모리의 30%
pub const MEMTABLE_SIZE_HARD_LIMIT_RATE: f64 = 0.5; // 시스템 메모리의 50%

pub const DISKTABLE_SEGMENT_SIZE: usize = 1024 * 1024 * 1024; // 1GB
pub const DISKTABLE_PAGE_SIZE: usize = 1024 * 1024; // 1MB
pub const DISKTABLE_PAGE_COUNT_PER_SEGMENT: usize = DISKTABLE_SEGMENT_SIZE / DISKTABLE_PAGE_SIZE; // 1024 pages
