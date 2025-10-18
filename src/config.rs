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
