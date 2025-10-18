use std::cell::LazyCell;

pub const HTTP_DEFAULT_PORT: u16 = 53000;
pub const GRPC_DEFAULT_PORT: u16 = 53001;

pub const HTTP_PORT: LazyCell<u16> = LazyCell::new(|| {
    std::env::var("BARUS_HTTP_PORT")
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(HTTP_DEFAULT_PORT)
});
pub const GRPC_PORT: LazyCell<u16> = LazyCell::new(|| {
    std::env::var("BARUS_GRPC_PORT")
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(GRPC_DEFAULT_PORT)
});
