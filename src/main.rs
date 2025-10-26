pub mod compaction;
pub mod config;
pub mod db;
pub mod disktable;
pub mod errors;
pub mod grpc;
pub mod http;
pub mod lock;
pub mod memtable;
pub mod os;
pub mod system;
pub mod validate;
pub mod wal;

use db::DBEngine;
use std::{path::PathBuf, sync::Arc};

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn setup_logging() {
    unsafe {
        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "info");
        }
    }
    env_logger::init();
}

fn get_data_dir() -> PathBuf {
    let path = std::env::var("BARUS_DATA_DIR").unwrap_or_else(|_| "data".to_string());
    PathBuf::from(path)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_logging();

    log::info!("Initializing DB Engine...");

    // DB Engine 초기화 (한 번만)
    let db_engine = DBEngine::initialize(get_data_dir()).await?;

    // Arc로 감싸서 여러 서버가 공유
    let shared_db = Arc::new(db_engine);

    log::info!("Starting servers...");

    // HTTP 서버와 gRPC 서버를 동시에 실행
    let http_db = shared_db.clone();
    let http_server = tokio::spawn(async move {
        http::run_server(http_db).await;
    });

    let grpc_db = shared_db.clone();
    let grpc_server = tokio::spawn(async move {
        if let Err(e) = grpc::run_grpc_server(grpc_db).await {
            eprintln!("gRPC server error: {}", e);
        }
    });

    // 둘 중 하나라도 종료되면 프로그램 종료
    tokio::select! {
        _ = http_server => log::info!("HTTP server stopped"),
        _ = grpc_server => log::info!("gRPC server stopped"),
    }

    Ok(())
}
