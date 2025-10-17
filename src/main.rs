pub mod db;
pub mod errors;
pub mod grpc;
pub mod http;
pub mod lock;
pub mod wal;

use db::DBEngine;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Initializing DB Engine...");

    // DB Engine 초기화 (한 번만)
    let mut db_engine = DBEngine::new("data".into());
    db_engine.initialize().await?;

    // Arc로 감싸서 여러 서버가 공유
    let shared_db = Arc::new(db_engine);

    println!("Starting servers...");

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
        _ = http_server => println!("HTTP server stopped"),
        _ = grpc_server => println!("gRPC server stopped"),
    }

    Ok(())
}
