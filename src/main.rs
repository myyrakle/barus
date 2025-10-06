pub mod http;
pub mod wal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    http::run_server().await;

    Ok(())
}
