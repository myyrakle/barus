use std::sync::Arc;
use tonic::{Request, Response, Status, transport::Server};

use crate::config::GRPC_PORT;
use crate::db::DBEngine;

// Include the generated proto code
pub mod barus {
    tonic::include_proto!("barus");
}

use barus::barus_service_server::{BarusService, BarusServiceServer};
use barus::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, HealthRequest, HealthResponse,
    PutRequest, PutResponse,
};

pub struct BarusGrpcService {
    db: Arc<DBEngine>,
}

impl BarusGrpcService {
    pub fn new(db: Arc<DBEngine>) -> Self {
        Self { db }
    }
}

#[tonic::async_trait]
impl BarusService for BarusGrpcService {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();

        if req.table.is_empty() {
            return Err(Status::invalid_argument("table name cannot be empty"));
        }

        if req.key.is_empty() {
            return Err(Status::invalid_argument("key cannot be empty"));
        }

        match self.db.get(&req.table, &req.key).await {
            Ok(result) => {
                let value = String::from_utf8_lossy(&result.value).to_string();
                Ok(Response::new(GetResponse {
                    key: req.key,
                    value,
                }))
            }
            Err(e) => Err(Status::internal(format!("Failed to get value: {:?}", e))),
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();

        if req.table.is_empty() {
            return Err(Status::invalid_argument("table name cannot be empty"));
        }

        if req.key.is_empty() {
            return Err(Status::invalid_argument("key cannot be empty"));
        }

        match self.db.put(req.table, req.key, req.value).await {
            Ok(_) => Ok(Response::new(PutResponse {
                message: "Stored".to_string(),
            })),
            Err(e) => Err(Status::internal(format!("Failed to put value: {:?}", e))),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();

        if req.table.is_empty() {
            return Err(Status::invalid_argument("table name cannot be empty"));
        }

        if req.key.is_empty() {
            return Err(Status::invalid_argument("key cannot be empty"));
        }

        match self.db.delete(req.table, req.key).await {
            Ok(_) => Ok(Response::new(DeleteResponse {
                message: "Deleted".to_string(),
            })),
            Err(e) => Err(Status::internal(format!("Failed to delete value: {:?}", e))),
        }
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "OK".to_string(),
        }))
    }
}

pub async fn run_grpc_server(db_engine: Arc<DBEngine>) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("0.0.0.0:{}", *GRPC_PORT).parse()?;

    println!("gRPC Server is running on {}", addr);

    let service = BarusGrpcService::new(db_engine);

    Server::builder()
        // 성능 최적화 설정
        .tcp_nodelay(true) // Nagle 알고리즘 비활성화
        .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)))
        .add_service(BarusServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
