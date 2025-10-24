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
    CreateTableRequest, CreateTableResponse, DeleteRequest, DeleteResponse, DropTableRequest,
    DropTableResponse, FlushMemtableRequest, FlushMemtableResponse, FlushWalRequest,
    FlushWalResponse, GetDbStatusRequest, GetDbStatusResponse, GetRequest, GetResponse,
    GetTableRequest, GetTableResponse, HealthRequest, HealthResponse, ListTablesRequest,
    ListTablesResponse, PutRequest, PutResponse, TableInfo,
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
    async fn list_tables(
        &self,
        _request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        match self.db.list_tables().await {
            Ok(result) => {
                let tables = result
                    .tables
                    .into_iter()
                    .map(|item| TableInfo {
                        table_name: item.table_name,
                    })
                    .collect();

                Ok(Response::new(ListTablesResponse { tables }))
            }
            Err(e) => Err(Status::internal(format!("Failed to list tables: {:?}", e))),
        }
    }

    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        let req = request.into_inner();

        if req.table.is_empty() {
            return Err(Status::invalid_argument("table name cannot be empty"));
        }

        match self.db.create_table(&req.table).await {
            Ok(_) => Ok(Response::new(CreateTableResponse {
                message: format!("Table '{}' created successfully", req.table),
            })),
            Err(e) => Err(Status::internal(format!(
                "Failed to create table '{}': {:?}",
                req.table, e
            ))),
        }
    }

    async fn get_table(
        &self,
        request: Request<GetTableRequest>,
    ) -> Result<Response<GetTableResponse>, Status> {
        let req = request.into_inner();

        if req.table.is_empty() {
            return Err(Status::invalid_argument("table name cannot be empty"));
        }

        match self.db.get_table(&req.table).await {
            Ok(table_info) => Ok(Response::new(GetTableResponse {
                table_name: table_info.name,
            })),
            Err(e) => Err(Status::internal(format!(
                "Failed to get table '{}': {:?}",
                req.table, e
            ))),
        }
    }

    async fn drop_table(
        &self,
        request: Request<DropTableRequest>,
    ) -> Result<Response<DropTableResponse>, Status> {
        let req = request.into_inner();

        if req.table.is_empty() {
            return Err(Status::invalid_argument("table name cannot be empty"));
        }

        match self.db.delete_table(&req.table).await {
            Ok(_) => Ok(Response::new(DropTableResponse {
                message: format!("Table '{}' dropped successfully", req.table),
            })),
            Err(e) => Err(Status::internal(format!(
                "Failed to drop table '{}': {:?}",
                req.table, e
            ))),
        }
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();

        if req.table.is_empty() {
            return Err(Status::invalid_argument("table name cannot be empty"));
        }

        if req.key.is_empty() {
            return Err(Status::invalid_argument("key cannot be empty"));
        }

        match self.db.get_value(&req.table, &req.key).await {
            Ok(result) => {
                let value = result.value;
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

        match self.db.put_value(req.table, req.key, req.value).await {
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

        match self.db.delete_value(req.table, req.key).await {
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

    async fn flush_wal(
        &self,
        _request: Request<FlushWalRequest>,
    ) -> Result<Response<FlushWalResponse>, Status> {
        match self.db.flush_wal().await {
            Ok(_) => Ok(Response::new(FlushWalResponse {
                message: "WAL flushed successfully".to_string(),
            })),
            Err(e) => Err(Status::internal(format!("Failed to flush WAL: {:?}", e))),
        }
    }

    async fn get_db_status(
        &self,
        _request: Request<GetDbStatusRequest>,
    ) -> Result<Response<GetDbStatusResponse>, Status> {
        match self.db.get_db_status().await {
            Ok(status) => Ok(Response::new(GetDbStatusResponse {
                memtable_size: status.memtable_size,
                table_count: status.table_count as u64,
            })),
            Err(e) => Err(Status::internal(format!(
                "Failed to get DB status: {:?}",
                e
            ))),
        }
    }

    async fn flush_memtable(
        &self,
        _request: Request<FlushMemtableRequest>,
    ) -> Result<Response<FlushMemtableResponse>, Status> {
        match self.db.trigger_memtable_flush().await {
            Ok(_) => Ok(Response::new(FlushMemtableResponse {
                message: "Memtable flushed successfully".to_string(),
            })),
            Err(e) => {
                // Check for specific error types
                use crate::errors::Errors;
                match e {
                    Errors::MemtableFlushAlreadyInProgress => {
                        Err(Status::already_exists("Memtable flush already in progress"))
                    }
                    _ => Err(Status::internal(format!(
                        "Failed to flush memtable: {:?}",
                        e
                    ))),
                }
            }
        }
    }
}

pub async fn run_grpc_server(db_engine: Arc<DBEngine>) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("0.0.0.0:{}", *GRPC_PORT).parse()?;

    log::info!("gRPC Server is running on {}", addr);

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
