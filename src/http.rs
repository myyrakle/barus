use std::{collections::HashMap, sync::Arc};

use axum::{
    Extension, Json,
    extract::{Path, Query},
    response::{IntoResponse, Response},
    routing::{delete, post, put},
};

use crate::{config::HTTP_PORT, db::DBEngine, errors::Errors};

pub async fn run_server(db_engine: Arc<DBEngine>) {
    use axum::{Router, routing::get};

    let app = Router::new()
        .route("/", get(root))
        .route("/status", get(get_db_status))
        .route("/tables", get(list_tables))
        .route("/tables/{table}", get(get_table))
        .route("/tables/{table}", post(create_table))
        .route("/tables/{table}", delete(delete_table))
        .route("/tables/{table}/value", get(get_value))
        .route("/tables/{table}/value", put(put_value))
        .route("/tables/{table}/value", delete(delete_value))
        .route("/wal/flush", post(flush_wal))
        .layer(axum::extract::Extension(db_engine));

    let addr = format!("0.0.0.0:{}", *HTTP_PORT);

    log::info!("HTTP Server is running on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn root() -> &'static str {
    "OK"
}

#[derive(serde::Serialize)]
pub struct DBStatusResponse {
    pub table_count: usize,
    pub memtable_size: u64,
    pub wal_total_size: u64,
}

async fn get_db_status(Extension(db): Extension<Arc<DBEngine>>) -> impl IntoResponse {
    let status = db.get_db_status().await;

    match status {
        Ok(status) => {
            let response = DBStatusResponse {
                table_count: status.table_count,
                memtable_size: status.memtable_size,
                wal_total_size: status.wal_total_size,
            };

            Response::builder()
                .status(200)
                .header("Content-Type", "application/json")
                .body(serde_json::to_string(&response).unwrap())
                .unwrap()
        }
        Err(err) => {
            let error_message = format!("Error getting database status: {:?}", err);
            Response::builder().status(500).body(error_message).unwrap()
        }
    }
}

#[derive(serde::Serialize)]
pub struct GetTableResponse {
    pub table_name: String,
}

async fn get_table(
    Extension(db): Extension<Arc<DBEngine>>,
    Path(table): Path<String>,
) -> impl IntoResponse {
    let result = db.get_table(&table).await;

    match result {
        Ok(table) => {
            let response = GetTableResponse {
                table_name: table.name,
            };

            Response::builder()
                .status(200)
                .header("Content-Type", "application/json")
                .body(serde_json::to_string(&response).unwrap())
                .unwrap()
        }
        Err(err) => match err {
            Errors::TableNotFound(_) => {
                let error_message = format!("Table '{}' not found", table);
                Response::builder().status(404).body(error_message).unwrap()
            }
            Errors::TableNameIsEmpty => {
                let error_message = "Table name is empty".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::TableNameTooLong => {
                let error_message = "Table name is too long".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::TableNameIsInvalid(_) => {
                let error_message = "Table name is invalid".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            _ => {
                let error_message = format!("Error getting table '{}': {:?}", table, err);
                Response::builder().status(500).body(error_message).unwrap()
            }
        },
    }
}

#[derive(serde::Serialize)]
pub struct ListTablesResponse {
    pub tables: Vec<ListTablesResponseItem>,
}

#[derive(serde::Serialize)]
pub struct ListTablesResponseItem {
    pub table_name: String,
}

async fn list_tables(Extension(db): Extension<Arc<DBEngine>>) -> impl IntoResponse {
    match db.list_tables().await {
        Ok(list_tables_result) => {
            let tables_response_items: Vec<ListTablesResponseItem> = list_tables_result
                .tables
                .into_iter()
                .map(|table| ListTablesResponseItem {
                    table_name: table.table_name,
                })
                .collect();

            let response = ListTablesResponse {
                tables: tables_response_items,
            };

            Response::builder()
                .status(200)
                .header("Content-Type", "application/json")
                .body(serde_json::to_string(&response).unwrap())
                .unwrap()
        }
        Err(e) => {
            let error_message = format!("Error listing tables: {:?}", e);
            Response::builder().status(500).body(error_message).unwrap()
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct CreateTableRequest {}

async fn create_table(
    Extension(db): Extension<Arc<DBEngine>>,
    Path(table): Path<String>,
    Json(_req): Json<CreateTableRequest>,
) -> impl IntoResponse {
    match db.create_table(&table).await {
        Ok(_) => Response::builder()
            .status(200)
            .body(format!("Table '{}' created successfully", table))
            .unwrap(),
        Err(error) => match error {
            Errors::TableNameIsEmpty => {
                let error_message = "Table name is empty".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::TableNameTooLong => {
                let error_message = "Table name is too long".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::TableNameIsInvalid(_) => {
                let error_message = "Table name is invalid".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::TableAlreadyExists(_) => {
                let error_message = format!("Table '{}' already exists", table);
                Response::builder().status(409).body(error_message).unwrap()
            }
            _ => {
                let error_message = format!("Error creating table '{}': {:?}", table, error);
                Response::builder().status(500).body(error_message).unwrap()
            }
        },
    }
}

async fn delete_table(
    Extension(db): Extension<Arc<DBEngine>>,
    Path(table): Path<String>,
) -> impl IntoResponse {
    match db.delete_table(&table).await {
        Ok(_) => Response::builder()
            .status(200)
            .body(format!("Table '{}' deleted successfully", table))
            .unwrap(),
        Err(e) => match e {
            Errors::TableNameIsEmpty => {
                let error_message = "Table name is empty".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::TableNameTooLong => {
                let error_message = "Table name is too long".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::TableNameIsInvalid(_) => {
                let error_message = "Table name is invalid".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            _ => {
                let error_message = format!("Error deleting table '{}': {:?}", table, e);
                Response::builder().status(500).body(error_message).unwrap()
            }
        },
    }
}

#[derive(serde::Serialize)]
pub struct GetValueResponse<'a> {
    pub key: &'a str,
    pub value: String,
}

async fn get_value(
    Query(params): Query<HashMap<String, String>>,
    Path(table): Path<String>,
    Extension(db): Extension<Arc<DBEngine>>,
) -> impl IntoResponse {
    let Some(key) = params.get("key") else {
        return Response::builder()
            .status(400)
            .body("Missing 'key' parameter".into())
            .unwrap();
    };

    let result = db.get_value(&table, key).await;

    match result {
        Ok(res) => {
            let response = GetValueResponse {
                key,
                value: res.value,
            };

            Response::builder()
                .status(200)
                .header("Content-Type", "application/json")
                .body(serde_json::to_string(&response).unwrap())
                .unwrap()
        }
        Err(error) => match error {
            Errors::TableNotFound(_) => {
                let error_message = format!("Table '{}' not found", table);
                Response::builder().status(404).body(error_message).unwrap()
            }
            Errors::TableNameIsEmpty => {
                let error_message = "Table name is empty".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::TableNameTooLong => {
                let error_message = "Table name is too long".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::TableNameIsInvalid(_) => {
                let error_message = "Table name is invalid".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::KeyIsEmpty => Response::builder()
                .status(400)
                .body("Key cannot be empty".into())
                .unwrap(),
            Errors::KeySizeTooLarge => Response::builder()
                .status(400)
                .body("Key size is too large".into())
                .unwrap(),
            Errors::ValueNotFound(_) => Response::builder()
                .status(404)
                .body("Value not found".into())
                .unwrap(),
            _ => {
                let error_message = format!("Error retrieving key {}: {:?}", key, error);
                Response::builder().status(500).body(error_message).unwrap()
            }
        },
    }
}

#[derive(serde::Deserialize)]
pub struct PutValueRequest {
    pub key: String,
    pub value: String,
}

#[derive(serde::Serialize)]
pub struct PutValueResponse {
    pub message: String,
}

async fn put_value(
    Extension(db): Extension<Arc<DBEngine>>,
    Path(table): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> impl IntoResponse {
    let Some(key) = req
        .get("key")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
    else {
        return Response::builder()
            .status(400)
            .body("Missing 'key' in request body".into())
            .unwrap();
    };

    let Some(value) = req
        .get("value")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
    else {
        return Response::builder()
            .status(400)
            .body("Missing 'value' in request body".into())
            .unwrap();
    };

    let result = db.put_value(table.clone(), key, value).await;

    match result {
        Ok(_) => {
            let response = PutValueResponse {
                message: "Stored".to_string(),
            };

            Response::builder()
                .status(200)
                .header("Content-Type", "application/json")
                .body(serde_json::to_string(&response).unwrap())
                .unwrap()
        }
        Err(error) => match error {
            Errors::TableNotFound(_) => {
                let error_message = format!("Table '{}' not found", table);
                Response::builder().status(404).body(error_message).unwrap()
            }
            Errors::TableNameIsEmpty => {
                let error_message = "Table name is empty".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::TableNameTooLong => {
                let error_message = "Table name is too long".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::TableNameIsInvalid(_) => {
                let error_message = "Table name is invalid".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::KeyIsEmpty => Response::builder()
                .status(400)
                .body("Key cannot be empty".into())
                .unwrap(),
            Errors::KeySizeTooLarge => Response::builder()
                .status(400)
                .body("Key size is too large".into())
                .unwrap(),
            Errors::ValueSizeTooLarge => Response::builder()
                .status(400)
                .body("Value size is too large".into())
                .unwrap(),
            _ => {
                let error_message = format!("Error storing key: {:?}", error);
                Response::builder().status(500).body(error_message).unwrap()
            }
        },
    }
}

async fn delete_value(
    Query(mut params): Query<HashMap<String, String>>,
    Path(table): Path<String>,
    Extension(db): Extension<Arc<DBEngine>>,
) -> impl IntoResponse {
    let Some(key) = params.remove("key") else {
        return Response::builder()
            .status(400)
            .body("Missing 'key' parameter".to_string())
            .unwrap();
    };

    let result = db.delete_value(table.clone(), key).await;

    match result {
        Ok(_) => {
            let response = "Deleted".to_string();

            Response::builder()
                .status(200)
                .header("Content-Type", "text/plain")
                .body(response)
                .unwrap()
        }
        Err(error) => match error {
            Errors::TableNotFound(_) => {
                let error_message = format!("Table '{}' not found", table);
                Response::builder().status(404).body(error_message).unwrap()
            }
            Errors::TableNameIsEmpty => {
                let error_message = "Table name is empty".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::TableNameTooLong => {
                let error_message = "Table name is too long".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::TableNameIsInvalid(_) => {
                let error_message = "Table name is invalid".to_string();
                Response::builder().status(400).body(error_message).unwrap()
            }
            Errors::KeyIsEmpty => Response::builder()
                .status(400)
                .body("Key cannot be empty".into())
                .unwrap(),
            Errors::KeySizeTooLarge => Response::builder()
                .status(400)
                .body("Key size is too large".into())
                .unwrap(),
            _ => {
                let error_message = format!("Error deleting key: {:?}", error);
                Response::builder().status(500).body(error_message).unwrap()
            }
        },
    }
}

async fn flush_wal(Extension(db): Extension<Arc<DBEngine>>) -> impl IntoResponse {
    match db.flush_wal().await {
        Ok(_) => Response::builder()
            .status(200)
            .body("WAL flushed successfully".into())
            .unwrap(),
        Err(e) => {
            let error_message = format!("Error flushing WAL: {:?}", e);
            Response::builder().status(500).body(error_message).unwrap()
        }
    }
}
