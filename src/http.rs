use std::{collections::HashMap, sync::Arc};

use axum::{
    Extension, Json,
    extract::{Path, Query},
    response::{IntoResponse, Response},
    routing::{delete, post, put},
};

use crate::{config::HTTP_PORT, db::DBEngine};

pub async fn run_server(db_engine: Arc<DBEngine>) {
    use axum::{Router, routing::get};

    let app = Router::new()
        .route("/", get(root))
        .route("/tables/{table}/value", get(get_value))
        .route("/tables/{table}/value", put(put_value))
        .route("/tables/{table}/value", delete(delete_value))
        .route("/wal/flush", post(flush_wal))
        .layer(axum::extract::Extension(db_engine));

    let addr = format!("0.0.0.0:{}", *HTTP_PORT);

    println!("HTTP Server is running on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn root() -> &'static str {
    "OK"
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

    println!("table requested: {}, key requested: {}", table, key);

    let result = db.get(&table, key).await;

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
        Err(e) => {
            let error_message = format!("Error retrieving key {}: {:?}", key, e);
            Response::builder().status(500).body(error_message).unwrap()
        }
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

    let result = db.put(table, key, value).await;

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
        Err(e) => {
            let error_message = format!("Error storing key: {:?}", e);
            Response::builder().status(500).body(error_message).unwrap()
        }
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

    let result = db.delete(table, key).await;

    match result {
        Ok(_) => {
            let response = "Deleted".to_string();

            Response::builder()
                .status(200)
                .header("Content-Type", "text/plain")
                .body(response)
                .unwrap()
        }
        Err(e) => {
            let error_message = format!("Error deleting key: {:?}", e);
            Response::builder().status(500).body(error_message).unwrap()
        }
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
