use std::{collections::HashMap, sync::Arc};

use axum::{
    Extension, Json,
    extract::Query,
    response::{IntoResponse, Response},
    routing::{delete, put},
};

use crate::db::DBEngine;

pub async fn run_server() {
    println!("Server is running...");

    use axum::{Router, routing::get};

    let mut db_engine = DBEngine::new("data".into());
    db_engine
        .initialize()
        .await
        .expect("DB Initialization failed");

    let wrapped_db = Arc::new(db_engine);

    let app = Router::new()
        .route("/", get(root))
        .route("/value", get(get_value))
        .route("/value", put(put_value))
        .route("/value", delete(delete_value))
        .layer(axum::extract::Extension(wrapped_db));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
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
    Extension(db): Extension<Arc<DBEngine>>,
) -> impl IntoResponse {
    let Some(key) = params.get("key") else {
        return Response::builder()
            .status(400)
            .body("Missing 'key' parameter".into())
            .unwrap();
    };

    let result = db.get(key).await;

    match result {
        Ok(res) => {
            let response = GetValueResponse {
                key,
                value: String::from_utf8_lossy(&res.value).to_string(),
            };

            Response::builder()
                .status(200)
                .header("Content-Type", "application/json")
                .body(serde_json::to_string(&response).unwrap())
                .unwrap()
        }
        Err(e) => {
            let error_message = format!("Error retrieving key {}: {:?}", key, e);
            Response::builder()
                .status(500)
                .body(error_message)
                .unwrap()
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

    let result = db.put(&key, value.as_bytes()).await;

    match result {
        Ok(_) => {
            let response = PutValueResponse {
                message: format!("Put Value: {key} = {value}"),
            };

            Response::builder()
                .status(200)
                .header("Content-Type", "application/json")
                .body(serde_json::to_string(&response).unwrap())
                .unwrap()
        }
        Err(e) => {
            let error_message = format!("Error storing key {}: {:?}", key, e);
            Response::builder()
                .status(500)
                .body(error_message)
                .unwrap()
        }
    }
}

async fn delete_value(
    Query(params): Query<HashMap<String, String>>,
    Extension(db): Extension<Arc<DBEngine>>,
) -> impl IntoResponse {
    let Some(key) = params.get("key") else {
        return Response::builder()
            .status(400)
            .body("Missing 'key' parameter".to_string())
            .unwrap();
    };

    let result = db.delete(key).await;

    match result {
        Ok(_) => {
            let response = format!("Deleted key: {key}");

            Response::builder()
                .status(200)
                .header("Content-Type", "text/plain")
                .body(response)
                .unwrap()
        }
        Err(e) => {
            let error_message = format!("Error deleting key {}: {:?}", key, e);
            Response::builder()
                .status(500)
                .body(error_message)
                .unwrap()
        }
    }
}
