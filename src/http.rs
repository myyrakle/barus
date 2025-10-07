use std::collections::HashMap;

use axum::{
    Json,
    extract::Query,
    response::{IntoResponse, Response},
    routing::put,
};

pub async fn run_server() {
    println!("Server is running...");

    use axum::{Router, routing::get};

    let app = Router::new()
        .route("/", get(root))
        .route("/value", get(get_value))
        .route("/value", put(put_value));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn root() -> &'static str {
    "OK"
}

#[derive(serde::Serialize)]
pub struct GetValueResponse {
    pub key: String,
    pub value: String,
}

async fn get_value(Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let Some(key) = params.get("key") else {
        return Response::builder()
            .status(400)
            .body("Missing 'key' parameter".into())
            .unwrap();
    };

    let response = GetValueResponse {
        key: key.clone(),
        value: format!("Value for key: {key}"),
    };

    Response::builder()
        .status(200)
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&response).unwrap())
        .unwrap()
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

async fn put_value(Json(req): Json<serde_json::Value>) -> impl IntoResponse {
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

    let response = PutValueResponse {
        message: format!("Put Value: {key} = {value}"),
    };

    Response::builder()
        .status(200)
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&response).unwrap())
        .unwrap()
}
