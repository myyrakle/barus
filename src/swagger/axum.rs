use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Router, response::Response};

use super::favicon::{FAVICON_16, FAVICON_32};
use super::{html, swagger_json, swagger_ui_bundle, swagger_ui_css};

pub fn router() -> Router {
    let swagger_router = Router::new()
        .route("/", get(get_docs))
        .route("/favicon-32x32.png", get(get_favicon32))
        .route("/favicon-16x16.png", get(get_favicon16))
        .route("/swagger.json", get(get_swagger_json))
        .route("/swagger-ui-bundle.js", get(get_swagger_ui_bundle))
        .route("/swagger-ui.css", get(get_swagger_ui_css));

    swagger_router
}

async fn get_docs(_: axum::http::Request<axum::body::Body>) -> impl IntoResponse {
    Response::builder()
        .status(200)
        .header("Content-Type", "text/html; charset=utf-8")
        .body(html::DOCS_INDEX_HTML.to_string())
        .unwrap()
}

async fn get_favicon32() -> impl IntoResponse {
    let base64 = FAVICON_32.to_string();

    Response::builder()
        .status(200)
        .header("Content-Type", "image/png")
        .body(base64.to_string())
        .unwrap()
}

async fn get_favicon16() -> impl IntoResponse {
    let base64 = FAVICON_16.to_string();

    Response::builder()
        .status(200)
        .header("Content-Type", "image/png")
        .body(base64.to_string())
        .unwrap()
}

async fn get_swagger_json() -> impl IntoResponse {
    let swagger_json = swagger_json::SWAGGER_JSON.to_string();
    Response::builder()
        .status(200)
        .header("Content-Type", "application/json")
        .body(swagger_json)
        .unwrap()
}

async fn get_swagger_ui_bundle() -> impl IntoResponse {
    Response::builder()
        .status(200)
        .header("Content-Type", "application/javascript; charset=utf-8")
        .header("access-control-allow-origin", "*")
        .body(swagger_ui_bundle::SWAGGER_UI_BUNDLE_JS.to_string())
        .unwrap()
}

async fn get_swagger_ui_css() -> impl IntoResponse {
    Response::builder()
        .status(200)
        .header("Content-Type", "text/css; charset=utf-8")
        .header("access-control-allow-origin", "*")
        .body(swagger_ui_css::SWAGGER_UI_CSS.to_string())
        .unwrap()
}
