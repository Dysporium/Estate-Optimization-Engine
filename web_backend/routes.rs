use axum::extract::Path;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use std::fs;
use std::path::{Component, PathBuf};

const WEB_DIST_DIR: &str = "web/dist";
const WEB_DIST_INDEX_FILE: &str = "web/dist/index.html";

pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/web", get(web_entry))
        .route("/web/", get(web_entry))
        .route("/web/{*path}", get(web_asset_or_spa))
}

async fn web_entry() -> Response {
    serve_index_or_fallback()
}

async fn web_asset_or_spa(Path(path): Path<String>) -> Response {
    let normalized = path.trim_start_matches('/').trim();
    if normalized.is_empty() {
        return serve_index_or_fallback();
    }

    let Some(candidate) = safe_join_dist(normalized) else {
        return (StatusCode::BAD_REQUEST, "Invalid path").into_response();
    };

    if candidate.is_file() {
        return serve_file(&candidate);
    }

    // SPA deep-link fallback (for client-side routes)
    serve_index_or_fallback()
}

fn serve_index_or_fallback() -> Response {
    match fs::read_to_string(WEB_DIST_INDEX_FILE) {
        Ok(index_html) => Html(index_html).into_response(),
        Err(_) => Html(FRONTEND_NOT_BUILT_HTML).into_response(),
    }
}

fn serve_file(path: &PathBuf) -> Response {
    match fs::read(path) {
        Ok(bytes) => {
            let content_type = guess_content_type(path);
            ([(header::CONTENT_TYPE, content_type)], bytes).into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND, "Not found").into_response(),
    }
}

fn safe_join_dist(relative_path: &str) -> Option<PathBuf> {
    let mut full_path = PathBuf::from(WEB_DIST_DIR);
    let candidate = PathBuf::from(relative_path);

    for component in candidate.components() {
        match component {
            Component::Normal(segment) => full_path.push(segment),
            Component::CurDir => {}
            _ => return None,
        }
    }

    Some(full_path)
}

fn guess_content_type(path: &PathBuf) -> &'static str {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("js") => "application/javascript; charset=utf-8",
        Some("mjs") => "application/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("html") => "text/html; charset=utf-8",
        Some("json") => "application/json; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        Some("map") => "application/json; charset=utf-8",
        _ => "application/octet-stream",
    }
}

const FRONTEND_NOT_BUILT_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Estate Optimization Engine Frontend Build Missing</title>
  <style>
    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: linear-gradient(140deg, #f1f5f9 0%, #fff7ed 100%);
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      color: #0f172a;
    }
    main {
      width: min(780px, 92vw);
      background: #fff;
      border-radius: 14px;
      padding: 24px;
      box-shadow: 0 20px 45px rgba(15, 23, 42, 0.14);
    }
    h1 {
      margin-top: 0;
      margin-bottom: 10px;
    }
    pre {
      margin: 14px 0 0;
      background: #111827;
      color: #e5e7eb;
      border-radius: 10px;
      padding: 12px;
      overflow: auto;
    }
  </style>
</head>
<body>
  <main>
    <h1>Estate Optimization Engine Frontend Build Not Found</h1>
    <p>Build the React app in <code>web/</code> and reload <code>/web</code>.</p>
    <pre>cd web
npm install
npm run build</pre>
  </main>
</body>
</html>
"#;
