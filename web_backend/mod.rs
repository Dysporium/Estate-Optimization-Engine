mod routes;

use axum::Router;

pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    routes::router()
}
