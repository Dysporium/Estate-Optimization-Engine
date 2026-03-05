#[path = "jurisdictions/mod.rs"]
pub mod jurisdictions;

#[path = "core/mod.rs"]
pub mod core;

#[path = "api/mod.rs"]
pub mod api;

#[path = "simulation/mod.rs"]
pub mod simulation;

#[path = "web_backend/mod.rs"]
pub mod web;

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;

fn resolve_bind_addr() -> String {
    if let Ok(bind) = std::env::var("ENGINE_BIND") {
        if !bind.trim().is_empty() {
            return bind;
        }
    }

    if let Ok(port) = std::env::var("PORT") {
        let trimmed = port.trim();
        if !trimmed.is_empty() {
            return format!("0.0.0.0:{trimmed}");
        }
    }

    "127.0.0.1:8080".to_string()
}

fn resolve_database_url() -> Result<String, std::io::Error> {
    match std::env::var("DATABASE_URL") {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "DATABASE_URL is required (PostgreSQL connection string)",
        )),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = dotenvy::dotenv();
    let bind_addr = resolve_bind_addr();
    let database_url = resolve_database_url()?;
    api::run(&bind_addr, &database_url).await
}
