# Estate Optimization Engine

Jurisdiction-aware estate-planning engine with:
- Combined Tax Liability calculation
- Liquidity Gap output
- Tax-rule version registry
- HTTP API (Cargo + Axum)
- Web upload interface for scenario documents

Current jurisdiction baselines:
- South Africa
- United States (state baselines: New York, Texas, California, Florida, Minnesota)

## Stack
- Rust 2021
- Cargo
- Axum + Tokio

## Run
Prerequisites:
- Rust toolchain (`cargo --version`)

Start API server:
```bash
cargo run
```

Custom bind address:
```bash
ENGINE_BIND=0.0.0.0:8080 cargo run
```

Open web interface:
- `http://127.0.0.1:8080/web`

Frontend (React + TypeScript in `web/`):
```bash
cd web
npm install
npm run dev
```

Build frontend for Rust-served `/web`:
```bash
cd web
npm run build
```

Compile checks:
```bash
cargo check
cargo check --all-targets
```

## Scenario Document Processing
Document endpoints:
- `POST /v1/scenario/ingest` to parse and validate document content.
- `POST /v1/scenario/document/calculate` to parse, validate, and calculate.

Supported JSON shapes:
- single scenario object
- array of scenario objects
- object with `scenarios` array

## Verification
Tax baselines are maintained in:
- `jurisdictions/south_africa/mod.rs`
- `jurisdictions/us/mod.rs`
