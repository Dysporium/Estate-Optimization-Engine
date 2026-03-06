# Estate Optimization Engine

In short, it does 4 things:

1. **Calculates estate outcomes**  
   It computes tax and liquidity results for a given estate scenario (by jurisdiction/tax year).

2. **Optimizes across scenarios**  
   It compares multiple candidate scenarios and returns the best option based on the model’s objective (lower tax burden / better liquidity position).

3. **Ingests documents into structured inputs**  
   It parses JSON/TXT/PDF/DOCX inputs into engine-ready scenario data when possible.

4. **Analyzes legal/tax estate document completeness**  
   For SA legal/tax packs, it detects document types (e.g., J294, J243, REV267), builds a checklist, flags missing required docs, and gives a readiness score.

It is a **planning/analysis tool**, not a legal filing system by itself.

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
