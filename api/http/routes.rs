use crate::api::contracts::{
    ApiErrorCode, ApiErrorResponse, ApiEstateDocumentAnalysisRequest,
    ApiEstateDocumentAnalysisResponse, ApiEstateDocumentChecklistItem, ApiEstateDocumentDetection,
    ApiEstateDocumentInput, ApiEstateDocumentRequirementStatus, ApiEstateDocumentType,
    ApiEstateScenarioInput, ApiHealthResponse, ApiJurisdiction,
    ApiJurisdictionTaxRuleRegistryResponse, ApiOptimizedScenario,
    ApiScenarioDocumentCalculateResponse, ApiScenarioDocumentFormat,
    ApiScenarioDocumentIngestRequest, ApiScenarioDocumentIngestResponse, ApiScenarioResult,
    ApiTaxRuleRegistryEntry, ApiValidationIssue, ApiVersionedJurisdictionTaxRuleSet,
};
use crate::api::handler::{
    analyze_estate_documents_contract, calculate_scenario_document_contract,
    calculate_single_scenario_contract, get_jurisdiction_tax_rule_registry_contract,
    ingest_scenario_document_contract, list_supported_jurisdictions_contract,
    list_tax_rule_registry_entries_contract, optimize_candidate_scenarios_contract,
    resolve_latest_tax_rules_contract, resolve_tax_rules_for_year_contract,
};
use crate::api::http::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

type HttpError = (StatusCode, Json<ApiErrorResponse>);
type HttpResult<T> = Result<Json<T>, HttpError>;

#[derive(OpenApi)]
#[openapi(
    paths(
        health,
        health_db,
        list_jurisdictions,
        list_registry_entries,
        get_registry_for_jurisdiction,
        resolve_latest_rules,
        resolve_rules_for_year,
        calculate_scenario,
        optimize_scenarios,
        ingest_scenario_document,
        calculate_scenario_document,
        analyze_estate_documents
    ),
    components(
        schemas(
            ApiErrorCode,
            ApiValidationIssue,
            ApiErrorResponse,
            ApiHealthResponse,
            ApiJurisdiction,
            ApiTaxRuleRegistryEntry,
            ApiJurisdictionTaxRuleRegistryResponse,
            ApiVersionedJurisdictionTaxRuleSet,
            ApiEstateScenarioInput,
            ApiScenarioResult,
            ApiOptimizedScenario,
            ApiScenarioDocumentFormat,
            ApiScenarioDocumentIngestRequest,
            ApiScenarioDocumentIngestResponse,
            ApiScenarioDocumentCalculateResponse,
            ApiEstateDocumentType,
            ApiEstateDocumentRequirementStatus,
            ApiEstateDocumentInput,
            ApiEstateDocumentAnalysisRequest,
            ApiEstateDocumentDetection,
            ApiEstateDocumentChecklistItem,
            ApiEstateDocumentAnalysisResponse
        )
    ),
    tags(
        (name = "health", description = "Health and readiness endpoints"),
        (name = "rules", description = "Tax-rule discovery and selection endpoints"),
        (name = "scenario", description = "Scenario calculation and optimization endpoints"),
        (name = "estate-documents", description = "Estate legal/tax document intake and checklist analysis")
    )
)]
struct ApiDoc;

pub fn router(state: AppState) -> Router {
    Router::new()
        .merge(crate::web::router())
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/health", get(health))
        .route("/health/db", get(health_db))
        .route("/v1/jurisdictions", get(list_jurisdictions))
        .route("/v1/rules/registry", get(list_registry_entries))
        .route(
            "/v1/rules/registry/{jurisdiction}",
            get(get_registry_for_jurisdiction),
        )
        .route("/v1/rules/latest/{jurisdiction}", get(resolve_latest_rules))
        .route(
            "/v1/rules/{jurisdiction}/{tax_year}",
            get(resolve_rules_for_year),
        )
        .route("/v1/scenario/calculate", post(calculate_scenario))
        .route("/v1/scenario/optimize", post(optimize_scenarios))
        .route("/v1/scenario/ingest", post(ingest_scenario_document))
        .route(
            "/v1/scenario/document/calculate",
            post(calculate_scenario_document),
        )
        .route(
            "/v1/estate/documents/analyze",
            post(analyze_estate_documents),
        )
        .with_state(state)
}

#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses(
        (status = 200, description = "API is alive", body = ApiHealthResponse)
    )
)]
async fn health() -> Json<ApiHealthResponse> {
    Json(ApiHealthResponse {
        status: "ok".to_string(),
    })
}

#[utoipa::path(
    get,
    path = "/health/db",
    tag = "health",
    responses(
        (status = 200, description = "Database connection is healthy", body = ApiHealthResponse),
        (status = 503, description = "Database is unavailable or not configured", body = ApiErrorResponse)
    )
)]
async fn health_db(State(state): State<AppState>) -> HttpResult<ApiHealthResponse> {
    let Some(pool) = state.db_pool else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiErrorResponse {
                code: ApiErrorCode::Computation,
                message: "Database pool is not configured".to_string(),
                validation_issues: Vec::new(),
            }),
        ));
    };

    sqlx::query_scalar::<_, i64>("SELECT 1")
        .fetch_one(&pool)
        .await
        .map_err(|err| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiErrorResponse {
                    code: ApiErrorCode::Computation,
                    message: format!("Database health check failed: {err}"),
                    validation_issues: Vec::new(),
                }),
            )
        })?;

    Ok(Json(ApiHealthResponse {
        status: "ok".to_string(),
    }))
}

#[utoipa::path(
    get,
    path = "/v1/jurisdictions",
    tag = "rules",
    responses(
        (status = 200, description = "Supported jurisdictions", body = [ApiJurisdiction])
    )
)]
async fn list_jurisdictions() -> Json<Vec<ApiJurisdiction>> {
    Json(list_supported_jurisdictions_contract())
}

#[utoipa::path(
    get,
    path = "/v1/rules/registry",
    tag = "rules",
    responses(
        (status = 200, description = "All registered rule versions", body = [ApiTaxRuleRegistryEntry])
    )
)]
async fn list_registry_entries() -> Json<Vec<ApiTaxRuleRegistryEntry>> {
    Json(list_tax_rule_registry_entries_contract())
}

#[utoipa::path(
    get,
    path = "/v1/rules/registry/{jurisdiction}",
    tag = "rules",
    params(
        ("jurisdiction" = String, Path, description = "Jurisdiction path token")
    ),
    responses(
        (status = 200, description = "Rule registry summary for jurisdiction", body = ApiJurisdictionTaxRuleRegistryResponse),
        (status = 400, description = "Invalid jurisdiction", body = ApiErrorResponse),
        (status = 404, description = "No registry data for jurisdiction", body = ApiErrorResponse)
    )
)]
async fn get_registry_for_jurisdiction(
    Path(jurisdiction): Path<String>,
) -> HttpResult<ApiJurisdictionTaxRuleRegistryResponse> {
    let jurisdiction = parse_jurisdiction(&jurisdiction).map_err(api_error_to_http)?;

    match get_jurisdiction_tax_rule_registry_contract(jurisdiction) {
        Some(response) => Ok(Json(response)),
        None => Err(not_found_response(
            "No rule registry found for requested jurisdiction",
        )),
    }
}

#[utoipa::path(
    get,
    path = "/v1/rules/latest/{jurisdiction}",
    tag = "rules",
    params(
        ("jurisdiction" = String, Path, description = "Jurisdiction path token")
    ),
    responses(
        (status = 200, description = "Latest rule version for jurisdiction", body = ApiVersionedJurisdictionTaxRuleSet),
        (status = 400, description = "Invalid jurisdiction", body = ApiErrorResponse)
    )
)]
async fn resolve_latest_rules(
    Path(jurisdiction): Path<String>,
) -> HttpResult<ApiVersionedJurisdictionTaxRuleSet> {
    let jurisdiction = parse_jurisdiction(&jurisdiction).map_err(api_error_to_http)?;
    Ok(Json(resolve_latest_tax_rules_contract(jurisdiction)))
}

#[utoipa::path(
    get,
    path = "/v1/rules/{jurisdiction}/{tax_year}",
    tag = "rules",
    params(
        ("jurisdiction" = String, Path, description = "Jurisdiction path token"),
        ("tax_year" = u16, Path, description = "Tax year to resolve")
    ),
    responses(
        (status = 200, description = "Rule version selected for tax year", body = ApiVersionedJurisdictionTaxRuleSet),
        (status = 400, description = "Invalid jurisdiction", body = ApiErrorResponse),
        (status = 422, description = "No rules found for jurisdiction/tax_year", body = ApiErrorResponse)
    )
)]
async fn resolve_rules_for_year(
    Path((jurisdiction, tax_year)): Path<(String, u16)>,
) -> HttpResult<ApiVersionedJurisdictionTaxRuleSet> {
    let jurisdiction = parse_jurisdiction(&jurisdiction).map_err(api_error_to_http)?;
    resolve_tax_rules_for_year_contract(jurisdiction, tax_year)
        .map(Json)
        .map_err(api_error_to_http)
}

#[utoipa::path(
    post,
    path = "/v1/scenario/calculate",
    tag = "scenario",
    request_body = ApiEstateScenarioInput,
    responses(
        (status = 200, description = "Calculated scenario output", body = ApiScenarioResult),
        (status = 400, description = "Input validation failed", body = ApiErrorResponse),
        (status = 422, description = "Rules could not be selected", body = ApiErrorResponse),
        (status = 500, description = "Computation failure", body = ApiErrorResponse)
    )
)]
async fn calculate_scenario(
    Json(input): Json<ApiEstateScenarioInput>,
) -> HttpResult<ApiScenarioResult> {
    calculate_single_scenario_contract(input)
        .map(Json)
        .map_err(api_error_to_http)
}

#[utoipa::path(
    post,
    path = "/v1/scenario/optimize",
    tag = "scenario",
    request_body = [ApiEstateScenarioInput],
    responses(
        (status = 200, description = "Best-scoring scenario or null for empty list", body = Option<ApiOptimizedScenario>),
        (status = 400, description = "Input validation failed", body = ApiErrorResponse),
        (status = 422, description = "Rules could not be selected", body = ApiErrorResponse),
        (status = 500, description = "Computation failure", body = ApiErrorResponse)
    )
)]
async fn optimize_scenarios(
    Json(candidates): Json<Vec<ApiEstateScenarioInput>>,
) -> HttpResult<Option<ApiOptimizedScenario>> {
    optimize_candidate_scenarios_contract(candidates)
        .map(Json)
        .map_err(api_error_to_http)
}

#[utoipa::path(
    post,
    path = "/v1/scenario/ingest",
    tag = "scenario",
    request_body = ApiScenarioDocumentIngestRequest,
    responses(
        (status = 200, description = "Parsed and validated scenarios from document", body = ApiScenarioDocumentIngestResponse),
        (status = 400, description = "Document format or validation failure", body = ApiErrorResponse)
    )
)]
async fn ingest_scenario_document(
    Json(request): Json<ApiScenarioDocumentIngestRequest>,
) -> HttpResult<ApiScenarioDocumentIngestResponse> {
    ingest_scenario_document_contract(request)
        .map(Json)
        .map_err(api_error_to_http)
}

#[utoipa::path(
    post,
    path = "/v1/scenario/document/calculate",
    tag = "scenario",
    request_body = ApiScenarioDocumentIngestRequest,
    responses(
        (status = 200, description = "Parsed, validated, and calculated scenarios from document", body = ApiScenarioDocumentCalculateResponse),
        (status = 400, description = "Document format or validation failure", body = ApiErrorResponse),
        (status = 422, description = "Rules could not be selected", body = ApiErrorResponse),
        (status = 500, description = "Computation failure", body = ApiErrorResponse)
    )
)]
async fn calculate_scenario_document(
    Json(request): Json<ApiScenarioDocumentIngestRequest>,
) -> HttpResult<ApiScenarioDocumentCalculateResponse> {
    calculate_scenario_document_contract(request)
        .map(Json)
        .map_err(api_error_to_http)
}

#[utoipa::path(
    post,
    path = "/v1/estate/documents/analyze",
    tag = "estate-documents",
    request_body = ApiEstateDocumentAnalysisRequest,
    responses(
        (status = 200, description = "Estate document checklist and detection analysis", body = ApiEstateDocumentAnalysisResponse),
        (status = 400, description = "Document parsing or validation failure", body = ApiErrorResponse)
    )
)]
async fn analyze_estate_documents(
    Json(request): Json<ApiEstateDocumentAnalysisRequest>,
) -> HttpResult<ApiEstateDocumentAnalysisResponse> {
    analyze_estate_documents_contract(request)
        .map(Json)
        .map_err(api_error_to_http)
}

fn parse_jurisdiction(raw: &str) -> Result<ApiJurisdiction, ApiErrorResponse> {
    match raw.to_ascii_lowercase().as_str() {
        "south-africa" | "south_africa" | "southafrica" | "za" => Ok(ApiJurisdiction::SouthAfrica),
        "us-new-york"
        | "us_new_york"
        | "usnewyork"
        | "new-york"
        | "new_york"
        | "newyork"
        | "newyoork"
        | "ny" => Ok(ApiJurisdiction::UsNewYork),
        "us-texas" | "us_texas" | "ustexas" | "texas" | "tx" => Ok(ApiJurisdiction::UsTexas),
        "us-california"
        | "us_california"
        | "uscalifornia"
        | "california"
        | "calfornia"
        | "ca" => Ok(ApiJurisdiction::UsCalifornia),
        "us-florida" | "us_florida" | "usflorida" | "florida" | "fl" => {
            Ok(ApiJurisdiction::UsFlorida)
        }
        "us-minnesota" | "us_minnesota" | "usminnesota" | "minnesota" | "mn" => {
            Ok(ApiJurisdiction::UsMinnesota)
        }
        _ => Err(ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: format!("Unsupported jurisdiction path value '{raw}'"),
            validation_issues: vec![ApiValidationIssue {
                field: "jurisdiction".to_string(),
                message: "Use one of: south-africa|za, us-new-york|new-york|newyoork|ny, us-texas|texas|tx, us-california|california|calfornia|ca, us-florida|florida|fl, us-minnesota|minnesota|mn".to_string(),
            }],
        }),
    }
}

fn api_error_to_http(error: ApiErrorResponse) -> HttpError {
    let status = match error.code {
        ApiErrorCode::Validation => StatusCode::BAD_REQUEST,
        ApiErrorCode::RuleSelection => StatusCode::UNPROCESSABLE_ENTITY,
        ApiErrorCode::Computation => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(error))
}

fn not_found_response(message: &str) -> HttpError {
    (
        StatusCode::NOT_FOUND,
        Json(ApiErrorResponse {
            code: ApiErrorCode::Computation,
            message: message.to_string(),
            validation_issues: Vec::new(),
        }),
    )
}
