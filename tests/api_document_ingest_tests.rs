use crate::api::contracts::{
    ApiErrorCode, ApiEstateAssetInput, ApiEstateScenarioInput, ApiScenarioDocumentFormat,
    ApiScenarioDocumentIngestRequest, ApiScenarioDocumentIngestResponse,
};
use crate::api::handler::{
    calculate_scenario_document_contract, ingest_scenario_document_contract,
};
use crate::api::http::app;
use crate::core::domain::models::EstateScenarioInput;
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

fn valid_scenario() -> ApiEstateScenarioInput {
    let mut input = ApiEstateScenarioInput::from(EstateScenarioInput::default());
    input.assets = vec![ApiEstateAssetInput {
        name: "Liquidity Portfolio".to_string(),
        market_value_amount: 5_000_000.0,
        base_cost_amount: 3_000_000.0,
        is_liquid: true,
        situs_in_jurisdiction: true,
        included_in_estate_duty: true,
        included_in_cgt_deemed_disposal: true,
        bequeathed_to_surviving_spouse: false,
        bequeathed_to_pbo: false,
        qualifies_primary_residence_exclusion: false,
    }];
    input
}

#[test]
fn ingest_document_contract_accepts_single_json_scenario() {
    let scenario = valid_scenario();
    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Json,
        document_content: serde_json::to_string(&scenario).expect("Failed to serialize scenario"),
        document_content_base64: None,
    };

    let response = ingest_scenario_document_contract(request)
        .expect("Expected JSON scenario document to parse successfully");
    assert_eq!(response.scenarios.len(), 1);
    assert_eq!(response.scenarios[0].assets.len(), 1);
}

#[test]
fn ingest_document_contract_accepts_envelope_with_scenarios() {
    let scenario = valid_scenario();
    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Json,
        document_content: format!(
            "{{\"scenarios\":[{}]}}",
            serde_json::to_string(&scenario).expect("Failed to serialize scenario")
        ),
        document_content_base64: None,
    };

    let response = ingest_scenario_document_contract(request)
        .expect("Expected envelope scenario document to parse successfully");
    assert_eq!(response.scenarios.len(), 1);
}

#[test]
fn ingest_document_contract_rejects_invalid_json() {
    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Json,
        document_content: "{not-valid-json}".to_string(),
        document_content_base64: None,
    };

    let err = ingest_scenario_document_contract(request)
        .expect_err("Expected invalid JSON scenario document to fail");
    assert_eq!(err.code, ApiErrorCode::Validation);
    assert!(err
        .validation_issues
        .iter()
        .any(|issue| issue.field == "document_content"));
}

#[test]
fn calculate_document_contract_returns_result_per_scenario() {
    let scenario = valid_scenario();
    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Json,
        document_content: serde_json::to_string(&vec![scenario])
            .expect("Failed to serialize scenario list"),
        document_content_base64: None,
    };

    let response = calculate_scenario_document_contract(request)
        .expect("Expected document calculate contract to succeed");
    assert_eq!(response.results.len(), 1);
    assert!(response.results[0].combined_tax.total_tax_liability_amount >= 0.0);
}

#[tokio::test]
async fn ingest_endpoint_returns_parsed_scenarios() {
    let scenario = valid_scenario();
    let ingest = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Json,
        document_content: serde_json::to_string(&scenario).expect("Failed to serialize scenario"),
        document_content_base64: None,
    };
    let body = serde_json::to_vec(&ingest).expect("Failed to serialize ingest request");

    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/scenario/ingest")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .expect("Failed to build request"),
        )
        .await
        .expect("Route call failed");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("Failed to read body");
    let parsed: ApiScenarioDocumentIngestResponse =
        serde_json::from_slice(&body).expect("Failed to deserialize ingest response");
    assert_eq!(parsed.scenarios.len(), 1);
}

#[test]
fn ingest_document_contract_accepts_txt_with_embedded_json() {
    let scenario = valid_scenario();
    let document = format!(
        "Client memo\n```json\n{}\n```",
        serde_json::to_string(&scenario).expect("Failed to serialize scenario")
    );

    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Txt,
        document_content: document,
        document_content_base64: None,
    };

    let response = ingest_scenario_document_contract(request)
        .expect("Expected TXT scenario document to parse successfully");
    assert_eq!(response.scenarios.len(), 1);
}

#[test]
fn ingest_document_contract_accepts_csv_with_scenario_json_column() {
    let scenario = valid_scenario();
    let scenario_json =
        serde_json::to_string(&scenario).expect("Failed to serialize scenario for CSV");
    let csv_document = format!("scenario_json\n\"{}\"\n", scenario_json.replace('"', "\"\""));

    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Csv,
        document_content: csv_document,
        document_content_base64: None,
    };

    let response = ingest_scenario_document_contract(request)
        .expect("Expected CSV scenario_json document to parse successfully");
    assert_eq!(response.scenarios.len(), 1);
}

#[test]
fn ingest_document_contract_accepts_flat_csv_row_with_asset_columns() {
    let csv_document = "jurisdiction,tax_year,taxpayer_class,residency_status,marginal_income_tax_rate,asset_name,asset_market_value_amount,asset_base_cost_amount,asset_is_liquid,asset_situs_in_jurisdiction,asset_included_in_estate_duty,asset_included_in_cgt_deemed_disposal,asset_bequeathed_to_surviving_spouse,asset_bequeathed_to_pbo,asset_qualifies_primary_residence_exclusion\nSouthAfrica,2026,NaturalPerson,Resident,0.45,Liquidity Portfolio,5000000,3000000,true,true,true,true,false,false,false\n".to_string();

    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Csv,
        document_content: csv_document,
        document_content_base64: None,
    };

    let response = ingest_scenario_document_contract(request)
        .expect("Expected flat CSV scenario document to parse successfully");
    assert_eq!(response.scenarios.len(), 1);
    assert_eq!(response.scenarios[0].assets.len(), 1);
}

#[test]
fn ingest_document_contract_rejects_pdf_without_base64_payload() {
    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Pdf,
        document_content: "".to_string(),
        document_content_base64: None,
    };

    let err = ingest_scenario_document_contract(request)
        .expect_err("Expected PDF ingest without base64 payload to fail");
    assert_eq!(err.code, ApiErrorCode::Validation);
    assert!(err
        .validation_issues
        .iter()
        .any(|issue| issue.field == "document_content_base64"));
}
