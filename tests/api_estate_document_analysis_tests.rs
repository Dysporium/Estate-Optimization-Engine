use crate::api::contracts::{
    ApiErrorCode, ApiEstateDocumentAnalysisRequest, ApiEstateDocumentAnalysisResponse,
    ApiEstateDocumentInput, ApiEstateDocumentRequirementStatus, ApiEstateDocumentType,
    ApiScenarioDocumentFormat,
};
use crate::api::handler::analyze_estate_documents_contract;
use crate::api::http::app;
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

#[test]
fn estate_document_analysis_contract_reports_missing_core_documents() {
    let request = ApiEstateDocumentAnalysisRequest {
        documents: vec![
            ApiEstateDocumentInput {
                declared_document_type: None,
                document_name: Some("J294 - Death Notice.pdf".to_string()),
                format: ApiScenarioDocumentFormat::Txt,
                document_content: "Department of Justice - Death Notice J294".to_string(),
                document_content_base64: None,
            },
            ApiEstateDocumentInput {
                declared_document_type: None,
                document_name: Some("Will and Codicil.docx".to_string()),
                format: ApiScenarioDocumentFormat::Txt,
                document_content: "Last Will and Testament with codicil".to_string(),
                document_content_base64: None,
            },
        ],
    };

    let response =
        analyze_estate_documents_contract(request).expect("Expected estate analysis to succeed");

    assert!(!response.detections.is_empty());
    assert!(response.detections.iter().any(|entry| entry
        .detected_document_types
        .contains(&ApiEstateDocumentType::DeathNoticeJ294)));

    let death_notice_requirement = response
        .checklist
        .iter()
        .find(|item| item.requirement_id == "sa-legal-death-notice-j294")
        .expect("Expected death notice requirement entry");
    assert_eq!(
        death_notice_requirement.status,
        ApiEstateDocumentRequirementStatus::Satisfied
    );

    let inventory_requirement = response
        .checklist
        .iter()
        .find(|item| item.requirement_id == "sa-legal-inventory-j243")
        .expect("Expected inventory requirement entry");
    assert_eq!(
        inventory_requirement.status,
        ApiEstateDocumentRequirementStatus::Missing
    );
    assert!(response
        .missing_required_document_types
        .contains(&ApiEstateDocumentType::InventoryJ243));
}

#[test]
fn estate_document_analysis_missing_types_do_not_include_detected_j294() {
    let request = ApiEstateDocumentAnalysisRequest {
        documents: vec![ApiEstateDocumentInput {
            declared_document_type: None,
            document_name: Some("J294 - Death Notice.pdf".to_string()),
            format: ApiScenarioDocumentFormat::Txt,
            document_content: "Department of Justice - Death Notice J294".to_string(),
            document_content_base64: None,
        }],
    };

    let response =
        analyze_estate_documents_contract(request).expect("Expected estate analysis to succeed");

    assert!(response.detections.iter().any(|entry| entry
        .detected_document_types
        .contains(&ApiEstateDocumentType::DeathNoticeJ294)));

    assert!(!response
        .missing_required_document_types
        .contains(&ApiEstateDocumentType::DeathNoticeJ294));
}

#[test]
fn estate_document_analysis_contract_rejects_empty_batch() {
    let request = ApiEstateDocumentAnalysisRequest {
        documents: Vec::new(),
    };

    let err = analyze_estate_documents_contract(request).expect_err("Expected empty batch to fail");
    assert_eq!(err.code, ApiErrorCode::Validation);
    assert!(err
        .validation_issues
        .iter()
        .any(|issue| issue.field == "documents"));
}

#[tokio::test]
async fn estate_document_analysis_endpoint_returns_checklist() {
    let request = ApiEstateDocumentAnalysisRequest {
        documents: vec![ApiEstateDocumentInput {
            declared_document_type: Some(ApiEstateDocumentType::EstateDutyReturnRev267),
            document_name: Some("REV267 estate duty return.txt".to_string()),
            format: ApiScenarioDocumentFormat::Txt,
            document_content: "SARS Estate Duty return REV267 for deceased estate".to_string(),
            document_content_base64: None,
        }],
    };
    let body = serde_json::to_vec(&request).expect("Failed to serialize request");

    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/estate/documents/analyze")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .expect("Failed to build request"),
        )
        .await
        .expect("Route call failed");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("Failed to read response body");
    let parsed: ApiEstateDocumentAnalysisResponse =
        serde_json::from_slice(&body).expect("Failed to deserialize analysis response");
    assert!(!parsed.checklist.is_empty());
    assert!((0.0..=1.0).contains(&parsed.readiness_score));
}
