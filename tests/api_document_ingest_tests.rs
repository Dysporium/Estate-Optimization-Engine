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
fn ingest_document_contract_accepts_txt_with_structured_key_value_fields() {
    let document = r#"
Jurisdiction: South Africa
Tax Year: 2026
Taxpayer Class: Natural Person
Residency Status: Resident
Marginal Income Tax Rate: 45%
Asset Name: Liquidity Portfolio
Asset Market Value: R 5,000,000
Asset Base Cost: 3,000,000
Asset Is Liquid: yes
Asset Situs In Jurisdiction: true
Asset Included In Estate Duty: true
Asset Included In CGT Deemed Disposal: true
Asset Bequeathed To Surviving Spouse: false
Asset Bequeathed To PBO: false
Asset Qualifies Primary Residence Exclusion: false
"#
    .to_string();

    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Txt,
        document_content: document,
        document_content_base64: None,
    };

    let response = ingest_scenario_document_contract(request)
        .expect("Expected structured TXT scenario document to parse successfully");
    assert_eq!(response.scenarios.len(), 1);
    assert_eq!(response.scenarios[0].tax_year, 2026);
    assert_eq!(response.scenarios[0].assets.len(), 1);
    assert_eq!(response.scenarios[0].assets[0].name, "Liquidity Portfolio");
    assert_eq!(
        response.scenarios[0].assets[0].market_value_amount,
        5_000_000.0
    );
}

#[test]
fn ingest_document_contract_accepts_txt_with_indexed_asset_fields() {
    let document = r#"
jurisdiction: za
tax_year: 2026
taxpayer_class: NaturalPerson
residency_status: Resident
marginal_income_tax_rate: 0.45
assets_1_name: Cash
assets_1_market_value_amount: 1000000
assets_1_base_cost_amount: 900000
assets_1_is_liquid: true
assets_1_situs_in_jurisdiction: true
assets_1_included_in_estate_duty: true
assets_1_included_in_cgt_deemed_disposal: true
assets_1_bequeathed_to_surviving_spouse: false
assets_1_bequeathed_to_pbo: false
assets_1_qualifies_primary_residence_exclusion: false
assets_2_name: Property
assets_2_market_value_amount: 3000000
assets_2_base_cost_amount: 1000000
assets_2_is_liquid: false
assets_2_situs_in_jurisdiction: true
assets_2_included_in_estate_duty: true
assets_2_included_in_cgt_deemed_disposal: true
assets_2_bequeathed_to_surviving_spouse: false
assets_2_bequeathed_to_pbo: false
assets_2_qualifies_primary_residence_exclusion: false
"#
    .to_string();

    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Txt,
        document_content: document,
        document_content_base64: None,
    };

    let response = ingest_scenario_document_contract(request)
        .expect("Expected indexed asset TXT scenario document to parse successfully");
    assert_eq!(response.scenarios.len(), 1);
    assert_eq!(response.scenarios[0].assets.len(), 2);
    assert_eq!(response.scenarios[0].assets[0].name, "Cash");
    assert_eq!(response.scenarios[0].assets[1].name, "Property");
}

#[test]
fn ingest_document_contract_accepts_narrative_estate_text_without_json() {
    let document = r#"
Estate Plan Summary
Jurisdiction South Africa
Tax year 2026
Marginal tax rate 45%
Debts and loans estimated at R 150,000
Funeral costs estimated at R 60,000
Executor fee rate 3.5%
Cash reserve amount R 75,000
Primary residence market value R 4,500,000
"#
    .to_string();

    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Txt,
        document_content: document,
        document_content_base64: None,
    };

    let response = ingest_scenario_document_contract(request)
        .expect("Expected narrative TXT estate document to parse successfully");
    assert_eq!(response.scenarios.len(), 1);
    assert_eq!(response.scenarios[0].tax_year, 2026);
    assert!(!response.scenarios[0].assets.is_empty());
    assert!(response.scenarios[0].assets[0].market_value_amount > 0.0);
}

#[test]
fn ingest_document_contract_accepts_bank_style_balance_text_without_json() {
    let document = r#"
Capitec Bank
Proof of Account Details
Savings account available balance R 1,245,300.50
"#
    .to_string();

    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Txt,
        document_content: document,
        document_content_base64: None,
    };

    let response = ingest_scenario_document_contract(request)
        .expect("Expected bank-style TXT document to parse successfully");
    assert_eq!(response.scenarios.len(), 1);
    assert_eq!(
        response.scenarios[0].jurisdiction,
        crate::api::contracts::ApiJurisdiction::SouthAfrica
    );
    assert!(!response.scenarios[0].assets.is_empty());
    assert!(response.scenarios[0].assets[0].market_value_amount >= 1_245_300.0);
    assert!(response.scenarios[0].assets[0].is_liquid);
}

#[test]
fn ingest_document_contract_accepts_bank_text_with_balance_split_across_lines() {
    let document = r#"
Capitec Bank
Proof of Account Details
Savings account available balance
R 1,245,300.50
"#
    .to_string();

    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Txt,
        document_content: document,
        document_content_base64: None,
    };

    let response = ingest_scenario_document_contract(request)
        .expect("Expected split-line balance TXT document to parse successfully");
    assert_eq!(response.scenarios.len(), 1);
    assert!(!response.scenarios[0].assets.is_empty());
    assert!(response.scenarios[0].assets[0].market_value_amount >= 1_245_300.0);
}

#[test]
fn ingest_document_contract_ignores_account_number_and_disclaimer_for_asset_parsing() {
    let document = r#"
Capitec Bank
Proof of Account Details
Account number 860102043
Available balance
R 1,245,300.50
Capitec Bank Limited shall have no liability whether in contract, delict (including without limitation negligence) or otherwise to the above accountholder or any third party in relation to the account details contained herein.
"#
    .to_string();

    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Txt,
        document_content: document,
        document_content_base64: None,
    };

    let response = ingest_scenario_document_contract(request)
        .expect("Expected parser to use balance amount, not account number/disclaimer");
    assert_eq!(response.scenarios.len(), 1);
    assert!(!response.scenarios[0].assets.is_empty());
    let first_asset = &response.scenarios[0].assets[0];
    assert!(first_asset.market_value_amount >= 1_245_300.0);
    assert!(first_asset.market_value_amount < 100_000_000.0);
    assert!(!first_asset
        .name
        .to_ascii_lowercase()
        .contains("shall have no liability"));
}

#[test]
fn ingest_document_contract_returns_guidance_for_j294_legal_document() {
    let document = r#"
J294 - Death Notice
Particulars of the Deceased
Surname: Mokoena
Full Names: Thabo Daniel
Identity Number: 800101 5800 089
"#
    .to_string();

    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Txt,
        document_content: document,
        document_content_base64: None,
    };

    let err = ingest_scenario_document_contract(request)
        .expect_err("Expected legal document guidance error for scenario ingest endpoint");
    assert_eq!(err.code, ApiErrorCode::Validation);
    assert!(err
        .message
        .to_ascii_lowercase()
        .contains("legal/tax estate document"));
    assert!(err.validation_issues.iter().any(|issue| {
        issue.field == "document_content" && issue.message.contains("/v1/estate/documents/analyze")
    }));
}

#[test]
fn ingest_document_contract_prefers_property_market_value_over_deed_metadata() {
    let document = r#"
REPUBLIC OF SOUTH AFRICA
DEEDS REGISTRY - TITLE DEED & PROPERTY VALUATION REPORT
Deed No: T 48291/2019
Issued: 14 March 2019
Deeds Registry: Pretoria

SECTION 1 - PROPERTY INFORMATION
Erf Number
Erf 4827
Township / Extension
Faerie Glen Extension 1

SECTION 3 - VALUATION
Market Value
R 2 150 000
Mortgage Bond BN 17623/2019 - Absa Bank Limited - R 1 300 000
20 August 2024 Valuer Mr. Andile Mokoena - Pr. Val. No 4427
SECTION 5 - SUPPORTING INFORMATION
VALUATION ADJUSTMENTS & COMMENTARY
Land Value (842 m2 @ R 1 200/m2)
"#
    .to_string();

    let request = ApiScenarioDocumentIngestRequest {
        format: ApiScenarioDocumentFormat::Txt,
        document_content: document,
        document_content_base64: None,
    };

    let response = ingest_scenario_document_contract(request)
        .expect("Expected property valuation TXT document to parse successfully");
    assert_eq!(response.scenarios.len(), 1);
    assert!(!response.scenarios[0].assets.is_empty());

    let assets = &response.scenarios[0].assets;
    assert_eq!(response.scenarios[0].debts_and_loans_amount, 1_300_000.0);
    assert!(assets
        .iter()
        .any(|asset| (asset.market_value_amount - 2_150_000.0).abs() < 0.5));
    assert!(assets
        .iter()
        .all(|asset| !asset.name.to_ascii_lowercase().contains("mortgage bond")));
    assert!(assets
        .iter()
        .all(|asset| (asset.market_value_amount - 48_291.0).abs() > 0.5));
    assert!(assets
        .iter()
        .all(|asset| (asset.market_value_amount - 4_427.0).abs() > 0.5));
    assert!(assets
        .iter()
        .all(|asset| (asset.market_value_amount - 1_200.0).abs() > 0.5));
    assert!(assets
        .iter()
        .all(|asset| (asset.market_value_amount - 5.0).abs() > 0.5));
}

#[test]
fn ingest_document_contract_accepts_csv_with_scenario_json_column() {
    let scenario = valid_scenario();
    let scenario_json =
        serde_json::to_string(&scenario).expect("Failed to serialize scenario for CSV");
    let csv_document = format!(
        "scenario_json\n\"{}\"\n",
        scenario_json.replace('"', "\"\"")
    );

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
