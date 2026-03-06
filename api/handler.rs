use crate::api::contracts::{
    ApiErrorCode, ApiErrorResponse, ApiEstateDocumentAnalysisRequest,
    ApiEstateDocumentAnalysisResponse, ApiEstateDocumentChecklistItem, ApiEstateDocumentDetection,
    ApiEstateDocumentInput, ApiEstateDocumentRequirementStatus, ApiEstateDocumentType,
    ApiEstateScenarioInput, ApiJurisdiction, ApiJurisdictionTaxRuleRegistryResponse,
    ApiOptimizedScenario, ApiScenarioDocumentCalculateResponse, ApiScenarioDocumentFormat,
    ApiScenarioDocumentIngestRequest, ApiScenarioDocumentIngestResponse, ApiScenarioResult,
    ApiTaxRuleRegistryEntry, ApiValidationIssue, ApiVersionedJurisdictionTaxRuleSet,
    JurisdictionTaxRuleRegistryResponse,
};
use crate::core::domain::models::{EstateScenarioInput, ScenarioResult};
use crate::core::engine::optimizer::{optimize_scenarios, OptimizedScenario};
use crate::core::engine::scenario::calculate_combined_tax_and_liquidity;
use crate::core::errors::EngineError;
use crate::core::rules::tax_rules::{
    latest_tax_rules_for, supported_jurisdictions, supported_tax_year_window, tax_rule_registry,
    tax_rule_registry_for, tax_rules_for, Jurisdiction, TaxRuleRegistryEntry,
    VersionedJurisdictionTaxRuleSet,
};
use crate::core::validation::InputValidationError;
use base64::Engine;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read};
use zip::ZipArchive;

pub fn list_supported_jurisdictions() -> Vec<Jurisdiction> {
    supported_jurisdictions()
}

pub fn list_supported_jurisdictions_contract() -> Vec<ApiJurisdiction> {
    list_supported_jurisdictions()
        .into_iter()
        .map(ApiJurisdiction::from)
        .collect()
}

pub fn list_tax_rule_registry_entries() -> Vec<TaxRuleRegistryEntry> {
    tax_rule_registry()
}

pub fn list_tax_rule_registry_entries_contract() -> Vec<ApiTaxRuleRegistryEntry> {
    list_tax_rule_registry_entries()
        .into_iter()
        .map(ApiTaxRuleRegistryEntry::from)
        .collect()
}

pub fn get_jurisdiction_tax_rule_registry(
    jurisdiction: Jurisdiction,
) -> Option<JurisdictionTaxRuleRegistryResponse> {
    let versions = tax_rule_registry_for(jurisdiction);
    if versions.is_empty() {
        return None;
    }

    let (supported_tax_year_from, supported_tax_year_to) = supported_tax_year_window(jurisdiction)?;
    let latest_version_id = latest_tax_rules_for(jurisdiction).version.version_id;

    Some(JurisdictionTaxRuleRegistryResponse {
        jurisdiction,
        versions,
        supported_tax_year_from,
        supported_tax_year_to,
        latest_version_id,
    })
}

pub fn get_jurisdiction_tax_rule_registry_contract(
    jurisdiction: ApiJurisdiction,
) -> Option<ApiJurisdictionTaxRuleRegistryResponse> {
    get_jurisdiction_tax_rule_registry(jurisdiction.into())
        .map(ApiJurisdictionTaxRuleRegistryResponse::from)
}

pub fn resolve_tax_rules_for_year(
    jurisdiction: Jurisdiction,
    tax_year: u16,
) -> Result<VersionedJurisdictionTaxRuleSet, EngineError> {
    tax_rules_for(jurisdiction, tax_year).map_err(EngineError::from)
}

pub fn resolve_tax_rules_for_year_contract(
    jurisdiction: ApiJurisdiction,
    tax_year: u16,
) -> Result<ApiVersionedJurisdictionTaxRuleSet, ApiErrorResponse> {
    resolve_tax_rules_for_year_api(jurisdiction.into(), tax_year)
        .map(ApiVersionedJurisdictionTaxRuleSet::from)
}

pub fn resolve_latest_tax_rules(jurisdiction: Jurisdiction) -> VersionedJurisdictionTaxRuleSet {
    latest_tax_rules_for(jurisdiction)
}

pub fn resolve_latest_tax_rules_contract(
    jurisdiction: ApiJurisdiction,
) -> ApiVersionedJurisdictionTaxRuleSet {
    resolve_latest_tax_rules(jurisdiction.into()).into()
}

pub fn to_api_error_response(error: EngineError) -> ApiErrorResponse {
    match error {
        EngineError::Validation(validation_error) => ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: validation_error.to_string(),
            validation_issues: validation_error
                .issues
                .into_iter()
                .map(|issue| ApiValidationIssue {
                    field: issue.field,
                    message: issue.message,
                })
                .collect(),
        },
        EngineError::RuleSelection(selection_error) => ApiErrorResponse {
            code: ApiErrorCode::RuleSelection,
            message: selection_error.to_string(),
            validation_issues: Vec::new(),
        },
        EngineError::Computation(message) => ApiErrorResponse {
            code: ApiErrorCode::Computation,
            message,
            validation_issues: Vec::new(),
        },
    }
}

pub fn calculate_single_scenario_api(
    input: &EstateScenarioInput,
) -> Result<ScenarioResult, ApiErrorResponse> {
    calculate_single_scenario(input).map_err(to_api_error_response)
}

pub fn calculate_single_scenario_contract(
    input: ApiEstateScenarioInput,
) -> Result<ApiScenarioResult, ApiErrorResponse> {
    let domain_input: EstateScenarioInput = input.into();
    calculate_single_scenario_api(&domain_input).map(ApiScenarioResult::from)
}

pub fn optimize_candidate_scenarios_api(
    candidates: Vec<EstateScenarioInput>,
) -> Result<Option<OptimizedScenario>, ApiErrorResponse> {
    optimize_candidate_scenarios(candidates).map_err(to_api_error_response)
}

pub fn optimize_candidate_scenarios_contract(
    candidates: Vec<ApiEstateScenarioInput>,
) -> Result<Option<ApiOptimizedScenario>, ApiErrorResponse> {
    let domain_candidates: Vec<EstateScenarioInput> = candidates
        .into_iter()
        .map(EstateScenarioInput::from)
        .collect();

    optimize_candidate_scenarios_api(domain_candidates)
        .map(|candidate| candidate.map(ApiOptimizedScenario::from))
}

#[derive(Debug, Deserialize)]
struct JsonScenarioEnvelope {
    scenarios: Vec<ApiEstateScenarioInput>,
}

enum StructuredTextParseOutcome {
    Parsed(Vec<ApiEstateScenarioInput>),
    NotDetected,
    Invalid(ApiErrorResponse),
}

fn ingest_debug_enabled() -> bool {
    std::env::var("ENGINE_INGEST_DEBUG")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn ingest_debug_log(message: &str) {
    if ingest_debug_enabled() {
        eprintln!("[ingest-debug] {message}");
    }
}

fn preview_for_log(input: &str, max_chars: usize) -> String {
    let mut preview = input.chars().take(max_chars).collect::<String>();
    preview = preview.replace('\n', "\\n");
    preview = preview.replace('\r', "\\r");
    if input.chars().count() > max_chars {
        preview.push_str("...");
    }
    preview
}

pub fn ingest_scenario_document_contract(
    request: ApiScenarioDocumentIngestRequest,
) -> Result<ApiScenarioDocumentIngestResponse, ApiErrorResponse> {
    let scenarios = parse_scenarios_from_document(&request)?;
    validate_scenarios_for_analysis(&scenarios)?;
    Ok(ApiScenarioDocumentIngestResponse { scenarios })
}

pub fn calculate_scenario_document_contract(
    request: ApiScenarioDocumentIngestRequest,
) -> Result<ApiScenarioDocumentCalculateResponse, ApiErrorResponse> {
    let scenarios = parse_scenarios_from_document(&request)?;
    validate_scenarios_for_analysis(&scenarios)?;

    let mut results = Vec::with_capacity(scenarios.len());
    for scenario in &scenarios {
        let domain: EstateScenarioInput = scenario.clone().into();
        let result = calculate_single_scenario_api(&domain)?;
        results.push(ApiScenarioResult::from(result));
    }

    Ok(ApiScenarioDocumentCalculateResponse { scenarios, results })
}

pub fn analyze_estate_documents_contract(
    request: ApiEstateDocumentAnalysisRequest,
) -> Result<ApiEstateDocumentAnalysisResponse, ApiErrorResponse> {
    if request.documents.is_empty() {
        return Err(ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: "No estate documents were provided for analysis".to_string(),
            validation_issues: vec![ApiValidationIssue {
                field: "documents".to_string(),
                message: "Provide at least one estate document".to_string(),
            }],
        });
    }

    let mut detections = Vec::with_capacity(request.documents.len());
    let mut by_document_type: BTreeMap<ApiEstateDocumentType, BTreeSet<usize>> = BTreeMap::new();

    for (index, document) in request.documents.iter().enumerate() {
        let text = extract_text_for_estate_document_analysis(document).map_err(|mut error| {
            for issue in &mut error.validation_issues {
                issue.field = format!("documents[{index}].{}", issue.field);
            }
            error
        })?;

        let mut detected_document_types =
            detect_estate_document_types(document.document_name.as_deref(), &text);
        let mut declared_not_explicitly_detected = false;
        if let Some(declared) = document.declared_document_type {
            if !detected_document_types.contains(&declared) {
                declared_not_explicitly_detected = true;
                detected_document_types.push(declared);
            }
        }
        detected_document_types.sort();
        detected_document_types.dedup();

        for document_type in &detected_document_types {
            by_document_type
                .entry(*document_type)
                .or_default()
                .insert(index);
        }

        let mut warnings = Vec::new();
        if detected_document_types.is_empty() {
            warnings.push(
                "No known SA legal/tax document markers were detected in this document".to_string(),
            );
        }
        if let Some(declared) = document.declared_document_type {
            if declared_not_explicitly_detected {
                warnings.push(format!(
                    "Declared document type {:?} was not explicitly detected in content",
                    declared
                ));
            }
        }

        detections.push(ApiEstateDocumentDetection {
            document_index: index,
            declared_document_type: document.declared_document_type,
            detected_document_types,
            text_length: text.len(),
            text_preview: Some(preview_for_log(&text, 180)),
            warnings,
        });
    }

    let checklist = build_sa_estate_document_checklist(&by_document_type);
    let missing_required_document_types =
        collect_missing_required_document_types(&checklist, &by_document_type);
    let readiness_score = compute_readiness_score(&checklist);

    Ok(ApiEstateDocumentAnalysisResponse {
        detections,
        checklist,
        missing_required_document_types,
        readiness_score,
    })
}

pub fn resolve_tax_rules_for_year_api(
    jurisdiction: Jurisdiction,
    tax_year: u16,
) -> Result<VersionedJurisdictionTaxRuleSet, ApiErrorResponse> {
    resolve_tax_rules_for_year(jurisdiction, tax_year).map_err(to_api_error_response)
}

pub fn calculate_single_scenario(
    input: &EstateScenarioInput,
) -> Result<ScenarioResult, EngineError> {
    input.validate().map_err(EngineError::from)?;
    calculate_combined_tax_and_liquidity(input).map_err(EngineError::from)
}

pub fn optimize_candidate_scenarios(
    candidates: Vec<EstateScenarioInput>,
) -> Result<Option<OptimizedScenario>, EngineError> {
    let mut all_issues = Vec::new();
    for (index, candidate) in candidates.iter().enumerate() {
        if let Err(err) = candidate.validate() {
            for mut issue in err.issues {
                issue.field = format!("candidates[{index}].{}", issue.field);
                all_issues.push(issue);
            }
        }
    }
    if !all_issues.is_empty() {
        return Err(EngineError::Validation(InputValidationError::new(
            all_issues,
        )));
    }
    optimize_scenarios(candidates).map_err(EngineError::from)
}

fn extract_text_for_estate_document_analysis(
    document: &ApiEstateDocumentInput,
) -> Result<String, ApiErrorResponse> {
    match document.format {
        ApiScenarioDocumentFormat::Json => require_text_document_content_from_parts(
            &document.document_content,
            document.document_content_base64.as_deref(),
            "Provide JSON content for this document",
        ),
        ApiScenarioDocumentFormat::Txt => require_text_document_content_from_parts(
            &document.document_content,
            document.document_content_base64.as_deref(),
            "Provide plain text content for this document",
        ),
        ApiScenarioDocumentFormat::Csv => require_text_document_content_from_parts(
            &document.document_content,
            document.document_content_base64.as_deref(),
            "Provide CSV content for this document",
        ),
        ApiScenarioDocumentFormat::Docx => {
            let bytes = decode_binary_payload(
                document.document_content_base64.as_deref(),
                &document.document_content,
                "DOCX",
            )?;
            extract_text_from_docx(&bytes)
        }
        ApiScenarioDocumentFormat::Pdf => {
            let bytes = decode_binary_payload(
                document.document_content_base64.as_deref(),
                &document.document_content,
                "PDF",
            )?;
            extract_text_from_pdf(&bytes)
        }
    }
}

fn require_text_document_content_from_parts(
    document_content: &str,
    document_content_base64: Option<&str>,
    guidance_message: &str,
) -> Result<String, ApiErrorResponse> {
    let content = document_content.trim();
    if !content.is_empty() {
        return Ok(content.to_string());
    }

    let Some(encoded) = document_content_base64 else {
        return Err(ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: "Document content cannot be empty".to_string(),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content".to_string(),
                message: guidance_message.to_string(),
            }],
        });
    };

    let bytes = decode_binary_payload(Some(encoded), "", "TEXT")?;
    String::from_utf8(bytes).map_err(|_| ApiErrorResponse {
        code: ApiErrorCode::Validation,
        message: "Unable to decode text content from base64 payload".to_string(),
        validation_issues: vec![ApiValidationIssue {
            field: "document_content_base64".to_string(),
            message: "Ensure the base64 payload contains UTF-8 text".to_string(),
        }],
    })
}

fn decode_binary_payload(
    encoded_payload: Option<&str>,
    fallback_payload: &str,
    format_label: &str,
) -> Result<Vec<u8>, ApiErrorResponse> {
    let encoded = encoded_payload.unwrap_or(fallback_payload).trim();

    if encoded.is_empty() {
        return Err(ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: format!("{format_label} payload is missing"),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content_base64".to_string(),
                message: format!(
                    "Provide base64-encoded {format_label} bytes in `document_content_base64`"
                ),
            }],
        });
    }

    let payload = if encoded.starts_with("data:") {
        encoded
            .split_once(',')
            .map(|(_, body)| body)
            .unwrap_or(encoded)
    } else {
        encoded
    };

    base64::engine::general_purpose::STANDARD
        .decode(payload.as_bytes())
        .map_err(|_| ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: format!("Unable to decode {format_label} document payload"),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content_base64".to_string(),
                message: "Ensure the uploaded document payload is valid base64".to_string(),
            }],
        })
}

fn detect_estate_document_types(
    document_name: Option<&str>,
    extracted_text: &str,
) -> Vec<ApiEstateDocumentType> {
    let name = document_name.unwrap_or("");
    let combined = format!("{name}\n{extracted_text}");
    let lower = combined.to_ascii_lowercase();

    let mut detected = BTreeSet::new();
    if line_contains_any(&lower, &["j294", "death notice"]) {
        detected.insert(ApiEstateDocumentType::DeathNoticeJ294);
    }
    if line_contains_any(
        &lower,
        &[
            "death certificate",
            "certificate of death",
            "dha-5",
            "certified copy of death certificate",
        ],
    ) {
        detected.insert(ApiEstateDocumentType::DeathCertificate);
    }
    if line_contains_any(
        &lower,
        &[
            "marriage certificate",
            "civil union",
            "customary marriage",
            "spouse declaration",
            "antenup",
            "antenuptial",
        ],
    ) {
        detected.insert(ApiEstateDocumentType::MarriageProofOrDeclaration);
    }
    if line_contains_any(
        &lower,
        &["last will", "last testament", "testamentary", "codicil"],
    ) {
        detected.insert(ApiEstateDocumentType::WillAndCodicils);
    }
    if line_contains_any(
        &lower,
        &[
            "intestate",
            "no valid will",
            "without a will",
            "letters of authority",
        ],
    ) {
        detected.insert(ApiEstateDocumentType::IntestateSupportingDocument);
    }
    if line_contains_any(&lower, &["j192", "next of kin affidavit"]) {
        detected.insert(ApiEstateDocumentType::NextOfKinAffidavitJ192);
    }
    if line_contains_any(&lower, &["j243", "inventory", "deceased estate inventory"]) {
        detected.insert(ApiEstateDocumentType::InventoryJ243);
    }
    if line_contains_any(&lower, &["j190", "acceptance of trust"]) {
        detected.insert(ApiEstateDocumentType::AcceptanceOfTrustJ190);
    }
    if line_contains_any(&lower, &["j155", "letters of authority"]) {
        detected.insert(ApiEstateDocumentType::LettersOfAuthorityJ155);
    }
    if line_contains_any(
        &lower,
        &[
            "j170",
            "declaration by person",
            "declaration regarding estate reporting",
        ],
    ) {
        detected.insert(ApiEstateDocumentType::DeclarationJ170);
    }
    if line_contains_any(&lower, &["j238", "letters of executorship"]) {
        detected.insert(ApiEstateDocumentType::LettersOfExecutorshipJ238);
    }
    if line_contains_any(&lower, &["j262", "security bond", "bond of security"]) {
        detected.insert(ApiEstateDocumentType::SecurityBondJ262);
    }
    if line_contains_any(&lower, &["rev267", "estate duty return"]) {
        detected.insert(ApiEstateDocumentType::EstateDutyReturnRev267);
    }
    if line_contains_any(
        &lower,
        &[
            "liquidation and distribution account",
            "l&d account",
            "l and d account",
        ],
    ) {
        detected.insert(ApiEstateDocumentType::LiquidationAndDistributionAccount);
    }
    if line_contains_any(
        &lower,
        &[
            "new estate case",
            "estate case supporting documents",
            "estate case pack",
        ],
    ) {
        detected.insert(ApiEstateDocumentType::NewEstateCaseSupportingPack);
    }
    if line_contains_any(
        &lower,
        &[
            "itr12",
            "income tax return",
            "deceased estate income tax",
            "deceased taxpayer return",
        ],
    ) {
        detected.insert(ApiEstateDocumentType::IncomeTaxReturnItr12);
    }

    detected.into_iter().collect()
}

fn build_sa_estate_document_checklist(
    by_document_type: &BTreeMap<ApiEstateDocumentType, BTreeSet<usize>>,
) -> Vec<ApiEstateDocumentChecklistItem> {
    let has_will = has_document_type(by_document_type, ApiEstateDocumentType::WillAndCodicils);
    let has_intestate = has_document_type(
        by_document_type,
        ApiEstateDocumentType::IntestateSupportingDocument,
    );
    let has_j192 = has_document_type(
        by_document_type,
        ApiEstateDocumentType::NextOfKinAffidavitJ192,
    );

    let has_j190 = has_document_type(
        by_document_type,
        ApiEstateDocumentType::AcceptanceOfTrustJ190,
    );
    let has_j155 = has_document_type(
        by_document_type,
        ApiEstateDocumentType::LettersOfAuthorityJ155,
    );
    let has_j170 = has_document_type(by_document_type, ApiEstateDocumentType::DeclarationJ170);
    let has_j238 = has_document_type(
        by_document_type,
        ApiEstateDocumentType::LettersOfExecutorshipJ238,
    );
    let appointment_satisfied = has_j190 || has_j238 || (has_j155 && has_j170);

    let mut checklist = vec![
        checklist_required_single(
            "sa-legal-death-notice-j294",
            "Death Notice (J294)",
            ApiEstateDocumentType::DeathNoticeJ294,
            by_document_type,
            Vec::new(),
        ),
        checklist_required_single(
            "sa-legal-death-certificate",
            "Death certificate",
            ApiEstateDocumentType::DeathCertificate,
            by_document_type,
            Vec::new(),
        ),
        checklist_conditional_single(
            "sa-legal-marriage-proof",
            "Marriage proof/declaration (where applicable)",
            ApiEstateDocumentType::MarriageProofOrDeclaration,
            by_document_type,
            "Applicable when marital regime/spousal deductions are relevant".to_string(),
        ),
        checklist_required_any_of(
            "sa-legal-testamentary-basis",
            "Will + codicils OR intestate supporting documentation",
            &[
                ApiEstateDocumentType::WillAndCodicils,
                ApiEstateDocumentType::IntestateSupportingDocument,
            ],
            by_document_type,
            vec!["At least one of these documents is required".to_string()],
        ),
    ];

    let j192_status = if has_intestate && !has_will {
        if has_j192 {
            ApiEstateDocumentRequirementStatus::Satisfied
        } else {
            ApiEstateDocumentRequirementStatus::Missing
        }
    } else {
        ApiEstateDocumentRequirementStatus::NotApplicable
    };
    checklist.push(ApiEstateDocumentChecklistItem {
        requirement_id: "sa-legal-next-of-kin-j192".to_string(),
        description: "Next-of-kin affidavit (J192) if no valid will".to_string(),
        status: j192_status,
        required_document_types: vec![ApiEstateDocumentType::NextOfKinAffidavitJ192],
        matched_document_indices: matched_indices_for_type(
            by_document_type,
            ApiEstateDocumentType::NextOfKinAffidavitJ192,
        ),
        notes: vec![
            "Required only when estate reporting is intestate and no valid will is available"
                .to_string(),
        ],
    });

    checklist.push(checklist_required_single(
        "sa-legal-inventory-j243",
        "Inventory of assets (J243)",
        ApiEstateDocumentType::InventoryJ243,
        by_document_type,
        Vec::new(),
    ));

    let appointment_status = if appointment_satisfied {
        ApiEstateDocumentRequirementStatus::Satisfied
    } else {
        ApiEstateDocumentRequirementStatus::Missing
    };
    let mut appointment_indices = matched_indices_for_type(
        by_document_type,
        ApiEstateDocumentType::AcceptanceOfTrustJ190,
    );
    appointment_indices.extend(matched_indices_for_type(
        by_document_type,
        ApiEstateDocumentType::LettersOfAuthorityJ155,
    ));
    appointment_indices.extend(matched_indices_for_type(
        by_document_type,
        ApiEstateDocumentType::DeclarationJ170,
    ));
    appointment_indices.extend(matched_indices_for_type(
        by_document_type,
        ApiEstateDocumentType::LettersOfExecutorshipJ238,
    ));
    appointment_indices.sort_unstable();
    appointment_indices.dedup();
    checklist.push(ApiEstateDocumentChecklistItem {
        requirement_id: "sa-legal-executor-appointment".to_string(),
        description:
            "Executor/Master appointment: J190 OR J238 OR (J155 + J170 for smaller estates)"
                .to_string(),
        status: appointment_status,
        required_document_types: vec![
            ApiEstateDocumentType::AcceptanceOfTrustJ190,
            ApiEstateDocumentType::LettersOfExecutorshipJ238,
            ApiEstateDocumentType::LettersOfAuthorityJ155,
            ApiEstateDocumentType::DeclarationJ170,
        ],
        matched_document_indices: appointment_indices,
        notes: vec![
            "Alternative satisfaction rules applied: J190 OR J238 OR (J155 + J170)".to_string(),
        ],
    });

    checklist.push(checklist_conditional_single(
        "sa-legal-security-bond-j262",
        "Security/bond forms (J262) where required",
        ApiEstateDocumentType::SecurityBondJ262,
        by_document_type,
        "Conditional on Master requirements and executor circumstances".to_string(),
    ));

    checklist.push(checklist_required_single(
        "sa-tax-estate-duty-rev267",
        "Estate Duty return (REV267)",
        ApiEstateDocumentType::EstateDutyReturnRev267,
        by_document_type,
        Vec::new(),
    ));
    checklist.push(checklist_required_single(
        "sa-tax-liquidation-distribution-account",
        "Liquidation and distribution account",
        ApiEstateDocumentType::LiquidationAndDistributionAccount,
        by_document_type,
        Vec::new(),
    ));

    let new_case_pack_complete =
        has_document_type(by_document_type, ApiEstateDocumentType::DeathNoticeJ294)
            && has_document_type(by_document_type, ApiEstateDocumentType::DeathCertificate)
            && has_document_type(by_document_type, ApiEstateDocumentType::InventoryJ243)
            && (has_will || has_intestate)
            && appointment_satisfied;
    let new_case_pack_status = if has_document_type(
        by_document_type,
        ApiEstateDocumentType::NewEstateCaseSupportingPack,
    ) || new_case_pack_complete
    {
        ApiEstateDocumentRequirementStatus::Satisfied
    } else {
        ApiEstateDocumentRequirementStatus::Missing
    };
    let mut new_case_indices = matched_indices_for_type(
        by_document_type,
        ApiEstateDocumentType::NewEstateCaseSupportingPack,
    );
    new_case_indices.extend(matched_indices_for_type(
        by_document_type,
        ApiEstateDocumentType::DeathNoticeJ294,
    ));
    new_case_indices.extend(matched_indices_for_type(
        by_document_type,
        ApiEstateDocumentType::DeathCertificate,
    ));
    new_case_indices.extend(matched_indices_for_type(
        by_document_type,
        ApiEstateDocumentType::InventoryJ243,
    ));
    new_case_indices.extend(matched_indices_for_type(
        by_document_type,
        ApiEstateDocumentType::WillAndCodicils,
    ));
    new_case_indices.extend(matched_indices_for_type(
        by_document_type,
        ApiEstateDocumentType::IntestateSupportingDocument,
    ));
    new_case_indices.sort_unstable();
    new_case_indices.dedup();
    checklist.push(ApiEstateDocumentChecklistItem {
        requirement_id: "sa-tax-new-estate-case-supporting-pack".to_string(),
        description: "New estate case supporting pack".to_string(),
        status: new_case_pack_status,
        required_document_types: vec![
            ApiEstateDocumentType::NewEstateCaseSupportingPack,
            ApiEstateDocumentType::DeathNoticeJ294,
            ApiEstateDocumentType::DeathCertificate,
            ApiEstateDocumentType::InventoryJ243,
            ApiEstateDocumentType::WillAndCodicils,
            ApiEstateDocumentType::IntestateSupportingDocument,
            ApiEstateDocumentType::AcceptanceOfTrustJ190,
            ApiEstateDocumentType::LettersOfAuthorityJ155,
            ApiEstateDocumentType::DeclarationJ170,
            ApiEstateDocumentType::LettersOfExecutorshipJ238,
        ],
        matched_document_indices: new_case_indices,
        notes: vec![
            "Satisfied by either an explicit 'supporting pack' document or the core components: J294 + death certificate + J243 + testamentary basis + appointment docs".to_string(),
            "IDs and executor detail attachments still require manual completeness check".to_string(),
        ],
    });

    checklist.push(checklist_conditional_single(
        "sa-tax-itr12",
        "Income tax filings for deceased/estate (ITR12 pre- and post-death where applicable)",
        ApiEstateDocumentType::IncomeTaxReturnItr12,
        by_document_type,
        "Required based on SARS filing obligations and assessment periods".to_string(),
    ));

    checklist
}

fn checklist_required_single(
    requirement_id: &str,
    description: &str,
    required_type: ApiEstateDocumentType,
    by_document_type: &BTreeMap<ApiEstateDocumentType, BTreeSet<usize>>,
    notes: Vec<String>,
) -> ApiEstateDocumentChecklistItem {
    let matched_document_indices = matched_indices_for_type(by_document_type, required_type);
    let status = if matched_document_indices.is_empty() {
        ApiEstateDocumentRequirementStatus::Missing
    } else {
        ApiEstateDocumentRequirementStatus::Satisfied
    };
    ApiEstateDocumentChecklistItem {
        requirement_id: requirement_id.to_string(),
        description: description.to_string(),
        status,
        required_document_types: vec![required_type],
        matched_document_indices,
        notes,
    }
}

fn checklist_conditional_single(
    requirement_id: &str,
    description: &str,
    required_type: ApiEstateDocumentType,
    by_document_type: &BTreeMap<ApiEstateDocumentType, BTreeSet<usize>>,
    conditional_note: String,
) -> ApiEstateDocumentChecklistItem {
    let matched_document_indices = matched_indices_for_type(by_document_type, required_type);
    let status = if matched_document_indices.is_empty() {
        ApiEstateDocumentRequirementStatus::Conditional
    } else {
        ApiEstateDocumentRequirementStatus::Satisfied
    };
    ApiEstateDocumentChecklistItem {
        requirement_id: requirement_id.to_string(),
        description: description.to_string(),
        status,
        required_document_types: vec![required_type],
        matched_document_indices,
        notes: vec![conditional_note],
    }
}

fn checklist_required_any_of(
    requirement_id: &str,
    description: &str,
    required_any_of: &[ApiEstateDocumentType],
    by_document_type: &BTreeMap<ApiEstateDocumentType, BTreeSet<usize>>,
    notes: Vec<String>,
) -> ApiEstateDocumentChecklistItem {
    let mut matched_document_indices = Vec::new();
    for required in required_any_of {
        matched_document_indices.extend(matched_indices_for_type(by_document_type, *required));
    }
    matched_document_indices.sort_unstable();
    matched_document_indices.dedup();

    let status = if matched_document_indices.is_empty() {
        ApiEstateDocumentRequirementStatus::Missing
    } else {
        ApiEstateDocumentRequirementStatus::Satisfied
    };

    ApiEstateDocumentChecklistItem {
        requirement_id: requirement_id.to_string(),
        description: description.to_string(),
        status,
        required_document_types: required_any_of.to_vec(),
        matched_document_indices,
        notes,
    }
}

fn collect_missing_required_document_types(
    checklist: &[ApiEstateDocumentChecklistItem],
    by_document_type: &BTreeMap<ApiEstateDocumentType, BTreeSet<usize>>,
) -> Vec<ApiEstateDocumentType> {
    let mut missing = BTreeSet::new();
    for item in checklist {
        if item.status != ApiEstateDocumentRequirementStatus::Missing {
            continue;
        }

        // Special handling for alternative executor requirement.
        if item.requirement_id == "sa-legal-executor-appointment" {
            let has_j190 = has_document_type(
                by_document_type,
                ApiEstateDocumentType::AcceptanceOfTrustJ190,
            );
            let has_j238 = has_document_type(
                by_document_type,
                ApiEstateDocumentType::LettersOfExecutorshipJ238,
            );
            let has_j155 = has_document_type(
                by_document_type,
                ApiEstateDocumentType::LettersOfAuthorityJ155,
            );
            let has_j170 =
                has_document_type(by_document_type, ApiEstateDocumentType::DeclarationJ170);
            if !has_j190 {
                missing.insert(ApiEstateDocumentType::AcceptanceOfTrustJ190);
            }
            if !has_j238 {
                missing.insert(ApiEstateDocumentType::LettersOfExecutorshipJ238);
            }
            if !(has_j155 && has_j170) {
                if !has_j155 {
                    missing.insert(ApiEstateDocumentType::LettersOfAuthorityJ155);
                }
                if !has_j170 {
                    missing.insert(ApiEstateDocumentType::DeclarationJ170);
                }
            }
            continue;
        }

        // Special handling for the supporting pack: this can be satisfied by either
        // a single explicit pack document or a set of component documents.
        if item.requirement_id == "sa-tax-new-estate-case-supporting-pack" {
            if has_document_type(
                by_document_type,
                ApiEstateDocumentType::NewEstateCaseSupportingPack,
            ) {
                continue;
            }

            if !has_document_type(by_document_type, ApiEstateDocumentType::DeathNoticeJ294) {
                missing.insert(ApiEstateDocumentType::DeathNoticeJ294);
            }
            if !has_document_type(by_document_type, ApiEstateDocumentType::DeathCertificate) {
                missing.insert(ApiEstateDocumentType::DeathCertificate);
            }
            if !has_document_type(by_document_type, ApiEstateDocumentType::InventoryJ243) {
                missing.insert(ApiEstateDocumentType::InventoryJ243);
            }

            let has_will =
                has_document_type(by_document_type, ApiEstateDocumentType::WillAndCodicils);
            let has_intestate = has_document_type(
                by_document_type,
                ApiEstateDocumentType::IntestateSupportingDocument,
            );
            if !(has_will || has_intestate) {
                missing.insert(ApiEstateDocumentType::WillAndCodicils);
                missing.insert(ApiEstateDocumentType::IntestateSupportingDocument);
            }

            let has_j190 = has_document_type(
                by_document_type,
                ApiEstateDocumentType::AcceptanceOfTrustJ190,
            );
            let has_j238 = has_document_type(
                by_document_type,
                ApiEstateDocumentType::LettersOfExecutorshipJ238,
            );
            let has_j155 = has_document_type(
                by_document_type,
                ApiEstateDocumentType::LettersOfAuthorityJ155,
            );
            let has_j170 =
                has_document_type(by_document_type, ApiEstateDocumentType::DeclarationJ170);
            if !(has_j190 || has_j238 || (has_j155 && has_j170)) {
                if !has_j190 {
                    missing.insert(ApiEstateDocumentType::AcceptanceOfTrustJ190);
                }
                if !has_j238 {
                    missing.insert(ApiEstateDocumentType::LettersOfExecutorshipJ238);
                }
                if !has_j155 {
                    missing.insert(ApiEstateDocumentType::LettersOfAuthorityJ155);
                }
                if !has_j170 {
                    missing.insert(ApiEstateDocumentType::DeclarationJ170);
                }
            }
            continue;
        }

        for required in &item.required_document_types {
            if !has_document_type(by_document_type, *required) {
                missing.insert(*required);
            }
        }
    }

    missing.into_iter().collect()
}

fn compute_readiness_score(checklist: &[ApiEstateDocumentChecklistItem]) -> f64 {
    let mut required_total = 0usize;
    let mut required_satisfied = 0usize;
    for item in checklist {
        if matches!(
            item.status,
            ApiEstateDocumentRequirementStatus::Conditional
                | ApiEstateDocumentRequirementStatus::NotApplicable
        ) {
            continue;
        }
        required_total += 1;
        if item.status == ApiEstateDocumentRequirementStatus::Satisfied {
            required_satisfied += 1;
        }
    }

    if required_total == 0 {
        return 1.0;
    }

    required_satisfied as f64 / required_total as f64
}

fn has_document_type(
    by_document_type: &BTreeMap<ApiEstateDocumentType, BTreeSet<usize>>,
    document_type: ApiEstateDocumentType,
) -> bool {
    by_document_type
        .get(&document_type)
        .map(|indices| !indices.is_empty())
        .unwrap_or(false)
}

fn matched_indices_for_type(
    by_document_type: &BTreeMap<ApiEstateDocumentType, BTreeSet<usize>>,
    document_type: ApiEstateDocumentType,
) -> Vec<usize> {
    by_document_type
        .get(&document_type)
        .map(|indices| indices.iter().copied().collect())
        .unwrap_or_default()
}

fn parse_scenarios_from_document(
    request: &ApiScenarioDocumentIngestRequest,
) -> Result<Vec<ApiEstateScenarioInput>, ApiErrorResponse> {
    ingest_debug_log(&format!(
        "parse_scenarios_from_document format={:?} content_len={} base64_len={}",
        request.format,
        request.document_content.len(),
        request
            .document_content_base64
            .as_ref()
            .map(|value| value.len())
            .unwrap_or(0)
    ));

    match request.format {
        ApiScenarioDocumentFormat::Json => {
            let content = require_text_document_content(
                request,
                "Provide a JSON document with scenario data",
            )?;
            parse_json_scenarios_document(content)
        }
        ApiScenarioDocumentFormat::Txt => {
            let content = require_text_document_content(
                request,
                "Provide a plain-text document with embedded JSON scenario data",
            )?;
            parse_structured_text_document(content, "TXT")
        }
        ApiScenarioDocumentFormat::Csv => {
            let content = require_text_document_content(
                request,
                "Provide a CSV document with scenario data",
            )?;
            parse_csv_scenarios_document(content)
        }
        ApiScenarioDocumentFormat::Docx => parse_docx_scenarios_document(request),
        ApiScenarioDocumentFormat::Pdf => parse_pdf_scenarios_document(request),
    }
}

fn parse_json_scenarios_document(
    document_content: &str,
) -> Result<Vec<ApiEstateScenarioInput>, ApiErrorResponse> {
    if let Ok(single) = serde_json::from_str::<ApiEstateScenarioInput>(document_content) {
        return Ok(vec![single]);
    }

    if let Ok(many) = serde_json::from_str::<Vec<ApiEstateScenarioInput>>(document_content) {
        return Ok(many);
    }

    if let Ok(envelope) = serde_json::from_str::<JsonScenarioEnvelope>(document_content) {
        return Ok(envelope.scenarios);
    }

    Err(ApiErrorResponse {
        code: ApiErrorCode::Validation,
        message: "Unable to parse JSON document into scenario input".to_string(),
        validation_issues: vec![ApiValidationIssue {
            field: "document_content".to_string(),
            message: "Expected a scenario object, an array of scenarios, or an object with a `scenarios` array".to_string(),
        }],
    })
}

fn require_text_document_content<'a>(
    request: &'a ApiScenarioDocumentIngestRequest,
    guidance_message: &str,
) -> Result<&'a str, ApiErrorResponse> {
    let content = request.document_content.trim();
    if !content.is_empty() {
        return Ok(content);
    }

    Err(ApiErrorResponse {
        code: ApiErrorCode::Validation,
        message: "Document content cannot be empty".to_string(),
        validation_issues: vec![ApiValidationIssue {
            field: "document_content".to_string(),
            message: guidance_message.to_string(),
        }],
    })
}

fn require_binary_document_bytes(
    request: &ApiScenarioDocumentIngestRequest,
    format_label: &str,
) -> Result<Vec<u8>, ApiErrorResponse> {
    let decoded = decode_binary_payload(
        request.document_content_base64.as_deref(),
        &request.document_content,
        format_label,
    )?;

    ingest_debug_log(&format!(
        "decoded {format_label} payload bytes={}",
        decoded.len()
    ));
    Ok(decoded)
}

fn parse_docx_scenarios_document(
    request: &ApiScenarioDocumentIngestRequest,
) -> Result<Vec<ApiEstateScenarioInput>, ApiErrorResponse> {
    let bytes = require_binary_document_bytes(request, "DOCX")?;
    let extracted_text = extract_text_from_docx(&bytes)?;
    parse_structured_text_document(&extracted_text, "DOCX")
}

fn parse_pdf_scenarios_document(
    request: &ApiScenarioDocumentIngestRequest,
) -> Result<Vec<ApiEstateScenarioInput>, ApiErrorResponse> {
    ingest_debug_log("parse_pdf_scenarios_document start");
    let bytes = require_binary_document_bytes(request, "PDF")?;
    let extracted_text = extract_text_from_pdf(&bytes)?;
    ingest_debug_log(&format!(
        "PDF extracted_text_len={} preview=\"{}\"",
        extracted_text.len(),
        preview_for_log(&extracted_text, 240)
    ));
    parse_structured_text_document(&extracted_text, "PDF")
}

fn extract_text_from_docx(bytes: &[u8]) -> Result<String, ApiErrorResponse> {
    let cursor = Cursor::new(bytes);
    let mut archive = ZipArchive::new(cursor).map_err(|_| ApiErrorResponse {
        code: ApiErrorCode::Validation,
        message: "Unable to open DOCX archive".to_string(),
        validation_issues: vec![ApiValidationIssue {
            field: "document_content_base64".to_string(),
            message: "Uploaded bytes are not a valid DOCX file".to_string(),
        }],
    })?;

    let mut xml_file = archive
        .by_name("word/document.xml")
        .map_err(|_| ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: "Unable to read DOCX content".to_string(),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content_base64".to_string(),
                message: "DOCX file does not contain word/document.xml".to_string(),
            }],
        })?;

    let mut xml = String::new();
    xml_file
        .read_to_string(&mut xml)
        .map_err(|_| ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: "Unable to decode DOCX XML content".to_string(),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content_base64".to_string(),
                message: "DOCX XML content is not valid UTF-8".to_string(),
            }],
        })?;

    let normalized = xml
        .replace("</w:p>", "\n")
        .replace("<w:br/>", "\n")
        .replace("<w:br />", "\n")
        .replace("<w:tab/>", "\t")
        .replace("<w:tab />", "\t");

    let stripped = strip_xml_tags(&normalized);
    let decoded = decode_common_xml_entities(&stripped);

    if decoded.trim().is_empty() {
        return Err(ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: "DOCX document did not contain readable text".to_string(),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content_base64".to_string(),
                message: "Include scenario JSON in the DOCX body text".to_string(),
            }],
        });
    }

    Ok(decoded)
}

fn strip_xml_tags(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut in_tag = false;

    for ch in input.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => output.push(ch),
            _ => {}
        }
    }

    output
}

fn decode_common_xml_entities(input: &str) -> String {
    input
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

fn extract_text_from_pdf(bytes: &[u8]) -> Result<String, ApiErrorResponse> {
    ingest_debug_log(&format!("extract_text_from_pdf bytes={}", bytes.len()));
    let document = lopdf::Document::load_mem(bytes).map_err(|_| ApiErrorResponse {
        code: ApiErrorCode::Validation,
        message: "Unable to parse PDF document".to_string(),
        validation_issues: vec![ApiValidationIssue {
            field: "document_content_base64".to_string(),
            message: "Uploaded bytes are not a valid PDF file".to_string(),
        }],
    })?;

    let pages = document.get_pages();
    ingest_debug_log(&format!("extract_text_from_pdf pages={}", pages.len()));
    if pages.is_empty() {
        return Err(ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: "PDF document does not contain pages".to_string(),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content_base64".to_string(),
                message: "Upload a PDF that contains textual content".to_string(),
            }],
        });
    }

    let page_numbers: Vec<u32> = pages.keys().copied().collect();
    let text = document
        .extract_text(&page_numbers)
        .map_err(|_| ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: "Unable to extract text from PDF document".to_string(),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content_base64".to_string(),
                message: "Ensure the PDF contains selectable text, not image-only scans"
                    .to_string(),
            }],
        })?;
    ingest_debug_log(&format!("extract_text_from_pdf text_len={}", text.len()));

    if text.trim().is_empty() {
        return Err(ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: "PDF document did not contain readable text".to_string(),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content_base64".to_string(),
                message: "Include scenario JSON in the PDF text body".to_string(),
            }],
        });
    }

    Ok(text)
}

fn parse_structured_text_document(
    document_content: &str,
    source_label: &str,
) -> Result<Vec<ApiEstateScenarioInput>, ApiErrorResponse> {
    ingest_debug_log(&format!(
        "parse_structured_text_document source={source_label} len={} preview=\"{}\"",
        document_content.len(),
        preview_for_log(document_content, 240)
    ));

    if let Ok(scenarios) = parse_json_scenarios_document(document_content) {
        ingest_debug_log(&format!(
            "parse_structured_text_document source={source_label} matched=raw_json scenarios={}",
            scenarios.len()
        ));
        return Ok(scenarios);
    }
    ingest_debug_log(&format!(
        "parse_structured_text_document source={source_label} raw_json=no_match"
    ));

    if let Some(block) = extract_json_from_code_fence(document_content) {
        ingest_debug_log(&format!(
            "parse_structured_text_document source={source_label} code_fence_found len={}",
            block.len()
        ));
        if let Ok(scenarios) = parse_json_scenarios_document(&block) {
            ingest_debug_log(&format!(
                "parse_structured_text_document source={source_label} matched=code_fence_json scenarios={}",
                scenarios.len()
            ));
            return Ok(scenarios);
        }
        ingest_debug_log(&format!(
            "parse_structured_text_document source={source_label} code_fence_json=no_match"
        ));
    }

    if let Some(candidate) = extract_first_balanced_json(document_content) {
        ingest_debug_log(&format!(
            "parse_structured_text_document source={source_label} balanced_json_found len={}",
            candidate.len()
        ));
        if let Ok(scenarios) = parse_json_scenarios_document(candidate) {
            ingest_debug_log(&format!(
                "parse_structured_text_document source={source_label} matched=balanced_json scenarios={}",
                scenarios.len()
            ));
            return Ok(scenarios);
        }
        ingest_debug_log(&format!(
            "parse_structured_text_document source={source_label} balanced_json=no_match"
        ));
    }

    match parse_structured_key_value_scenarios_document(document_content) {
        StructuredTextParseOutcome::Parsed(scenarios) => {
            ingest_debug_log(&format!(
                "parse_structured_text_document source={source_label} matched=structured_key_value scenarios={}",
                scenarios.len()
            ));
            return Ok(scenarios);
        }
        StructuredTextParseOutcome::Invalid(error) => {
            ingest_debug_log(&format!(
                "parse_structured_text_document source={source_label} structured_key_value=invalid message=\"{}\"",
                error.message
            ));
            return Err(error);
        }
        StructuredTextParseOutcome::NotDetected => {
            ingest_debug_log(&format!(
                "parse_structured_text_document source={source_label} structured_key_value=not_detected"
            ));
        }
    }

    match parse_narrative_estate_document(document_content) {
        StructuredTextParseOutcome::Parsed(scenarios) => {
            let has_any_assets = scenarios.iter().any(|scenario| !scenario.assets.is_empty());
            if !has_any_assets {
                if let Some(guidance_error) =
                    legal_document_as_scenario_guidance_error(document_content, source_label)
                {
                    ingest_debug_log(&format!(
                        "parse_structured_text_document source={source_label} narrative_estate=legal_guidance message=\"{}\"",
                        guidance_error.message
                    ));
                    return Err(guidance_error);
                }
            }
            ingest_debug_log(&format!(
                "parse_structured_text_document source={source_label} matched=narrative_estate scenarios={}",
                scenarios.len()
            ));
            return Ok(scenarios);
        }
        StructuredTextParseOutcome::Invalid(error) => {
            ingest_debug_log(&format!(
                "parse_structured_text_document source={source_label} narrative_estate=invalid message=\"{}\"",
                error.message
            ));
            return Err(error);
        }
        StructuredTextParseOutcome::NotDetected => {
            ingest_debug_log(&format!(
                "parse_structured_text_document source={source_label} narrative_estate=not_detected"
            ));
        }
    }

    ingest_debug_log(&format!(
        "parse_structured_text_document source={source_label} no_supported_payload_detected"
    ));
    if let Some(guidance_error) =
        legal_document_as_scenario_guidance_error(document_content, source_label)
    {
        ingest_debug_log(&format!(
            "parse_structured_text_document source={source_label} legal_guidance message=\"{}\"",
            guidance_error.message
        ));
        return Err(guidance_error);
    }

    Err(ApiErrorResponse {
        code: ApiErrorCode::Validation,
        message: format!("Unable to locate scenario JSON in {source_label} document"),
        validation_issues: vec![ApiValidationIssue {
            field: "document_content".to_string(),
            message:
                "Embed a JSON scenario object/array (or `{ \"scenarios\": [...] }`) in the uploaded document"
                    .to_string(),
        }],
    })
}

fn legal_document_as_scenario_guidance_error(
    document_content: &str,
    source_label: &str,
) -> Option<ApiErrorResponse> {
    let detected = detect_estate_document_types(None, document_content);
    if detected.is_empty() {
        return None;
    }

    Some(ApiErrorResponse {
        code: ApiErrorCode::Validation,
        message: format!(
            "{source_label} appears to be a legal/tax estate document, not a scenario input document"
        ),
        validation_issues: vec![ApiValidationIssue {
            field: "document_content".to_string(),
            message: "Use `/v1/estate/documents/analyze` for legal/tax intake completeness. For scenario calculation, upload financial inventory data (for example J243, valuations, bank/investment statements) or provide scenario JSON."
                .to_string(),
        }],
    })
}

fn parse_structured_key_value_scenarios_document(
    document_content: &str,
) -> StructuredTextParseOutcome {
    let mut scenario_value =
        serde_json::to_value(ApiEstateScenarioInput::from(EstateScenarioInput::default()))
            .expect("Default scenario should serialize");
    let Some(scenario_object) = scenario_value.as_object_mut() else {
        return StructuredTextParseOutcome::NotDetected;
    };

    let mut assets: Vec<serde_json::Map<String, Value>> = Vec::new();
    let mut recognized_fields = 0usize;
    let mut ignored_pairs = 0usize;

    for line in document_content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("```") {
            continue;
        }

        let Some((raw_key, raw_value)) = split_structured_pair(trimmed) else {
            continue;
        };
        let normalized_key = normalize_structured_key(raw_key);
        if normalized_key.is_empty() || raw_value.trim().is_empty() {
            continue;
        }

        if let Some((asset_index, asset_field)) = resolve_prefixed_asset_key(&normalized_key) {
            ensure_asset_slot(&mut assets, asset_index);
            assets[asset_index].insert(asset_field.to_string(), parse_structured_value(raw_value));
            recognized_fields += 1;
            continue;
        }

        if let Some(scenario_field) = resolve_scenario_key(&normalized_key) {
            let value = parse_structured_scenario_value(scenario_field, raw_value);
            scenario_object.insert(scenario_field.to_string(), value);
            recognized_fields += 1;
            continue;
        }

        if let Some(asset_field) = resolve_asset_field(&normalized_key) {
            ensure_asset_slot(&mut assets, 0);
            assets[0].insert(asset_field.to_string(), parse_structured_value(raw_value));
            recognized_fields += 1;
            continue;
        }

        ignored_pairs += 1;
    }

    ingest_debug_log(&format!(
        "parse_structured_key_value_scenarios_document recognized_fields={recognized_fields} ignored_pairs={ignored_pairs} asset_slots={}",
        assets.len()
    ));

    if recognized_fields == 0 {
        return StructuredTextParseOutcome::NotDetected;
    }

    if assets.iter().any(|asset| !asset.is_empty()) {
        let parsed_assets = assets
            .into_iter()
            .filter(|asset| !asset.is_empty())
            .map(Value::Object)
            .collect::<Vec<_>>();
        scenario_object.insert("assets".to_string(), Value::Array(parsed_assets));
    }

    match serde_json::from_value::<ApiEstateScenarioInput>(scenario_value) {
        Ok(parsed) => StructuredTextParseOutcome::Parsed(vec![parsed]),
        Err(err) => StructuredTextParseOutcome::Invalid(ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: "Unable to parse structured scenario fields from document".to_string(),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content".to_string(),
                message: format!(
                    "Use `key: value` fields that map to scenario/asset input names: {err}"
                ),
            }],
        }),
    }
}

fn split_structured_pair(line: &str) -> Option<(&str, &str)> {
    let separators = [':', '=', '|'];
    let mut best: Option<(usize, char)> = None;

    for separator in separators {
        if let Some(index) = line.find(separator) {
            if index == 0 {
                continue;
            }

            match best {
                Some((best_index, _)) if index >= best_index => {}
                _ => best = Some((index, separator)),
            }
        }
    }

    let (index, separator) = best?;
    let (left, right_with_separator) = line.split_at(index);
    let right = right_with_separator.strip_prefix(separator)?;
    Some((left.trim(), right.trim()))
}

fn normalize_structured_key(raw: &str) -> String {
    let mut normalized = String::with_capacity(raw.len());
    let mut last_was_underscore = false;

    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
            last_was_underscore = false;
            continue;
        }

        if !last_was_underscore {
            normalized.push('_');
            last_was_underscore = true;
        }
    }

    normalized.trim_matches('_').to_string()
}

fn resolve_scenario_key(normalized_key: &str) -> Option<&'static str> {
    match normalized_key {
        "jurisdiction" | "country" | "state" => Some("jurisdiction"),
        "tax_year" | "year" => Some("tax_year"),
        "taxpayer_class" | "tax_payer_class" | "taxpayer" | "tax_payer" | "entity_type" => {
            Some("taxpayer_class")
        }
        "residency_status" | "residency" | "residence_status" => Some("residency_status"),
        "marginal_income_tax_rate" | "marginal_tax_rate" | "income_tax_rate" => {
            Some("marginal_income_tax_rate")
        }
        "debts_and_loans_amount" | "debts_and_loans" | "debts_and_loans_zar" => {
            Some("debts_and_loans_amount")
        }
        "funeral_costs_amount" | "funeral_costs" | "funeral_costs_zar" => {
            Some("funeral_costs_amount")
        }
        "administration_costs_amount" | "administration_costs" | "administration_costs_zar" => {
            Some("administration_costs_amount")
        }
        "masters_office_fees_amount" | "masters_office_fees" | "masters_office_fees_zar" => {
            Some("masters_office_fees_amount")
        }
        "conveyancing_costs_amount" | "conveyancing_costs" | "conveyancing_costs_zar" => {
            Some("conveyancing_costs_amount")
        }
        "other_settlement_costs_amount"
        | "other_settlement_costs"
        | "other_settlement_costs_zar" => Some("other_settlement_costs_amount"),
        "final_income_tax_due_amount" | "final_income_tax_due" | "final_income_tax_due_zar" => {
            Some("final_income_tax_due_amount")
        }
        "ongoing_estate_income_tax_provision_amount"
        | "ongoing_estate_income_tax_provision"
        | "ongoing_estate_income_tax_provision_zar" => {
            Some("ongoing_estate_income_tax_provision_amount")
        }
        "additional_allowable_estate_transfer_tax_deductions_amount"
        | "additional_allowable_estate_transfer_tax_deductions"
        | "additional_allowable_estate_duty_deductions_zar" => {
            Some("additional_allowable_estate_transfer_tax_deductions_amount")
        }
        "ported_estate_tax_exemption_amount"
        | "ported_estate_tax_exemption"
        | "ported_section_4a_abatement_zar" => Some("ported_estate_tax_exemption_amount"),
        "primary_residence_cgt_exclusion_cap_amount"
        | "primary_residence_cgt_exclusion_cap"
        | "primary_residence_cgt_exclusion_cap_zar" => {
            Some("primary_residence_cgt_exclusion_cap_amount")
        }
        "executor_fee_rate" => Some("executor_fee_rate"),
        "vat_rate" => Some("vat_rate"),
        "explicit_executor_fee_amount" | "explicit_executor_fee" | "explicit_executor_fee_zar" => {
            Some("explicit_executor_fee_amount")
        }
        "external_liquidity_proceeds_amount"
        | "external_liquidity_proceeds"
        | "external_liquidity_proceeds_zar" => Some("external_liquidity_proceeds_amount"),
        "cash_reserve_amount" | "cash_reserve" | "cash_reserve_zar" => Some("cash_reserve_amount"),
        _ => None,
    }
}

fn resolve_asset_field(normalized_key: &str) -> Option<&'static str> {
    match normalized_key {
        "name" | "asset_name" => Some("name"),
        "market_value_amount" | "market_value" | "market_value_zar" | "asset_market_value" => {
            Some("market_value_amount")
        }
        "base_cost_amount" | "base_cost" | "base_cost_zar" | "asset_base_cost" => {
            Some("base_cost_amount")
        }
        "is_liquid" | "liquid" => Some("is_liquid"),
        "situs_in_jurisdiction" | "situs_in_south_africa" | "situs" => {
            Some("situs_in_jurisdiction")
        }
        "included_in_estate_duty" => Some("included_in_estate_duty"),
        "included_in_cgt_deemed_disposal" => Some("included_in_cgt_deemed_disposal"),
        "bequeathed_to_surviving_spouse" => Some("bequeathed_to_surviving_spouse"),
        "bequeathed_to_pbo" => Some("bequeathed_to_pbo"),
        "qualifies_primary_residence_exclusion" => Some("qualifies_primary_residence_exclusion"),
        _ => None,
    }
}

fn resolve_prefixed_asset_key(normalized_key: &str) -> Option<(usize, &'static str)> {
    if let Some(rest) = normalized_key.strip_prefix("asset_") {
        return resolve_indexed_asset_field(0, rest);
    }
    if let Some(rest) = normalized_key.strip_prefix("assets_") {
        let (index, field_key) = split_optional_index(rest);
        return resolve_indexed_asset_field(index, field_key);
    }
    if let Some(rest) = normalized_key.strip_prefix("asset") {
        let (index, field_key) = split_required_index(rest)?;
        return resolve_indexed_asset_field(index, field_key);
    }
    if let Some(rest) = normalized_key.strip_prefix("assets") {
        let (index, field_key) = split_required_index(rest)?;
        return resolve_indexed_asset_field(index, field_key);
    }
    None
}

fn resolve_indexed_asset_field(index: usize, field_key: &str) -> Option<(usize, &'static str)> {
    let field = resolve_asset_field(field_key)?;
    Some((index, field))
}

fn split_optional_index(raw: &str) -> (usize, &str) {
    if let Some((index, field)) = split_digits_and_field(raw) {
        return (normalize_asset_index(index), field);
    }
    (0, raw)
}

fn split_required_index(raw: &str) -> Option<(usize, &str)> {
    let (index, field) = split_digits_and_field(raw)?;
    Some((normalize_asset_index(index), field))
}

fn split_digits_and_field(raw: &str) -> Option<(usize, &str)> {
    let digit_count = raw.bytes().take_while(|byte| byte.is_ascii_digit()).count();
    if digit_count == 0 || raw.len() <= digit_count + 1 {
        return None;
    }

    let index = raw[..digit_count].parse::<usize>().ok()?;
    let field = raw[digit_count..].strip_prefix('_')?;
    if field.is_empty() {
        return None;
    }
    Some((index, field))
}

fn normalize_asset_index(index: usize) -> usize {
    if index > 0 {
        index - 1
    } else {
        0
    }
}

fn ensure_asset_slot(assets: &mut Vec<serde_json::Map<String, Value>>, index: usize) {
    while assets.len() <= index {
        assets.push(serde_json::Map::new());
    }
}

fn parse_structured_scenario_value(scenario_field: &str, raw_value: &str) -> Value {
    match scenario_field {
        "jurisdiction" => normalize_jurisdiction_value(raw_value)
            .map(|value| Value::String(value.to_string()))
            .unwrap_or_else(|| parse_structured_value(raw_value)),
        "taxpayer_class" => normalize_taxpayer_class_value(raw_value)
            .map(|value| Value::String(value.to_string()))
            .unwrap_or_else(|| parse_structured_value(raw_value)),
        "residency_status" => normalize_residency_status_value(raw_value)
            .map(|value| Value::String(value.to_string()))
            .unwrap_or_else(|| parse_structured_value(raw_value)),
        _ => parse_structured_value(raw_value),
    }
}

fn normalize_jurisdiction_value(raw_value: &str) -> Option<&'static str> {
    match normalize_structured_key(raw_value).as_str() {
        "south_africa" | "southafrica" | "za" => Some("SouthAfrica"),
        "us_new_york" | "new_york" | "newyork" | "newyoork" | "ny" => Some("UsNewYork"),
        "us_texas" | "texas" | "tx" => Some("UsTexas"),
        "us_california" | "california" | "calfornia" | "ca" => Some("UsCalifornia"),
        "us_florida" | "florida" | "fl" => Some("UsFlorida"),
        "us_minnesota" | "minnesota" | "mn" => Some("UsMinnesota"),
        _ => None,
    }
}

fn normalize_taxpayer_class_value(raw_value: &str) -> Option<&'static str> {
    match normalize_structured_key(raw_value).as_str() {
        "natural_person" | "naturalperson" | "individual" => Some("NaturalPerson"),
        "company" | "corporate" | "corporation" => Some("Company"),
        "trust" => Some("Trust"),
        "special_trust" | "specialtrust" => Some("SpecialTrust"),
        _ => None,
    }
}

fn normalize_residency_status_value(raw_value: &str) -> Option<&'static str> {
    match normalize_structured_key(raw_value).as_str() {
        "resident" => Some("Resident"),
        "non_resident" | "nonresident" => Some("NonResident"),
        _ => None,
    }
}

fn parse_structured_value(raw_value: &str) -> Value {
    let trimmed = raw_value.trim().trim_end_matches(',').trim();
    let unquoted = trimmed
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            trimmed
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(trimmed)
        .trim();

    if unquoted.eq_ignore_ascii_case("null")
        || unquoted.eq_ignore_ascii_case("none")
        || unquoted.eq_ignore_ascii_case("n_a")
        || unquoted.eq_ignore_ascii_case("n/a")
    {
        return Value::Null;
    }

    if unquoted.eq_ignore_ascii_case("true")
        || unquoted.eq_ignore_ascii_case("yes")
        || unquoted.eq_ignore_ascii_case("y")
    {
        return Value::Bool(true);
    }
    if unquoted.eq_ignore_ascii_case("false")
        || unquoted.eq_ignore_ascii_case("no")
        || unquoted.eq_ignore_ascii_case("n")
    {
        return Value::Bool(false);
    }

    if let Some(number) = parse_structured_number(unquoted) {
        return number;
    }

    Value::String(unquoted.to_string())
}

fn parse_structured_number(raw_value: &str) -> Option<Value> {
    let mut normalized = raw_value.trim().replace(',', "");
    if normalized.is_empty() {
        return None;
    }

    let mut negative = false;
    if normalized.starts_with('(') && normalized.ends_with(')') && normalized.len() > 2 {
        normalized = normalized[1..normalized.len() - 1].to_string();
        negative = true;
    }

    let upper = normalized.to_ascii_uppercase();
    for suffix in ["ZAR", "USD", "EUR", "GBP"] {
        if upper.ends_with(suffix) && normalized.len() > suffix.len() {
            let keep_len = normalized.len() - suffix.len();
            normalized = normalized[..keep_len].trim().to_string();
            break;
        }
    }

    normalized = normalized.trim_start_matches(['R', '$']).trim().to_string();
    normalized = normalized.replace([' ', '\t'], "");

    let percent = normalized.ends_with('%');
    if percent {
        normalized.pop();
        normalized = normalized.trim().to_string();
    }

    if normalized.is_empty() {
        return None;
    }

    if !percent && !normalized.contains('.') {
        if let Ok(integer_value) = normalized.parse::<i64>() {
            let signed_integer = if negative {
                integer_value.saturating_neg()
            } else {
                integer_value
            };
            return Some(Value::Number(signed_integer.into()));
        }
    }

    let value = normalized.parse::<f64>().ok()?;
    let signed = if negative { -value } else { value };
    let converted = if percent { signed / 100.0 } else { signed };
    serde_json::Number::from_f64(converted).map(Value::Number)
}

#[derive(Debug, Clone)]
struct NumericCandidate {
    value: f64,
    start: usize,
    end: usize,
}

fn parse_narrative_estate_document(document_content: &str) -> StructuredTextParseOutcome {
    let lines = document_content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return StructuredTextParseOutcome::NotDetected;
    }

    let mut scenario_value =
        serde_json::to_value(ApiEstateScenarioInput::from(EstateScenarioInput::default()))
            .expect("Default scenario should serialize");
    let Some(scenario_object) = scenario_value.as_object_mut() else {
        return StructuredTextParseOutcome::NotDetected;
    };

    let mut recognized_fields = 0usize;
    let mut inferred_assets: Vec<serde_json::Map<String, Value>> = Vec::new();

    if let Some(jurisdiction) = infer_jurisdiction_from_text(document_content) {
        scenario_object.insert(
            "jurisdiction".to_string(),
            Value::String(jurisdiction.to_string()),
        );
        recognized_fields += 1;
    }

    if let Some(tax_year) = infer_tax_year_from_text(document_content) {
        scenario_object.insert(
            "tax_year".to_string(),
            Value::Number(serde_json::Number::from(tax_year)),
        );
        recognized_fields += 1;
    }

    let mut asset_window_skip_until: Option<usize> = None;
    for index in 0..lines.len() {
        let line = lines[index];
        let lower = line.to_ascii_lowercase();
        if line_contains_any(
            &lower,
            &["marginal tax", "marginal_income_tax", "income tax rate"],
        ) {
            if let Some(rate) = extract_rate_from_line(line) {
                scenario_object.insert(
                    "marginal_income_tax_rate".to_string(),
                    serde_json::Number::from_f64(rate)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                );
                recognized_fields += 1;
            }
        }
        if line_contains_any(&lower, &["executor fee rate", "executor rate"]) {
            if let Some(rate) = extract_rate_from_line(line) {
                scenario_object.insert(
                    "executor_fee_rate".to_string(),
                    serde_json::Number::from_f64(rate)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                );
                recognized_fields += 1;
            }
        }
        if line_contains_any(&lower, &["vat rate", "value added tax"]) {
            if let Some(rate) = extract_rate_from_line(line) {
                scenario_object.insert(
                    "vat_rate".to_string(),
                    serde_json::Number::from_f64(rate)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                );
                recognized_fields += 1;
            }
        }

        let is_liability_line = line_contains_any(
            &lower,
            &[
                "debt",
                "loan",
                "liabilit",
                "mortgage bond",
                "bond amount",
                "home loan",
                "home loans division",
                "loan account",
            ],
        );
        if is_liability_line {
            if let Some(amount) = extract_amount_from_line(line) {
                scenario_object.insert(
                    "debts_and_loans_amount".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(amount)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
                recognized_fields += 1;
            }
        }
        if line_contains_any(&lower, &["funeral"]) {
            if let Some(amount) = extract_amount_from_line(line) {
                scenario_object.insert(
                    "funeral_costs_amount".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(amount)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
                recognized_fields += 1;
            }
        }
        if line_contains_any(&lower, &["administration cost", "admin cost"]) {
            if let Some(amount) = extract_amount_from_line(line) {
                scenario_object.insert(
                    "administration_costs_amount".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(amount)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
                recognized_fields += 1;
            }
        }
        if line_contains_any(&lower, &["master", "master's office"]) {
            if let Some(amount) = extract_amount_from_line(line) {
                scenario_object.insert(
                    "masters_office_fees_amount".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(amount)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
                recognized_fields += 1;
            }
        }
        if line_contains_any(&lower, &["conveyancing"]) {
            if let Some(amount) = extract_amount_from_line(line) {
                scenario_object.insert(
                    "conveyancing_costs_amount".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(amount)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
                recognized_fields += 1;
            }
        }
        if line_contains_any(
            &lower,
            &["final income tax", "final tax due", "income tax due"],
        ) {
            if let Some(amount) = extract_amount_from_line(line) {
                scenario_object.insert(
                    "final_income_tax_due_amount".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(amount)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
                recognized_fields += 1;
            }
        }
        if line_contains_any(&lower, &["cash reserve", "cash on hand"]) {
            if let Some(amount) = extract_amount_from_line(line) {
                scenario_object.insert(
                    "cash_reserve_amount".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(amount)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
                recognized_fields += 1;
            }
        }
        if line_contains_any(
            &lower,
            &[
                "external liquidity",
                "insurance payout",
                "life cover payout",
            ],
        ) {
            if let Some(amount) = extract_amount_from_line(line) {
                scenario_object.insert(
                    "external_liquidity_proceeds_amount".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(amount)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
                recognized_fields += 1;
            }
        }

        let should_attempt_asset_inference = asset_window_skip_until
            .map(|limit| index > limit)
            .unwrap_or(true);
        if should_attempt_asset_inference && !is_liability_line {
            let mut matched_asset: Option<(serde_json::Map<String, Value>, usize)> = None;
            for span in 1..=3usize {
                if index + span > lines.len() {
                    break;
                }
                let window = lines[index..index + span].join(" ");
                if let Some(asset) = infer_asset_from_line(&window, inferred_assets.len() + 1) {
                    matched_asset = Some((asset, span));
                    break;
                }
            }

            if let Some((asset, span)) = matched_asset {
                inferred_assets.push(asset);
                recognized_fields += 1;
                asset_window_skip_until = Some(index + span - 1);
            }
        }
    }

    if inferred_assets.is_empty() {
        if let Some(amount) = infer_primary_document_balance(&lines) {
            let mut asset = serde_json::Map::new();
            asset.insert(
                "name".to_string(),
                Value::String("Document-derived Liquid Asset".to_string()),
            );
            asset.insert(
                "market_value_amount".to_string(),
                Value::Number(
                    serde_json::Number::from_f64(amount)
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                ),
            );
            asset.insert(
                "base_cost_amount".to_string(),
                Value::Number(
                    serde_json::Number::from_f64(amount)
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                ),
            );
            asset.insert("is_liquid".to_string(), Value::Bool(true));
            asset.insert("situs_in_jurisdiction".to_string(), Value::Bool(true));
            asset.insert("included_in_estate_duty".to_string(), Value::Bool(true));
            asset.insert(
                "included_in_cgt_deemed_disposal".to_string(),
                Value::Bool(true),
            );
            asset.insert(
                "bequeathed_to_surviving_spouse".to_string(),
                Value::Bool(false),
            );
            asset.insert("bequeathed_to_pbo".to_string(), Value::Bool(false));
            asset.insert(
                "qualifies_primary_residence_exclusion".to_string(),
                Value::Bool(false),
            );
            inferred_assets.push(asset);
            recognized_fields += 1;
        }
    }

    if !inferred_assets.is_empty() {
        scenario_object.insert(
            "assets".to_string(),
            Value::Array(inferred_assets.into_iter().map(Value::Object).collect()),
        );
    }

    ingest_debug_log(&format!(
        "parse_narrative_estate_document recognized_fields={recognized_fields} inferred_assets={}",
        scenario_object
            .get("assets")
            .and_then(|value| value.as_array())
            .map(|assets| assets.len())
            .unwrap_or(0)
    ));

    if recognized_fields == 0 {
        return StructuredTextParseOutcome::NotDetected;
    }

    match serde_json::from_value::<ApiEstateScenarioInput>(scenario_value) {
        Ok(parsed) => StructuredTextParseOutcome::Parsed(vec![parsed]),
        Err(err) => StructuredTextParseOutcome::Invalid(ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: "Unable to parse narrative estate document into scenario".to_string(),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content".to_string(),
                message: format!("Could not map narrative fields to a scenario: {err}"),
            }],
        }),
    }
}

fn infer_jurisdiction_from_text(document_content: &str) -> Option<&'static str> {
    let lower = document_content.to_ascii_lowercase();
    if line_contains_any(&lower, &["south africa", "sars", "zar", "capitec"]) {
        return Some("SouthAfrica");
    }
    if line_contains_any(&lower, &["new york", "ny estate", "newyork"]) {
        return Some("UsNewYork");
    }
    if line_contains_any(&lower, &["texas", "tx estate"]) {
        return Some("UsTexas");
    }
    if line_contains_any(&lower, &["california", "ca estate"]) {
        return Some("UsCalifornia");
    }
    if line_contains_any(&lower, &["florida", "fl estate"]) {
        return Some("UsFlorida");
    }
    if line_contains_any(&lower, &["minnesota", "mn estate"]) {
        return Some("UsMinnesota");
    }
    None
}

fn infer_tax_year_from_text(document_content: &str) -> Option<u16> {
    let mut explicit_years = Vec::new();
    for line in document_content.lines() {
        let lower = line.to_ascii_lowercase();
        if !line_contains_any(
            &lower,
            &["tax year", "assessment year", "year of assessment"],
        ) {
            continue;
        }
        for numeric in extract_numeric_candidates(line) {
            if numeric.value.fract() != 0.0 {
                continue;
            }
            let year = numeric.value as i64;
            if (2018..=2100).contains(&year) {
                explicit_years.push(year as u16);
            }
        }
    }
    explicit_years.into_iter().max()
}

fn infer_asset_from_line(line: &str, next_index: usize) -> Option<serde_json::Map<String, Value>> {
    let lower = line.to_ascii_lowercase();
    if is_property_metadata_line(&lower) {
        return None;
    }
    let line_has_money_context = line_contains_any(
        &lower,
        &[
            "balance",
            "amount",
            "value",
            "market value",
            "total",
            "cost",
            "fee",
            "payable",
            "portfolio",
            "investment",
            "property",
            "asset",
            "cash",
        ],
    );
    let has_asset_hint = line_contains_any(
        &lower,
        &[
            "balance",
            "account",
            "portfolio",
            "investment",
            "property",
            "asset",
            "cash",
            "fund",
            "bond",
            "equity",
            "value",
        ],
    );
    if !has_asset_hint {
        return None;
    }
    if is_disclaimer_or_legal_line(&lower) {
        return None;
    }
    if line_contains_any(&lower, &["account number", "acc number", "account no"])
        && !line_contains_any(&lower, &["balance", "amount", "value"])
    {
        return None;
    }

    let amount_candidate = extract_amount_candidate_from_line(line)?;
    let amount_fragment = line
        .get(amount_candidate.start..amount_candidate.end)
        .unwrap_or("")
        .trim();
    if is_tiny_unformatted_property_number(&lower, amount_fragment, amount_candidate.value) {
        return None;
    }
    let amount_looks_money_like = fragment_looks_money_like(amount_fragment, &lower);
    if !line_has_money_context && !amount_looks_money_like {
        return None;
    }
    if fragment_looks_identifier_like(amount_fragment, &lower) && !line_has_money_context {
        return None;
    }

    if amount_candidate.value < 1.0 {
        return None;
    }
    if is_small_property_rate_or_reference(&lower, amount_candidate.value) {
        return None;
    }

    let name = infer_asset_name_from_line(line, &amount_candidate, next_index);
    let is_liquid = line_contains_any(
        &lower,
        &[
            "account",
            "balance",
            "cash",
            "deposit",
            "money market",
            "savings",
        ],
    );

    let mut asset = serde_json::Map::new();
    asset.insert("name".to_string(), Value::String(name));
    asset.insert(
        "market_value_amount".to_string(),
        Value::Number(
            serde_json::Number::from_f64(amount_candidate.value)
                .unwrap_or_else(|| serde_json::Number::from(0)),
        ),
    );
    asset.insert(
        "base_cost_amount".to_string(),
        Value::Number(
            serde_json::Number::from_f64(amount_candidate.value)
                .unwrap_or_else(|| serde_json::Number::from(0)),
        ),
    );
    asset.insert("is_liquid".to_string(), Value::Bool(is_liquid));
    asset.insert("situs_in_jurisdiction".to_string(), Value::Bool(true));
    asset.insert("included_in_estate_duty".to_string(), Value::Bool(true));
    asset.insert(
        "included_in_cgt_deemed_disposal".to_string(),
        Value::Bool(true),
    );
    asset.insert(
        "bequeathed_to_surviving_spouse".to_string(),
        Value::Bool(false),
    );
    asset.insert("bequeathed_to_pbo".to_string(), Value::Bool(false));
    asset.insert(
        "qualifies_primary_residence_exclusion".to_string(),
        Value::Bool(false),
    );
    Some(asset)
}

fn infer_asset_name_from_line(line: &str, amount: &NumericCandidate, next_index: usize) -> String {
    let lower = line.to_ascii_lowercase();
    if line_contains_any(
        &lower,
        &[
            "savings account",
            "global one",
            "cheque account",
            "current account",
            "available balance",
        ],
    ) {
        return "Bank Account Balance".to_string();
    }
    if line_contains_any(&lower, &["portfolio", "investment"]) {
        return "Investment Portfolio".to_string();
    }
    if line_contains_any(&lower, &["market value", "property valuation", "valuation report"]) {
        return "Property Market Value".to_string();
    }
    if line_contains_any(&lower, &["property", "immovable"]) {
        return "Property Asset".to_string();
    }
    if is_disclaimer_or_legal_line(&lower) {
        return format!("Document-derived Asset {next_index}");
    }

    if let Some((left, _)) = line.split_once(':') {
        let trimmed = left.trim();
        if is_reasonable_asset_name(trimmed) {
            return normalize_asset_name(trimmed);
        }
    }

    let mut name = String::new();
    if amount.start > 0 {
        name = line[..amount.start]
            .trim()
            .trim_matches('-')
            .trim()
            .to_string();
    }
    if name.is_empty() && amount.end < line.len() {
        name = line[amount.end..]
            .trim()
            .trim_matches('-')
            .trim()
            .to_string();
    }
    let normalized = normalize_asset_name(&name);
    if !is_reasonable_asset_name(&normalized) {
        format!("Document-derived Asset {next_index}")
    } else {
        normalized
    }
}

fn infer_primary_document_balance(lines: &[&str]) -> Option<f64> {
    for index in 0..lines.len() {
        let line = lines[index];
        let lower = line.to_ascii_lowercase();
        if !line_contains_any(&lower, &["balance", "amount", "total", "value"]) {
            continue;
        }

        if let Some(amount) = extract_amount_from_line(line) {
            if amount > 0.0 {
                return Some(amount);
            }
        }

        let max_end = (index + 5).min(lines.len());
        let window = lines[index..max_end].join(" ");
        if let Some(amount) = extract_amount_from_line(&window) {
            if amount > 0.0 {
                return Some(amount);
            }
        }
    }

    // Bank letters often split "available balance" and the amount onto separate lines.
    for index in 0..lines.len() {
        let max_end = (index + 4).min(lines.len());
        let window = lines[index..max_end].join(" ");
        let lower = window.to_ascii_lowercase();
        if !line_contains_any(&lower, &["balance", "account", "available"]) {
            continue;
        }
        if let Some(amount) = extract_amount_from_line(&window) {
            if amount > 0.0 {
                return Some(amount);
            }
        }
    }

    None
}

fn extract_rate_from_line(line: &str) -> Option<f64> {
    for candidate in extract_numeric_candidates(line) {
        if !candidate.value.is_finite() {
            continue;
        }
        let mut rate = candidate.value;
        if rate > 1.0 && rate <= 100.0 {
            rate /= 100.0;
        }
        if (0.0..=1.0).contains(&rate) {
            return Some(rate);
        }
    }
    None
}

fn extract_amount_from_line(line: &str) -> Option<f64> {
    extract_amount_candidate_from_line(line).map(|candidate| candidate.value)
}

fn extract_amount_candidate_from_line(line: &str) -> Option<NumericCandidate> {
    let lower = line.to_ascii_lowercase();
    let candidates = extract_numeric_candidates(line)
        .into_iter()
        .filter(|candidate| candidate.value.is_finite() && candidate.value > 0.0)
        .filter(|candidate| candidate.value < 1_000_000_000_000.0)
        .filter(|candidate| !(line.contains('/') && (1900.0..=2100.0).contains(&candidate.value)))
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        return None;
    }

    for candidate in candidates.iter().rev() {
        let fragment = line
            .get(candidate.start..candidate.end)
            .unwrap_or("")
            .trim();
        if fragment_looks_money_like(fragment, &lower)
            && !fragment_looks_identifier_like(fragment, &lower)
        {
            return Some(candidate.clone());
        }
    }

    if line_contains_any(
        &lower,
        &[
            "balance",
            "amount",
            "value",
            "market value",
            "total",
            "cost",
            "fee",
            "payable",
            "portfolio",
            "investment",
            "property",
        ],
    ) {
        for candidate in candidates.iter().rev() {
            let fragment = line
                .get(candidate.start..candidate.end)
                .unwrap_or("")
                .trim();
            if !fragment_looks_identifier_like(fragment, &lower) {
                return Some(candidate.clone());
            }
        }
    }

    None
}

fn extract_numeric_candidates(input: &str) -> Vec<NumericCandidate> {
    let mut candidates = Vec::new();
    let chars = input.char_indices().collect::<Vec<_>>();
    let mut index = 0usize;

    while index < chars.len() {
        let (start_offset, ch) = chars[index];
        if !ch.is_ascii_digit() {
            index += 1;
            continue;
        }

        let mut end_char_index = index + 1;
        while end_char_index < chars.len() {
            let (_, next_char) = chars[end_char_index];
            if next_char.is_ascii_digit()
                || matches!(next_char, ',' | '.' | '-' | '(' | ')' | '%' | ' ')
            {
                end_char_index += 1;
                continue;
            }
            if next_char.is_ascii_alphabetic() {
                end_char_index += 1;
                continue;
            }
            break;
        }

        let end_offset = if end_char_index < chars.len() {
            chars[end_char_index].0
        } else {
            input.len()
        };
        let raw = input[start_offset..end_offset].trim();
        if raw.is_empty() {
            index = end_char_index;
            continue;
        }

        if let Value::Number(number) = parse_structured_value(raw) {
            if let Some(value) = number.as_f64() {
                candidates.push(NumericCandidate {
                    value,
                    start: start_offset,
                    end: end_offset,
                });
            }
        }

        index = end_char_index;
    }

    candidates
}

fn line_contains_any(line: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| line.contains(needle))
}

fn fragment_looks_money_like(fragment: &str, lower_line: &str) -> bool {
    if fragment.is_empty() {
        return false;
    }
    let upper = fragment.to_ascii_uppercase();
    if upper.contains("ZAR")
        || upper.contains("USD")
        || upper.contains("EUR")
        || upper.contains("GBP")
    {
        return true;
    }
    if fragment.contains(',') || fragment.contains('.') {
        return true;
    }
    if fragment.contains(' ') && fragment.chars().any(|ch| ch.is_ascii_digit()) {
        return true;
    }
    line_contains_any(
        lower_line,
        &[
            "balance",
            "amount",
            "value",
            "market value",
            "total",
            "cost",
            "fee",
            "payable",
            "liability",
        ],
    )
}

fn fragment_has_explicit_money_marker(fragment: &str) -> bool {
    let upper = fragment.to_ascii_uppercase();
    upper.contains('R')
        || upper.contains('$')
        || upper.contains("ZAR")
        || upper.contains("USD")
        || upper.contains("EUR")
        || upper.contains("GBP")
        || fragment.contains(',')
        || fragment.contains('.')
}

fn fragment_looks_identifier_like(fragment: &str, lower_line: &str) -> bool {
    let stripped = fragment
        .chars()
        .filter(|ch| ch.is_ascii_digit())
        .collect::<String>();
    let has_only_digits = !stripped.is_empty()
        && fragment
            .chars()
            .all(|ch| ch.is_ascii_digit() || ch.is_ascii_whitespace());
    let has_money_punctuation = fragment.contains(',')
        || fragment.contains('.')
        || fragment.to_ascii_uppercase().contains("ZAR");
    let identifier_context = line_contains_any(
        lower_line,
        &[
            "account number",
            "account no",
            "acc number",
            "reference",
            "ref no",
            "id number",
            "branch",
            "device",
            "deed no",
            "title deed",
            "erf number",
            "valuer",
            "pr. val",
            "registration",
        ],
    );

    (has_only_digits && stripped.len() >= 7 && !has_money_punctuation)
        || (identifier_context && stripped.len() >= 6 && !has_money_punctuation)
}

fn is_disclaimer_or_legal_line(lower_line: &str) -> bool {
    line_contains_any(
        lower_line,
        &[
            "shall have no liability",
            "whether in contract",
            "delict",
            "negligence",
            "third party",
            "terms and conditions",
            "disclaimer",
            "to whom it may concern",
        ],
    )
}

fn is_tiny_unformatted_property_number(lower_line: &str, fragment: &str, amount: f64) -> bool {
    if amount >= 1_000.0 {
        return false;
    }

    if fragment_has_explicit_money_marker(fragment) {
        return false;
    }

    line_contains_any(
        lower_line,
        &[
            "section",
            "property",
            "valuation",
            "market value",
            "mortgage bond",
            "bond",
            "title deed",
        ],
    )
}

fn is_property_metadata_line(lower_line: &str) -> bool {
    let has_metadata_context = line_contains_any(
        lower_line,
        &[
            "deed no",
            "title deed",
            "erf number",
            "township",
            "extension",
            "issued:",
            "deeds registry",
            "valuer",
            "pr. val",
            "section 1",
            "section 2",
            "section 3",
            "section 4",
            "owner details",
            "property information",
        ],
    );
    let has_valuation_context = line_contains_any(
        lower_line,
        &[
            "market value",
            "property value",
            "valuation",
            "forced sale",
            "purchase price",
            "bond amount",
            "loan amount",
        ],
    );

    has_metadata_context && !has_valuation_context
}

fn is_small_property_rate_or_reference(lower_line: &str, amount: f64) -> bool {
    if amount <= 0.0 {
        return false;
    }

    if amount < 100_000.0
        && line_contains_any(
            lower_line,
            &[
                "m²",
                "sqm",
                "/m²",
                "per m²",
                "pr. val",
                "valuer",
                "deed no",
                "erf number",
            ],
        )
    {
        return true;
    }

    false
}

fn normalize_asset_name(input: &str) -> String {
    input
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_matches(&['-', ',', ';', ':', '.'][..])
        .to_string()
}

fn is_reasonable_asset_name(name: &str) -> bool {
    if name.trim().is_empty() {
        return false;
    }
    if name.len() > 80 {
        return false;
    }
    let lower = name.to_ascii_lowercase();
    if is_disclaimer_or_legal_line(&lower) {
        return false;
    }
    if line_contains_any(
        &lower,
        &[
            "proof of account details",
            "capitec bank limited",
            "one of the global one money management products or services",
        ],
    ) {
        return false;
    }
    true
}

fn extract_json_from_code_fence(document_content: &str) -> Option<String> {
    let mut cursor = 0usize;

    while let Some(open_rel) = document_content[cursor..].find("```") {
        let open = cursor + open_rel + 3;
        let rest = &document_content[open..];
        let Some(newline_rel) = rest.find('\n') else {
            break;
        };

        let language = rest[..newline_rel].trim().to_ascii_lowercase();
        let body_start = open + newline_rel + 1;
        let Some(close_rel) = document_content[body_start..].find("```") else {
            break;
        };

        let body_end = body_start + close_rel;
        let body = document_content[body_start..body_end].trim();

        if body.is_empty() {
            cursor = body_end + 3;
            continue;
        }

        if language.is_empty() || language.contains("json") {
            return Some(body.to_string());
        }

        cursor = body_end + 3;
    }

    None
}

fn extract_first_balanced_json(document_content: &str) -> Option<&str> {
    let mut start_index = None;
    for (index, ch) in document_content.char_indices() {
        if ch == '{' || ch == '[' {
            start_index = Some(index);
            break;
        }
    }

    let start = start_index?;
    let mut stack: Vec<char> = Vec::new();
    let mut in_string = false;
    let mut escaped = false;

    for (offset, ch) in document_content[start..].char_indices() {
        let absolute = start + offset;

        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' | '[' => stack.push(ch),
            '}' => {
                if stack.pop() != Some('{') {
                    return None;
                }
                if stack.is_empty() {
                    return Some(&document_content[start..=absolute]);
                }
            }
            ']' => {
                if stack.pop() != Some('[') {
                    return None;
                }
                if stack.is_empty() {
                    return Some(&document_content[start..=absolute]);
                }
            }
            _ => {}
        }
    }

    None
}

fn parse_csv_scenarios_document(
    document_content: &str,
) -> Result<Vec<ApiEstateScenarioInput>, ApiErrorResponse> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(document_content.as_bytes());

    let headers = reader
        .headers()
        .map_err(|err| ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: format!("Unable to read CSV headers: {err}"),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content".to_string(),
                message: "CSV documents must include a header row".to_string(),
            }],
        })?
        .clone();

    if headers.is_empty() {
        return Err(ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: "CSV document has no headers".to_string(),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content".to_string(),
                message: "Include CSV headers such as `scenario_json` or scenario fields"
                    .to_string(),
            }],
        });
    }

    let scenario_json_index = headers.iter().position(|header| {
        let normalized = header.trim().to_ascii_lowercase();
        normalized == "scenario_json" || normalized == "scenario"
    });

    let mut parsed = Vec::new();
    for (row_offset, record_result) in reader.records().enumerate() {
        let row_number = row_offset + 2; // Header is line 1.
        let record = record_result.map_err(|err| ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: format!("Unable to read CSV row {row_number}: {err}"),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content".to_string(),
                message: format!("CSV row {row_number} is malformed"),
            }],
        })?;

        if record.iter().all(|cell| cell.trim().is_empty()) {
            continue;
        }

        if let Some(index) = scenario_json_index {
            let scenario_payload = record.get(index).unwrap_or("").trim();
            if scenario_payload.is_empty() {
                continue;
            }

            let mut row_scenarios =
                parse_json_scenarios_document(scenario_payload).map_err(|_| ApiErrorResponse {
                    code: ApiErrorCode::Validation,
                    message: format!("Unable to parse `scenario_json` in CSV row {row_number}"),
                    validation_issues: vec![ApiValidationIssue {
                        field: format!("document_content.row_{row_number}.scenario_json"),
                        message: "Provide valid scenario JSON for this row".to_string(),
                    }],
                })?;
            parsed.append(&mut row_scenarios);
            continue;
        }

        parsed.push(parse_flat_csv_row_as_scenario(
            &headers, &record, row_number,
        )?);
    }

    if parsed.is_empty() {
        return Err(ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: "CSV document did not contain any scenarios".to_string(),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content".to_string(),
                message: "Include at least one populated CSV row".to_string(),
            }],
        });
    }

    Ok(parsed)
}

fn parse_flat_csv_row_as_scenario(
    headers: &csv::StringRecord,
    record: &csv::StringRecord,
    row_number: usize,
) -> Result<ApiEstateScenarioInput, ApiErrorResponse> {
    let mut scenario_value =
        serde_json::to_value(ApiEstateScenarioInput::from(EstateScenarioInput::default()))
            .expect("Default scenario should serialize");

    let scenario_object = scenario_value
        .as_object_mut()
        .expect("Default scenario should be an object");
    let mut asset_object = serde_json::Map::new();

    for (header, cell) in headers.iter().zip(record.iter()) {
        let key = header.trim();
        if key.is_empty() {
            continue;
        }

        let raw_value = cell.trim();
        if raw_value.is_empty() {
            continue;
        }

        if key.eq_ignore_ascii_case("scenario_json") || key.eq_ignore_ascii_case("scenario") {
            continue;
        }

        let parsed_cell = parse_csv_cell(raw_value);
        if let Some(asset_key) = key.strip_prefix("asset_") {
            asset_object.insert(asset_key.to_string(), parsed_cell);
            continue;
        }

        if key.eq_ignore_ascii_case("assets_json") || key.eq_ignore_ascii_case("assets") {
            scenario_object.insert("assets".to_string(), parsed_cell);
            continue;
        }

        scenario_object.insert(key.to_string(), parsed_cell);
    }

    if !asset_object.is_empty() {
        match scenario_object.get_mut("assets") {
            Some(Value::Array(assets)) => assets.push(Value::Object(asset_object)),
            _ => {
                scenario_object.insert(
                    "assets".to_string(),
                    Value::Array(vec![Value::Object(asset_object)]),
                );
            }
        }
    }

    serde_json::from_value::<ApiEstateScenarioInput>(scenario_value).map_err(|err| {
        ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: format!("Unable to parse CSV row {row_number} into scenario"),
            validation_issues: vec![ApiValidationIssue {
                field: format!("document_content.row_{row_number}"),
                message: format!("Ensure row values map to ApiEstateScenarioInput fields: {err}"),
            }],
        }
    })
}

fn parse_csv_cell(raw_value: &str) -> Value {
    if raw_value.eq_ignore_ascii_case("null") {
        return Value::Null;
    }

    if raw_value.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }

    if raw_value.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }

    if (raw_value.starts_with('{') && raw_value.ends_with('}'))
        || (raw_value.starts_with('[') && raw_value.ends_with(']'))
    {
        if let Ok(value) = serde_json::from_str::<Value>(raw_value) {
            return value;
        }
    }

    if let Ok(integer) = raw_value.parse::<i64>() {
        return Value::Number(integer.into());
    }

    if let Ok(float) = raw_value.parse::<f64>() {
        if let Some(number) = serde_json::Number::from_f64(float) {
            return Value::Number(number);
        }
    }

    Value::String(raw_value.to_string())
}

fn validate_scenarios_for_analysis(
    scenarios: &[ApiEstateScenarioInput],
) -> Result<(), ApiErrorResponse> {
    if scenarios.is_empty() {
        return Err(ApiErrorResponse {
            code: ApiErrorCode::Validation,
            message: "Document did not contain any scenarios".to_string(),
            validation_issues: vec![ApiValidationIssue {
                field: "document_content".to_string(),
                message: "Provide at least one scenario in the document".to_string(),
            }],
        });
    }

    let mut all_issues = Vec::new();
    for (index, scenario) in scenarios.iter().enumerate() {
        let domain: EstateScenarioInput = scenario.clone().into();
        if let Err(err) = domain.validate() {
            for mut issue in err.issues {
                issue.field = format!("scenarios[{index}].{}", issue.field);
                all_issues.push(issue);
            }
        }
    }

    if all_issues.is_empty() {
        return Ok(());
    }

    Err(ApiErrorResponse {
        code: ApiErrorCode::Validation,
        message: "Document parsed, but one or more scenarios failed validation".to_string(),
        validation_issues: all_issues
            .into_iter()
            .map(|issue| ApiValidationIssue {
                field: issue.field,
                message: issue.message,
            })
            .collect(),
    })
}
