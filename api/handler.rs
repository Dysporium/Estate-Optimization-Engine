use crate::api::contracts::{
    ApiErrorCode, ApiErrorResponse, ApiEstateScenarioInput, ApiJurisdiction,
    ApiJurisdictionTaxRuleRegistryResponse, ApiOptimizedScenario,
    ApiScenarioDocumentCalculateResponse, ApiScenarioDocumentFormat,
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

fn parse_scenarios_from_document(
    request: &ApiScenarioDocumentIngestRequest,
) -> Result<Vec<ApiEstateScenarioInput>, ApiErrorResponse> {
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
    let encoded = request
        .document_content_base64
        .as_deref()
        .unwrap_or(&request.document_content)
        .trim();

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
        encoded.split_once(',').map(|(_, body)| body).unwrap_or(encoded)
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
    let bytes = require_binary_document_bytes(request, "PDF")?;
    let extracted_text = extract_text_from_pdf(&bytes)?;
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

    let mut xml_file = archive.by_name("word/document.xml").map_err(|_| ApiErrorResponse {
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
    let document = lopdf::Document::load_mem(bytes).map_err(|_| ApiErrorResponse {
        code: ApiErrorCode::Validation,
        message: "Unable to parse PDF document".to_string(),
        validation_issues: vec![ApiValidationIssue {
            field: "document_content_base64".to_string(),
            message: "Uploaded bytes are not a valid PDF file".to_string(),
        }],
    })?;

    let pages = document.get_pages();
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
                message: "Ensure the PDF contains selectable text, not image-only scans".to_string(),
            }],
        })?;

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
    if let Ok(scenarios) = parse_json_scenarios_document(document_content) {
        return Ok(scenarios);
    }

    if let Some(block) = extract_json_from_code_fence(document_content) {
        if let Ok(scenarios) = parse_json_scenarios_document(&block) {
            return Ok(scenarios);
        }
    }

    if let Some(candidate) = extract_first_balanced_json(document_content) {
        if let Ok(scenarios) = parse_json_scenarios_document(candidate) {
            return Ok(scenarios);
        }
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
                message: "Include CSV headers such as `scenario_json` or scenario fields".to_string(),
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
            &headers,
            &record,
            row_number,
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
                message: format!(
                    "Ensure row values map to ApiEstateScenarioInput fields: {err}"
                ),
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
