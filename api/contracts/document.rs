use super::{ApiEstateScenarioInput, ApiScenarioResult};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum ApiScenarioDocumentFormat {
    Json,
    Txt,
    Csv,
    Docx,
    Pdf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ApiScenarioDocumentIngestRequest {
    pub format: ApiScenarioDocumentFormat,
    #[serde(default)]
    pub document_content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_content_base64: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ApiScenarioDocumentIngestResponse {
    pub scenarios: Vec<ApiEstateScenarioInput>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ApiScenarioDocumentCalculateResponse {
    pub scenarios: Vec<ApiEstateScenarioInput>,
    pub results: Vec<ApiScenarioResult>,
}
