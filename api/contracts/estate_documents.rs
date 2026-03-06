use super::ApiScenarioDocumentFormat;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
pub enum ApiEstateDocumentType {
    DeathNoticeJ294,
    DeathCertificate,
    MarriageProofOrDeclaration,
    WillAndCodicils,
    IntestateSupportingDocument,
    NextOfKinAffidavitJ192,
    InventoryJ243,
    AcceptanceOfTrustJ190,
    LettersOfAuthorityJ155,
    DeclarationJ170,
    LettersOfExecutorshipJ238,
    SecurityBondJ262,
    EstateDutyReturnRev267,
    LiquidationAndDistributionAccount,
    NewEstateCaseSupportingPack,
    IncomeTaxReturnItr12,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum ApiEstateDocumentRequirementStatus {
    Satisfied,
    Missing,
    Conditional,
    NotApplicable,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ApiEstateDocumentInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_document_type: Option<ApiEstateDocumentType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_name: Option<String>,
    pub format: ApiScenarioDocumentFormat,
    #[serde(default)]
    pub document_content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_content_base64: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ApiEstateDocumentAnalysisRequest {
    pub documents: Vec<ApiEstateDocumentInput>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ApiEstateDocumentDetection {
    pub document_index: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_document_type: Option<ApiEstateDocumentType>,
    pub detected_document_types: Vec<ApiEstateDocumentType>,
    pub text_length: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_preview: Option<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ApiEstateDocumentChecklistItem {
    pub requirement_id: String,
    pub description: String,
    pub status: ApiEstateDocumentRequirementStatus,
    pub required_document_types: Vec<ApiEstateDocumentType>,
    pub matched_document_indices: Vec<usize>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ApiEstateDocumentAnalysisResponse {
    pub detections: Vec<ApiEstateDocumentDetection>,
    pub checklist: Vec<ApiEstateDocumentChecklistItem>,
    pub missing_required_document_types: Vec<ApiEstateDocumentType>,
    pub readiness_score: f64,
}
