use serde::Serialize;
use std::path::PathBuf;
use thiserror::Error;

pub const EXIT_OK: i32 = 0;
pub const EXIT_STEP_FAILED: i32 = 1;
pub const EXIT_ERROR: i32 = 2;

#[derive(Debug, Error)]
pub enum RunKeelError {
    #[error("Failed to read '{path}': {source}")]
    IoRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to write '{path}': {source}")]
    IoWrite {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to parse pipeline manifest '{path}': {detail}")]
    ManifestParse { path: String, detail: String },

    #[error("Duplicate step id '{id}' in pipeline manifest")]
    DuplicateStep { id: String },

    #[error("Step '{step}' depends on unknown step '{dep}'")]
    UnknownDependency { step: String, dep: String },

    #[error("Step '{step}' not found in pipeline (available: {available})")]
    UnknownStep { step: String, available: String },

    #[error("Dependency cycle detected in pipeline DAG involving: {nodes}")]
    CycleDetected { nodes: String },

    #[error("Invalid step configuration for '{step}': {detail}")]
    InvalidStep { step: String, detail: String },

    #[error("{0}")]
    General(String),
}

impl RunKeelError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::IoRead { .. } => "RK_IO_READ",
            Self::IoWrite { .. } => "RK_IO_WRITE",
            Self::ManifestParse { .. } => "RK_MANIFEST_PARSE",
            Self::DuplicateStep { .. } => "RK_DUPLICATE_STEP",
            Self::UnknownDependency { .. } => "RK_UNKNOWN_DEPENDENCY",
            Self::UnknownStep { .. } => "RK_UNKNOWN_STEP",
            Self::CycleDetected { .. } => "RK_CYCLE_DETECTED",
            Self::InvalidStep { .. } => "RK_INVALID_STEP",
            Self::General(_) => "RK_ERROR",
        }
    }

    pub fn fix_hint(&self) -> Option<String> {
        match self {
            Self::DuplicateStep { id } => Some(format!(
                "Give each `[[steps]]` entry a unique `id` (duplicate: '{id}')."
            )),
            Self::UnknownDependency { step, dep } => Some(format!(
                "Fix `depends_on` on step '{step}' — no step with id '{dep}' exists. Run `runkeel plan <manifest>` to inspect the DAG."
            )),
            Self::UnknownStep { available, .. } => Some(format!(
                "Use one of the defined step ids: {available}."
            )),
            Self::CycleDetected { .. } => Some(
                "Remove circular `depends_on` references so the pipeline forms a directed acyclic graph (DAG)."
                    .to_string(),
            ),
            _ => None,
        }
    }

    pub fn retryable(&self) -> bool {
        matches!(self, Self::IoRead { .. } | Self::IoWrite { .. })
    }

    pub fn to_envelope(&self) -> ErrorEnvelope {
        ErrorEnvelope {
            schema_version: "1.0",
            ok: false,
            error: ErrorBody {
                code: self.code(),
                message: self.to_string(),
                fix_hint: self.fix_hint(),
                retryable: self.retryable(),
            },
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorEnvelope {
    pub schema_version: &'static str,
    pub ok: bool,
    pub error: ErrorBody,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix_hint: Option<String>,
    pub retryable: bool,
}

pub type Result<T> = std::result::Result<T, RunKeelError>;
