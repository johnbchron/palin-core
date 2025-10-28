use miette::Diagnostic;
use thiserror::Error;

/// Errors that can occur during storage operations.
#[derive(Debug, Error, Diagnostic)]
pub enum StoreError {
  /// Record not found
  #[error("Record not found: {0}")]
  NotFound(String),

  /// Index not found
  #[error("Index not found: {0}")]
  IndexNotFound(String),

  /// Index not unique
  #[error("Index {0} is not unique")]
  IndexNotUnique(String),

  /// Uniqueness violation
  #[error("Unique constraint violation on index {index}: {value}")]
  UniqueViolation {
    /// The index whose uniqueness constraint was violated
    index: String,
    /// The duplicate value
    value: String,
  },

  /// Serialization error
  #[error("Serialization error: {0}")]
  Serialization(#[diagnostic_source] miette::Report),

  /// Database error
  #[error("Database error: {0}")]
  Database(#[diagnostic_source] miette::Report),

  /// Other error
  #[error("{0}")]
  Other(#[diagnostic_source] miette::Report),
}
