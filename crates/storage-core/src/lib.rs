//! Trait for a cloud storage interface.

use std::{collections::HashMap, io, pin::Pin};

use async_trait::async_trait;
pub use bytes::Bytes;
pub use futures::stream::Stream;
use miette::Diagnostic;
pub use storage_types::BlobKey;

/// Type alias for streaming request data
pub type RequestStream =
  Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>>;
/// Type alias for streaming response data
pub type ResponseStream =
  Pin<Box<dyn Stream<Item = Result<Bytes, BlobStorageError>> + Send>>;

/// Metadata associated with a blob object
#[derive(Debug, Clone)]
pub struct BlobMetadata {
  /// Size of the blob in bytes
  pub size:             u64,
  /// Content encoding (compression, etc.)
  pub content_encoding: Option<String>,
  /// Content type (MIME type)
  pub content_type:     Option<String>,
  /// ETag for versioning/caching
  pub etag:             Option<String>,
  /// Last modified timestamp
  pub last_modified:    Option<String>,
  /// Custom metadata key-value pairs
  pub metadata:         HashMap<String, String>,
}

/// Options for uploading blobs
#[derive(Debug, Clone, Default)]
pub struct UploadOptions {
  /// Content type to set
  pub content_type: Option<String>,
  /// Whether to overwrite existing blob
  pub overwrite:    bool,
}

/// A single entry in a list operation
#[derive(Debug, Clone)]
pub struct BlobEntry {
  /// The blob's key
  pub key:           BlobKey,
  /// Size in bytes
  pub size:          u64,
  /// Last modified timestamp
  pub last_modified: Option<String>,
  /// ETag if available
  pub etag:          Option<String>,
}

/// Error types for blob storage operations
#[derive(Debug, thiserror::Error, Diagnostic)]
pub enum BlobStorageError {
  /// Blob not found.
  #[error("Blob not found: {0}")]
  NotFound(BlobKey),

  /// Permission denied.
  #[error("Permission denied: {0}")]
  PermissionDenied(miette::Report),

  /// Invalid config.
  #[error("Invalid configuration: {0}")]
  InvalidConfig(miette::Report),

  /// Network error.
  #[error("Network error: {0}")]
  NetworkError(miette::Report),

  /// IO error.
  #[error("IO error: {0}")]
  IoError(#[from] io::Error),

  /// Serialization error.
  #[error("Serialization error: {0}")]
  SerializationError(miette::Report),

  /// Stream error.
  #[error("Stream error: {0}")]
  StreamError(miette::Report),

  /// Unknown error.
  #[error("Unknown error: {0}")]
  Unknown(miette::Report),
}

/// A type alias for [`Result`] with [`BlobStorageError`].
pub type BlobStorageResult<T> = std::result::Result<T, BlobStorageError>;

/// Main trait for blob storage operations
#[async_trait]
pub trait BlobStorageLike: Send + Sync {
  /// Upload data from a stream to a blob
  async fn put_stream(
    &self,
    key: &str,
    data: RequestStream,
    options: UploadOptions,
  ) -> BlobStorageResult<()>;

  /// Download data from a blob as a stream
  async fn get_stream(
    &self,
    key: &BlobKey,
  ) -> BlobStorageResult<ResponseStream>;

  /// Get metadata for a blob without downloading content
  async fn head(&self, key: &BlobKey) -> BlobStorageResult<BlobMetadata>;

  /// Delete a blob
  async fn delete(&self, key: &BlobKey) -> BlobStorageResult<()>;

  /// Check if a blob exists
  async fn exists(&self, key: &BlobKey) -> BlobStorageResult<bool>;

  /// Copy a blob from one key to another
  async fn copy(
    &self,
    from_key: &BlobKey,
    to_key: &BlobKey,
  ) -> BlobStorageResult<()>;

  /// Get a pre-signed URL for temporary access (if supported)
  async fn get_presigned_url(
    &self,
    key: &BlobKey,
    expiry: std::time::Duration,
  ) -> BlobStorageResult<String>;
}
