//! Trait for a cloud storage interface.

use std::{collections::HashMap, fmt, io, pin::Pin};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::Stream;
use storage_types::BlobKey;

/// Type alias for streaming data
pub type ByteStream = Pin<Box<dyn Stream<Item = StorageResult<Bytes>> + Send>>;

/// Metadata associated with a blob object
#[derive(Debug, Clone)]
pub struct BlobMetadata {
  /// Size of the blob in bytes
  pub size:          u64,
  /// Content type (MIME type)
  pub content_type:  Option<String>,
  /// ETag for versioning/caching
  pub etag:          Option<String>,
  /// Last modified timestamp
  pub last_modified: Option<DateTime<Utc>>,
  /// Custom metadata key-value pairs
  pub metadata:      HashMap<String, String>,
}

/// Options for uploading blobs
#[derive(Debug, Clone, Default)]
pub struct UploadOptions {
  /// Content type to set
  pub content_type: Option<String>,
  /// Custom metadata
  pub metadata:     Option<HashMap<String, String>>,
  /// Whether to overwrite existing blob
  pub overwrite:    bool,
}

/// Options for listing blobs
#[derive(Debug, Clone, Default)]
pub struct ListOptions {
  /// Prefix to filter by
  pub prefix:             Option<String>,
  /// Maximum number of results
  pub max_results:        Option<usize>,
  /// Continuation token for pagination
  pub continuation_token: Option<String>,
}

/// A single entry in a list operation
#[derive(Debug, Clone)]
pub struct BlobEntry {
  /// The blob's key
  pub key:           BlobKey,
  /// Size in bytes
  pub size:          u64,
  /// Last modified timestamp
  pub last_modified: Option<DateTime<Utc>>,
  /// ETag if available
  pub etag:          Option<String>,
}

/// Result of a list operation
pub struct ListResult {
  /// Stream of blob entries
  pub entries: Pin<Box<dyn Stream<Item = StorageResult<BlobEntry>> + Send>>,
  /// Continuation token for next page (if pagination is handled manually)
  pub continuation_token: Option<String>,
}

impl fmt::Debug for ListResult {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ListResult")
      .field("entries", &format_args!("_"))
      .field("continuation_token", &self.continuation_token)
      .finish()
  }
}

/// Error types for blob storage operations
#[derive(Debug, thiserror::Error)]
pub enum BlobStorageError {
  /// Blob not found.
  #[error("Blob not found: {0}")]
  NotFound(BlobKey),

  /// Permission denied.
  #[error("Permission denied: {0}")]
  PermissionDenied(String),

  /// Invalid config.
  #[error("Invalid configuration: {0}")]
  InvalidConfig(String),

  /// Network error.
  #[error("Network error: {0}")]
  NetworkError(String),

  /// IO error.
  #[error("IO error: {0}")]
  IoError(#[from] io::Error),

  /// Serialization error.
  #[error("Serialization error: {0}")]
  SerializationError(String),

  /// Stream error.
  #[error("Stream error: {0}")]
  StreamError(String),

  /// Unknown error.
  #[error("Unknown error: {0}")]
  Unknown(String),
}

/// A type alias for [`Result`] with [`BlobStorageError`].
pub type StorageResult<T> = std::result::Result<T, BlobStorageError>;

/// Main trait for blob storage operations
#[async_trait]
pub trait BlobStorage: Send + Sync {
  /// Upload data from a stream to a blob
  async fn put_stream(
    &self,
    key: &str,
    data: ByteStream,
    options: UploadOptions,
  ) -> StorageResult<()>;

  /// Download data from a blob as a stream
  async fn get_stream(&self, key: &BlobKey) -> StorageResult<ByteStream>;

  /// Get metadata for a blob without downloading content
  async fn head(&self, key: &BlobKey) -> StorageResult<BlobMetadata>;

  /// Delete a blob
  async fn delete(&self, key: &BlobKey) -> StorageResult<()>;

  /// Check if a blob exists
  async fn exists(&self, key: &BlobKey) -> StorageResult<bool>;

  /// List blobs with optional filtering, returns a stream of entries
  async fn list(&self, options: ListOptions) -> StorageResult<ListResult>;

  /// Copy a blob from one key to another
  async fn copy(
    &self,
    from_key: &BlobKey,
    to_key: &BlobKey,
  ) -> StorageResult<()>;

  /// Get a pre-signed URL for temporary access (if supported)
  async fn get_presigned_url(
    &self,
    key: &BlobKey,
    expiry: std::time::Duration,
  ) -> StorageResult<String>;
}
