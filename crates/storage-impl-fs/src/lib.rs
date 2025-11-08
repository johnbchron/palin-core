//! Filesystem-based implementation of the blob storage interface.

use std::{
  fmt,
  path::{Path, PathBuf},
  time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures::TryStreamExt;
use storage_core::{
  BlobKey, BlobMetadata, BlobStorageError, BlobStorageLike, BlobStorageResult,
  RequestStream, ResponseStream, UploadOptions,
};
use tokio::fs;
use tracing::{debug, error, info, instrument, warn};

/// Filesystem-based implementation of [`BlobStorageLike`].
///
/// This implementation stores all blobs as files in a directory structure.
/// Each blob is stored with its metadata in a sidecar file.
#[derive(Debug, Clone)]
pub struct BlobStorageFilesystem {
  /// Root directory for blob storage
  root_path: PathBuf,
}

impl BlobStorageFilesystem {
  /// Creates a new filesystem-based blob storage.
  ///
  /// # Arguments
  /// * `root_path` - The root directory where blobs will be stored
  ///
  /// # Errors
  /// Returns an error if the directory cannot be created.
  #[instrument]
  pub async fn new<P: AsRef<Path> + fmt::Debug>(
    root_path: P,
  ) -> BlobStorageResult<Self> {
    let root_path = root_path.as_ref().to_path_buf();

    info!(path = ?root_path, "Creating filesystem blob storage");

    // Create the root directory if it doesn't exist
    fs::create_dir_all(&root_path).await.map_err(|e| {
      error!(error = ?e, "Failed to create root directory");
      BlobStorageError::IoError(e)
    })?;

    Ok(Self { root_path })
  }

  /// Returns the file path for a given blob key
  fn blob_path(&self, key: &BlobKey) -> PathBuf {
    self.root_path.join(key.as_str())
  }

  /// Returns the metadata file path for a given blob key
  fn metadata_path(&self, key: &BlobKey) -> PathBuf {
    self.root_path.join(format!("{}.meta", key.as_str()))
  }

  /// Computes the MD5 hash of data
  fn compute_etag(data: &[u8]) -> String { format!("{:x}", md5::compute(data)) }

  /// Gets the current timestamp in ISO 8601 format
  fn current_timestamp() -> String {
    let now = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_secs();
    chrono::DateTime::from_timestamp(i64::try_from(now).unwrap(), 0)
      .unwrap()
      .to_rfc3339()
  }

  /// Reads metadata from a metadata file
  async fn read_metadata(
    &self,
    key: &BlobKey,
  ) -> BlobStorageResult<BlobMetadata> {
    let metadata_path = self.metadata_path(key);

    let content = fs::read_to_string(&metadata_path).await.map_err(|e| {
      error!(error = ?e, path = ?metadata_path, "Failed to read metadata file");
      BlobStorageError::IoError(e)
    })?;

    let metadata: BlobMetadata =
      serde_json::from_str(&content).map_err(|e| {
        error!(error = ?e, "Failed to parse metadata JSON");
        BlobStorageError::InvalidInput(miette::miette!(
          "Failed to parse metadata: {}",
          e
        ))
      })?;

    Ok(metadata)
  }

  /// Writes metadata to a metadata file
  async fn write_metadata(
    &self,
    key: &BlobKey,
    metadata: &BlobMetadata,
  ) -> BlobStorageResult<()> {
    let metadata_path = self.metadata_path(key);

    let content = serde_json::to_string(metadata).map_err(|e| {
      error!(error = ?e, "Failed to serialize metadata");
      BlobStorageError::InvalidInput(miette::miette!(
        "Failed to serialize metadata: {}",
        e
      ))
    })?;

    fs::write(&metadata_path, content).await.map_err(|e| {
      error!(error = ?e, path = ?metadata_path, "Failed to write metadata file");
      BlobStorageError::IoError(e)
    })?;

    Ok(())
  }
}

#[async_trait::async_trait]
impl BlobStorageLike for BlobStorageFilesystem {
  #[instrument(
    skip(self, data),
    fields(
      key = %key,
      overwrite = options.overwrite,
    ),
    err
  )]
  async fn put_stream(
    &self,
    key: &BlobKey,
    data: RequestStream,
    options: UploadOptions,
  ) -> BlobStorageResult<()> {
    debug!("Starting stream upload");

    let blob_path = self.blob_path(key);

    // Check if we should overwrite
    if !options.overwrite {
      debug!("Checking if blob already exists");
      if blob_path.exists() {
        warn!("Blob already exists and overwrite=false");
        return Err(BlobStorageError::AlreadyExists(key.clone()));
      }
      debug!("Blob does not exist, proceeding with upload");
    }

    // Create parent directories if they don't exist
    if let Some(parent) = blob_path.parent() {
      fs::create_dir_all(parent).await.map_err(|e| {
        error!(error = ?e, "Failed to create parent directories");
        BlobStorageError::IoError(e)
      })?;
    }

    // Collect the stream into a single Bytes object
    debug!("Collecting stream chunks");
    let chunks: Vec<Bytes> = data.try_collect().await.map_err(|e| {
      error!(error = ?e, "Failed to collect stream chunks");
      BlobStorageError::StreamError(miette::miette!(e))
    })?;

    let chunk_count = chunks.len();
    debug!(chunk_count = chunk_count, "Collected stream chunks");

    // Combine all chunks
    let total_size: usize = chunks.iter().map(Bytes::len).sum();
    let mut combined = Vec::with_capacity(total_size);
    for chunk in chunks {
      combined.extend_from_slice(&chunk);
    }

    debug!(total_size = total_size, "Combined chunks into single blob");

    // Compute metadata
    let etag = Self::compute_etag(&combined);
    let last_modified = Self::current_timestamp();

    // Write the blob to disk
    fs::write(&blob_path, &combined).await.map_err(|e| {
      error!(error = ?e, path = ?blob_path, "Failed to write blob file");
      BlobStorageError::IoError(e)
    })?;

    // Write metadata
    let metadata = BlobMetadata {
      size:          total_size as u64,
      etag:          Some(etag),
      last_modified: Some(last_modified),
    };

    self.write_metadata(key, &metadata).await?;

    info!(
      size = total_size,
      chunk_count = chunk_count,
      "Blob uploaded successfully"
    );

    Ok(())
  }

  #[instrument(
    skip(self),
    fields(key = %key),
    err
  )]
  async fn get_stream(
    &self,
    key: &BlobKey,
  ) -> BlobStorageResult<ResponseStream> {
    debug!("Retrieving blob stream");

    let blob_path = self.blob_path(key);

    // Check if file exists
    if !blob_path.exists() {
      error!("Blob not found");
      return Err(BlobStorageError::NotFound(key.clone()));
    }

    // Read the entire file into memory
    let data = fs::read(&blob_path).await.map_err(|e| {
      error!(error = ?e, path = ?blob_path, "Failed to read blob file");
      BlobStorageError::IoError(e)
    })?;

    let data_size = data.len();
    debug!(size = data_size, "Retrieved blob data");

    // Create a stream that yields the data
    let bytes = Bytes::from(data);
    let stream = futures::stream::once(async move { Ok(bytes) });

    info!(size = data_size, "Blob stream created successfully");

    Ok(Box::pin(stream))
  }

  #[instrument(
    skip(self),
    fields(key = %key),
    err
  )]
  async fn head(
    &self,
    key: &BlobKey,
  ) -> BlobStorageResult<Option<BlobMetadata>> {
    debug!("Fetching blob metadata");

    let blob_path = self.blob_path(key);

    if !blob_path.exists() {
      debug!("Blob not found");
      return Ok(None);
    }

    // Try to read metadata file
    let metadata = if let Ok(meta) = self.read_metadata(key).await {
      meta
    } else {
      // Fallback: compute metadata from file
      warn!("Metadata file not found, computing from file");
      let file_metadata = fs::metadata(&blob_path).await.map_err(|e| {
        error!(error = ?e, path = ?blob_path, "Failed to read file metadata");
        BlobStorageError::IoError(e)
      })?;

      BlobMetadata {
        size:          file_metadata.len(),
        etag:          None,
        last_modified: None,
      }
    };

    info!(
      size = metadata.size,
      etag = ?metadata.etag,
      "Blob metadata retrieved successfully"
    );

    Ok(Some(metadata))
  }

  #[instrument(
    skip(self),
    fields(key = %key),
    err
  )]
  async fn delete(&self, key: &BlobKey) -> BlobStorageResult<()> {
    debug!("Deleting blob");

    let blob_path = self.blob_path(key);
    let metadata_path = self.metadata_path(key);

    if !blob_path.exists() {
      error!("Blob not found");
      return Err(BlobStorageError::NotFound(key.clone()));
    }

    // Get size before deletion for logging
    let size = fs::metadata(&blob_path).await.map(|m| m.len()).unwrap_or(0);

    // Delete blob file
    fs::remove_file(&blob_path).await.map_err(|e| {
      error!(error = ?e, path = ?blob_path, "Failed to delete blob file");
      BlobStorageError::IoError(e)
    })?;

    // Delete metadata file (ignore errors if it doesn't exist)
    let _ = fs::remove_file(&metadata_path).await;

    info!(size = size, "Blob deleted successfully");

    Ok(())
  }

  #[instrument(
    skip(self),
    fields(
      key = %key,
      expiry_secs = expiry.as_secs(),
    ),
    err
  )]
  async fn get_presigned_url(
    &self,
    key: &BlobKey,
    expiry: std::time::Duration,
  ) -> BlobStorageResult<String> {
    let expiry_secs = expiry.as_secs();
    debug!(expiry_secs = expiry_secs, "Generating presigned URL");

    // Validate duration (same as S3 implementation for consistency)
    if expiry_secs > u64::from(u32::MAX) {
      error!(
        expiry_secs = expiry_secs,
        max_secs = u32::MAX,
        "Expiry duration exceeds maximum"
      );
      return Err(BlobStorageError::InvalidInput(miette::miette!(
        "Expiry duration of {} seconds exceeds maximum supported duration of \
         {} seconds (~136 years)",
        expiry_secs,
        u32::MAX
      )));
    }

    // Check if the blob exists
    debug!("Checking if blob exists for presigned URL");
    let blob_path = self.blob_path(key);
    if !blob_path.exists() {
      error!("Blob not found");
      return Err(BlobStorageError::NotFound(key.clone()));
    }

    // For filesystem storage, presigned URLs would typically be served
    // by a web server. Here we return a file:// URL for local access.
    let url = format!("file://{}", blob_path.display());

    info!(
      expiry_secs = expiry_secs,
      url_length = url.len(),
      "Presigned URL generated successfully"
    );

    Ok(url)
  }
}

#[cfg(test)]
mod tests {
  use futures::stream;
  use tempfile::TempDir;

  use super::*;

  #[tokio::test]
  async fn test_put_and_get() {
    let temp_dir = TempDir::new().unwrap();
    let storage = BlobStorageFilesystem::new(temp_dir.path()).await.unwrap();

    let key = BlobKey::new("test-blob".to_string());
    let data = Bytes::from("hello world");
    let stream = Box::pin(stream::once(async move { Ok(data) }));

    storage
      .put_stream(&key, stream, UploadOptions { overwrite: true })
      .await
      .unwrap();

    let result_stream = storage.get_stream(&key).await.unwrap();
    let result: Vec<Bytes> = result_stream.try_collect().await.unwrap();
    let combined: Vec<u8> =
      result.iter().flat_map(|b| b.iter()).copied().collect();

    assert_eq!(combined, b"hello world");
  }

  #[tokio::test]
  async fn test_head() {
    let temp_dir = TempDir::new().unwrap();
    let storage = BlobStorageFilesystem::new(temp_dir.path()).await.unwrap();

    let key = BlobKey::new("test-blob".to_string());
    let data = Bytes::from("hello world");
    let stream = Box::pin(stream::once(async move { Ok(data) }));

    storage
      .put_stream(&key, stream, UploadOptions { overwrite: true })
      .await
      .unwrap();

    let metadata = storage.head(&key).await.unwrap().unwrap();
    assert_eq!(metadata.size, 11);
    assert!(metadata.etag.is_some());
    assert!(metadata.last_modified.is_some());
  }

  #[tokio::test]
  async fn test_delete() {
    let temp_dir = TempDir::new().unwrap();
    let storage = BlobStorageFilesystem::new(temp_dir.path()).await.unwrap();

    let key = BlobKey::new("test-blob".to_string());
    let data = Bytes::from("hello world");
    let stream = Box::pin(stream::once(async move { Ok(data) }));

    storage
      .put_stream(&key, stream, UploadOptions { overwrite: true })
      .await
      .unwrap();

    storage.delete(&key).await.unwrap();

    let result = storage.head(&key).await.unwrap();
    assert!(result.is_none());
  }
}
