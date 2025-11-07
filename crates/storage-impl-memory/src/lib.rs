//! In-memory implementation of the blob storage interface.

use std::{
  collections::HashMap,
  sync::Arc,
  time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures::{TryStreamExt, stream};
use storage_core::{
  BlobEntry, BlobKey, BlobMetadata, BlobStorageError, BlobStorageLike,
  BlobStorageResult, RequestStream, ResponseStream, UploadOptions,
};
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument, warn};

/// Internal representation of a stored blob
#[derive(Debug, Clone)]
struct StoredBlob {
  /// The actual blob data
  data:          Bytes,
  /// `ETag` (generated from content hash)
  etag:          String,
  /// Last modified timestamp
  last_modified: String,
}

impl StoredBlob {
  fn new(data: Bytes) -> Self {
    let etag = format!("{:x}", md5::compute(&data));
    let last_modified = Self::current_timestamp();

    debug!(
      data_size = data.len(),
      etag = %etag,
      "Created new stored blob"
    );

    Self {
      data,
      etag,
      last_modified,
    }
  }

  fn current_timestamp() -> String {
    let now = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_secs();
    // ISO 8601 format
    chrono::DateTime::from_timestamp(i64::try_from(now).unwrap(), 0)
      .unwrap()
      .to_rfc3339()
  }

  fn metadata(&self) -> BlobMetadata {
    BlobMetadata {
      size:          self.data.len() as u64,
      etag:          Some(self.etag.clone()),
      last_modified: Some(self.last_modified.clone()),
    }
  }
}

/// In-memory implementation of [`BlobStorageLike`].
///
/// This implementation stores all blobs in memory and is useful for
/// testing and development. All data is lost when the instance is dropped.
#[derive(Debug, Clone)]
pub struct BlobStorageMemory {
  storage: Arc<RwLock<HashMap<String, StoredBlob>>>,
}

impl BlobStorageMemory {
  /// Creates a new empty in-memory blob storage.
  #[instrument]
  pub fn new() -> Self {
    info!("Creating new in-memory blob storage");
    Self {
      storage: Arc::new(RwLock::new(HashMap::new())),
    }
  }

  /// Returns the number of blobs currently stored.
  #[instrument(skip(self))]
  pub async fn len(&self) -> usize {
    let len = self.storage.read().await.len();
    debug!(blob_count = len, "Retrieved blob count");
    len
  }

  /// Returns true if no blobs are stored.
  #[instrument(skip(self))]
  pub async fn is_empty(&self) -> bool {
    let empty = self.storage.read().await.is_empty();
    debug!(is_empty = empty, "Checked if storage is empty");
    empty
  }

  /// Clears all stored blobs.
  #[instrument(skip(self))]
  pub async fn clear(&self) {
    let count = self.storage.read().await.len();
    self.storage.write().await.clear();
    info!(cleared_count = count, "Cleared all blobs from storage");
  }

  /// Lists all blob keys with optional prefix filter.
  #[instrument(skip(self), fields(prefix = ?prefix))]
  pub async fn list(
    &self,
    prefix: Option<&str>,
  ) -> BlobStorageResult<Vec<BlobEntry>> {
    debug!("Listing blobs");

    let storage = self.storage.read().await;
    let total_blobs = storage.len();

    let mut entries: Vec<BlobEntry> = storage
      .iter()
      .filter(|(key, _)| {
        if let Some(prefix) = prefix {
          key.starts_with(prefix)
        } else {
          true
        }
      })
      .map(|(key, blob)| BlobEntry {
        key:           BlobKey::new(key.clone()),
        size:          blob.data.len() as u64,
        last_modified: Some(blob.last_modified.clone()),
        etag:          Some(blob.etag.clone()),
      })
      .collect();

    // Sort by key for consistent ordering
    entries.sort_by(|a, b| a.key.as_str().cmp(b.key.as_str()));

    info!(
      total_blobs = total_blobs,
      filtered_count = entries.len(),
      prefix = ?prefix,
      "Listed blobs successfully"
    );

    Ok(entries)
  }
}

impl Default for BlobStorageMemory {
  fn default() -> Self { Self::new() }
}

#[async_trait::async_trait]
impl BlobStorageLike for BlobStorageMemory {
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

    // Check if we should overwrite
    if !options.overwrite {
      debug!("Checking if blob already exists");
      let storage = self.storage.read().await;
      if storage.contains_key(key.as_str()) {
        warn!("Blob already exists and overwrite=false");
        return Err(BlobStorageError::AlreadyExists(key.clone()));
      }
      debug!("Blob does not exist, proceeding with upload");
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

    let blob = StoredBlob::new(Bytes::from(combined));

    // Store the blob
    self
      .storage
      .write()
      .await
      .insert(key.as_str().to_string(), blob);

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

    let storage = self.storage.read().await;
    let blob = storage.get(key.as_str()).ok_or_else(|| {
      error!("Blob not found");
      BlobStorageError::NotFound(key.clone())
    })?;

    let data = blob.data.clone();
    let data_size = data.len();

    debug!(size = data_size, "Retrieved blob data");

    // Create a stream that yields the data in chunks
    // For simplicity, we'll just yield the entire blob as one chunk
    let stream = stream::once(async move { Ok(data) });

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

    let storage = self.storage.read().await;
    let blob = storage.get(key.as_str());

    let metadata = blob.map(StoredBlob::metadata);

    if let Some(ref metadata) = metadata {
      info!(
        size = metadata.size,
        etag = ?metadata.etag,
        "Blob metadata retrieved successfully"
      );
    }

    Ok(metadata)
  }

  #[instrument(
    skip(self),
    fields(key = %key),
    err
  )]
  async fn delete(&self, key: &BlobKey) -> BlobStorageResult<()> {
    debug!("Deleting blob");

    let mut storage = self.storage.write().await;
    let blob = storage.remove(key.as_str()).ok_or_else(|| {
      error!("Blob not found");
      BlobStorageError::NotFound(key.clone())
    })?;

    info!(size = blob.data.len(), "Blob deleted successfully");

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
    let storage = self.storage.read().await;
    if !storage.contains_key(key.as_str()) {
      error!("Blob not found");
      return Err(BlobStorageError::NotFound(key.clone()));
    }

    // For in-memory storage, presigned URLs don't make much sense,
    // but we can return a fake URL for testing purposes
    let url = format!("memory://blob/{}", key.as_str());

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
  use futures::{stream, stream::StreamExt};

  use super::*;

  #[tokio::test]
  async fn test_put_and_get() {
    let storage = BlobStorageMemory::new();
    let key = BlobKey::new("test-key");
    let data = Bytes::from("hello world");

    // Upload
    let stream = Box::pin(stream::once({
      let data = data.clone();
      async move { Ok(data.clone()) }
    }));

    storage
      .put_stream(&key, stream, UploadOptions { overwrite: true })
      .await
      .unwrap();

    // Download
    let mut result_stream = storage.get_stream(&key).await.unwrap();
    let result = result_stream.next().await.unwrap().unwrap();

    assert_eq!(result, data);
  }

  #[tokio::test]
  async fn test_head() {
    let storage = BlobStorageMemory::new();
    let key = BlobKey::new("test-key");
    let data = Bytes::from("hello world");

    let stream = Box::pin(stream::once({
      let data = data.clone();
      async move { Ok(data.clone()) }
    }));
    storage
      .put_stream(&key, stream, UploadOptions { overwrite: true })
      .await
      .unwrap();

    let metadata = storage.head(&key).await.unwrap().unwrap();
    assert_eq!(metadata.size, 11);
  }

  #[tokio::test]
  async fn test_delete() {
    let storage = BlobStorageMemory::new();
    let key = BlobKey::new("test-key");
    let data = Bytes::from("hello world");

    let stream = Box::pin(stream::once(async { Ok(data) }));
    storage
      .put_stream(&key, stream, UploadOptions::default())
      .await
      .unwrap();

    assert!(storage.head(&key).await.unwrap().is_some());
    storage.delete(&key).await.unwrap();
    assert!(storage.head(&key).await.unwrap().is_none());
  }

  #[tokio::test]
  async fn test_list() {
    let storage = BlobStorageMemory::new();

    // Add some blobs
    for i in 0..5 {
      let key = BlobKey::new(format!("prefix/file{i}"));
      let data = Bytes::from(format!("data{i}"));
      let stream = Box::pin(stream::once(async move { Ok(data) }));
      storage
        .put_stream(&key, stream, UploadOptions::default())
        .await
        .unwrap();
    }

    // List all
    let all = storage.list(None).await.unwrap();
    assert_eq!(all.len(), 5);

    // List with prefix
    let filtered = storage.list(Some("prefix/")).await.unwrap();
    assert_eq!(filtered.len(), 5);

    let empty = storage.list(Some("other/")).await.unwrap();
    assert_eq!(empty.len(), 0);
  }

  #[tokio::test]
  async fn test_overwrite_false() {
    let storage = BlobStorageMemory::new();
    let key = BlobKey::new("test-key");
    let data1 = Bytes::from("first");
    let data2 = Bytes::from("second");

    // First upload should succeed
    let stream1 = Box::pin(stream::once(async move { Ok(data1) }));
    storage
      .put_stream(&key, stream1, UploadOptions { overwrite: false })
      .await
      .unwrap();

    // Second upload with overwrite=false should fail
    let stream2 = Box::pin(stream::once(async move { Ok(data2) }));
    let result = storage
      .put_stream(&key, stream2, UploadOptions { overwrite: false })
      .await;

    assert!(matches!(result, Err(BlobStorageError::AlreadyExists(_))));
  }

  #[tokio::test]
  async fn test_presigned_url_duration_validation() {
    let storage = BlobStorageMemory::new();
    let key = BlobKey::new("test-key");
    let data = Bytes::from("test");

    // Upload a blob first
    let stream = Box::pin(stream::once(async move { Ok(data) }));
    storage
      .put_stream(&key, stream, UploadOptions::default())
      .await
      .unwrap();

    // Valid duration should succeed
    let result = storage
      .get_presigned_url(&key, std::time::Duration::from_secs(3600))
      .await;
    assert!(result.is_ok());

    // Duration exceeding u32::MAX should fail
    let result = storage
      .get_presigned_url(&key, std::time::Duration::from_secs(u64::MAX))
      .await;
    assert!(matches!(result, Err(BlobStorageError::InvalidInput(_))));
  }
}
