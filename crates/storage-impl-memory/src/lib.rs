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
use tracing::instrument;

/// Internal representation of a stored blob
#[derive(Debug, Clone)]
struct StoredBlob {
  /// The actual blob data
  data:          Bytes,
  /// Content type
  content_type:  Option<String>,
  /// ETag (generated from content hash)
  etag:          String,
  /// Last modified timestamp
  last_modified: String,
}

impl StoredBlob {
  fn new(data: Bytes, content_type: Option<String>) -> Self {
    let etag = format!("{:x}", md5::compute(&data));
    let last_modified = Self::current_timestamp();

    Self {
      data,
      content_type,
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
    chrono::DateTime::from_timestamp(now as i64, 0)
      .unwrap()
      .to_rfc3339()
  }

  fn metadata(&self) -> BlobMetadata {
    BlobMetadata {
      size:             self.data.len() as u64,
      content_encoding: None,
      content_type:     self.content_type.clone(),
      etag:             Some(self.etag.clone()),
      last_modified:    Some(self.last_modified.clone()),
      metadata:         HashMap::new(),
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
  pub fn new() -> Self {
    Self {
      storage: Arc::new(RwLock::new(HashMap::new())),
    }
  }

  /// Returns the number of blobs currently stored.
  pub async fn len(&self) -> usize { self.storage.read().await.len() }

  /// Returns true if no blobs are stored.
  pub async fn is_empty(&self) -> bool { self.storage.read().await.is_empty() }

  /// Clears all stored blobs.
  pub async fn clear(&self) { self.storage.write().await.clear(); }

  /// Lists all blob keys with optional prefix filter.
  pub async fn list(
    &self,
    prefix: Option<&str>,
  ) -> BlobStorageResult<Vec<BlobEntry>> {
    let storage = self.storage.read().await;
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

    Ok(entries)
  }
}

impl Default for BlobStorageMemory {
  fn default() -> Self { Self::new() }
}

#[async_trait::async_trait]
impl BlobStorageLike for BlobStorageMemory {
  #[instrument(skip(self, data))]
  async fn put_stream(
    &self,
    key: &str,
    data: RequestStream,
    options: UploadOptions,
  ) -> BlobStorageResult<()> {
    // Check if we should overwrite
    if !options.overwrite {
      let storage = self.storage.read().await;
      if storage.contains_key(key) {
        return Err(BlobStorageError::PermissionDenied(miette::miette!(
          "Blob already exists and overwrite is false: {}",
          key
        )));
      }
    }

    // Collect the stream into a single Bytes object
    let chunks: Vec<Bytes> = data
      .try_collect()
      .await
      .map_err(|e| BlobStorageError::StreamError(miette::miette!(e)))?;

    // Combine all chunks
    let total_size: usize = chunks.iter().map(|c| c.len()).sum();
    let mut combined = Vec::with_capacity(total_size);
    for chunk in chunks {
      combined.extend_from_slice(&chunk);
    }

    let blob = StoredBlob::new(Bytes::from(combined), options.content_type);

    // Store the blob
    self.storage.write().await.insert(key.to_string(), blob);

    Ok(())
  }

  #[instrument(skip(self))]
  async fn get_stream(
    &self,
    key: &BlobKey,
  ) -> BlobStorageResult<ResponseStream> {
    let storage = self.storage.read().await;
    let blob = storage
      .get(key.as_str())
      .ok_or_else(|| BlobStorageError::NotFound(key.clone()))?;

    let data = blob.data.clone();

    // Create a stream that yields the data in chunks
    // For simplicity, we'll just yield the entire blob as one chunk
    let stream = stream::once(async move { Ok(data) });

    Ok(Box::pin(stream))
  }

  #[instrument(skip(self))]
  async fn head(&self, key: &BlobKey) -> BlobStorageResult<BlobMetadata> {
    let storage = self.storage.read().await;
    let blob = storage
      .get(key.as_str())
      .ok_or_else(|| BlobStorageError::NotFound(key.clone()))?;

    Ok(blob.metadata())
  }

  #[instrument(skip(self))]
  async fn delete(&self, key: &BlobKey) -> BlobStorageResult<()> {
    let mut storage = self.storage.write().await;
    storage
      .remove(key.as_str())
      .ok_or_else(|| BlobStorageError::NotFound(key.clone()))?;

    Ok(())
  }

  #[instrument(skip(self))]
  async fn exists(&self, key: &BlobKey) -> BlobStorageResult<bool> {
    let storage = self.storage.read().await;
    Ok(storage.contains_key(key.as_str()))
  }

  #[instrument(skip(self))]
  async fn copy(
    &self,
    from_key: &BlobKey,
    to_key: &BlobKey,
  ) -> BlobStorageResult<()> {
    let storage = self.storage.read().await;
    let blob = storage
      .get(from_key.as_str())
      .ok_or_else(|| BlobStorageError::NotFound(from_key.clone()))?
      .clone();
    drop(storage);

    // Update the last_modified timestamp for the copy
    let mut copied_blob = blob;
    copied_blob.last_modified = StoredBlob::current_timestamp();

    self
      .storage
      .write()
      .await
      .insert(to_key.as_str().to_string(), copied_blob);

    Ok(())
  }

  #[instrument(skip(self))]
  async fn get_presigned_url(
    &self,
    key: &BlobKey,
    _expiry: std::time::Duration,
  ) -> BlobStorageResult<String> {
    // Check if the blob exists
    let storage = self.storage.read().await;
    if !storage.contains_key(key.as_str()) {
      return Err(BlobStorageError::NotFound(key.clone()));
    }

    // For in-memory storage, presigned URLs don't make much sense,
    // but we can return a fake URL for testing purposes
    Ok(format!("memory://blob/{}", key.as_str()))
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
      .put_stream(key.as_str(), stream, UploadOptions {
        content_type: Some("text/plain".to_string()),
        overwrite:    true,
      })
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
      .put_stream(key.as_str(), stream, UploadOptions {
        content_type: Some("text/plain".to_string()),
        overwrite:    true,
      })
      .await
      .unwrap();

    let metadata = storage.head(&key).await.unwrap();
    assert_eq!(metadata.size, 11);
    assert_eq!(metadata.content_type, Some("text/plain".to_string()));
  }

  #[tokio::test]
  async fn test_delete() {
    let storage = BlobStorageMemory::new();
    let key = BlobKey::new("test-key");
    let data = Bytes::from("hello world");

    let stream = Box::pin(stream::once(async { Ok(data) }));
    storage
      .put_stream(key.as_str(), stream, UploadOptions::default())
      .await
      .unwrap();

    assert!(storage.exists(&key).await.unwrap());
    storage.delete(&key).await.unwrap();
    assert!(!storage.exists(&key).await.unwrap());
  }

  #[tokio::test]
  async fn test_copy() {
    let storage = BlobStorageMemory::new();
    let from_key = BlobKey::new("from-key");
    let to_key = BlobKey::new("to-key");
    let data = Bytes::from("hello world");

    let stream = Box::pin(stream::once({
      let data = data.clone();
      async move { Ok(data.clone()) }
    }));
    storage
      .put_stream(from_key.as_str(), stream, UploadOptions::default())
      .await
      .unwrap();

    storage.copy(&from_key, &to_key).await.unwrap();

    let mut result_stream = storage.get_stream(&to_key).await.unwrap();
    let result = result_stream.next().await.unwrap().unwrap();
    assert_eq!(result, data);
  }

  #[tokio::test]
  async fn test_list() {
    let storage = BlobStorageMemory::new();

    // Add some blobs
    for i in 0..5 {
      let key = format!("prefix/file{}", i);
      let data = Bytes::from(format!("data{}", i));
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
}
