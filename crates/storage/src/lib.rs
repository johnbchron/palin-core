//! Frontend for a cloud storage interface.

use std::sync::Arc;

use storage_core::RequestStream;
pub use storage_core::{
  BlobKey, BlobMetadata, BlobStorageError, BlobStorageResult, Bytes,
  ResponseStream, UploadOptions,
};
use storage_impl_memory::BlobStorageMemory;
use storage_impl_s3::BlobStorageS3;

/// Frontend for a cloud storage interface.
pub struct BlobStorage {
  inner: Arc<dyn storage_core::BlobStorageLike>,
}

impl BlobStorage {
  /// Creates a new [`BlobStorage`] from an S3 bucket.
  pub async fn new_s3_bucket(
    bucket: &str,
    region: &str,
    endpoint: &str,
    access_key: Option<&str>,
    secret_access_key: Option<&str>,
  ) -> BlobStorageResult<Self> {
    Ok(BlobStorage {
      inner: Arc::new(BlobStorageS3::new(
        bucket,
        region,
        endpoint,
        access_key,
        secret_access_key,
      )?),
    })
  }

  /// Creates a new [`BlobStorage`] from an in-memory store.
  pub fn new_memory() -> Self {
    BlobStorage {
      inner: Arc::new(BlobStorageMemory::new()),
    }
  }
}

impl BlobStorage {
  /// Upload data from a stream to a blob
  pub async fn put_stream(
    &self,
    key: &str,
    data: RequestStream,
    options: UploadOptions,
  ) -> BlobStorageResult<()> {
    self.inner.put_stream(key, data, options).await
  }
  /// Download data from a blob as a stream
  pub async fn get_stream(
    &self,
    key: &BlobKey,
  ) -> BlobStorageResult<ResponseStream> {
    self.inner.get_stream(key).await
  }
  /// Get metadata for a blob without downloading content
  pub async fn head(&self, key: &BlobKey) -> BlobStorageResult<BlobMetadata> {
    self.inner.head(key).await
  }
  /// Delete a blob
  pub async fn delete(&self, key: &BlobKey) -> BlobStorageResult<()> {
    self.inner.delete(key).await
  }
  /// Check if a blob exists
  pub async fn exists(&self, key: &BlobKey) -> BlobStorageResult<bool> {
    self.inner.exists(key).await
  }
  /// Copy a blob from one key to another
  pub async fn copy(
    &self,
    from_key: &BlobKey,
    to_key: &BlobKey,
  ) -> BlobStorageResult<()> {
    self.inner.copy(from_key, to_key).await
  }
  /// Get a pre-signed URL for temporary access (if supported)
  pub async fn get_presigned_url(
    &self,
    key: &BlobKey,
    expiry: std::time::Duration,
  ) -> BlobStorageResult<String> {
    self.inner.get_presigned_url(key, expiry).await
  }
}
