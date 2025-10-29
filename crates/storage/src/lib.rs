//! Frontend for a cloud storage interface.

use std::sync::Arc;

use storage_core::StorageResult;
use storage_s3::BlobStorageS3;

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
  ) -> StorageResult<Self> {
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
}
