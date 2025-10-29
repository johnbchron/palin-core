//! An implementation of the storage interface for S3 compatible stores.

mod errors;

use miette::{Context, IntoDiagnostic};
use s3::Bucket;
use storage_core::{
  BlobKey, BlobMetadata, BlobStorageLike, BlobStorageError, ByteStream,
  StorageResult, UploadOptions,
};
use tokio_util::io::{ReaderStream, StreamReader};
use tracing::instrument;

use self::errors::s3_error_to_blob_storage_error;

/// [`BlobStorage`] implementer for S3-compatible backends.
#[derive(Debug)]
pub struct BlobStorageS3 {
  bucket: Bucket,
}

#[async_trait::async_trait]
impl BlobStorageLike for BlobStorageS3 {
  #[instrument(skip(self, data))]
  async fn put_stream(
    &self,
    key: &str,
    data: ByteStream,
    options: UploadOptions,
  ) -> StorageResult<()> {
    // adapt to AsyncReader
    let mut stream = StreamReader::new(data);

    // start building request
    let req = self.bucket.put_object_stream_builder(key);

    // add content type if available
    let req = if let Some(content_type) = options.content_type {
      req.with_content_type(content_type)
    } else {
      req
    };

    // send the request
    let _resp = req
      .execute_stream(&mut stream)
      .await
      .map_err(s3_error_to_blob_storage_error)?;

    Ok(())
  }

  #[instrument(skip(self))]
  async fn get_stream(&self, key: &BlobKey) -> StorageResult<ByteStream> {
    let data = self
      .bucket
      .get_object_stream(key)
      .await
      .map_err(s3_error_to_blob_storage_error)?;
    let data = Box::pin(ReaderStream::new(data));

    Ok(data)
  }

  #[instrument(skip(self))]
  async fn head(&self, key: &BlobKey) -> StorageResult<BlobMetadata> {
    let (head, _code) = self
      .bucket
      .head_object(key)
      .await
      .map_err(s3_error_to_blob_storage_error)?;

    Ok(BlobMetadata {
      size: head
        .content_length
        .ok_or(BlobStorageError::NetworkError(miette::miette!(
          "head response did not include content_length"
        )))?
        .try_into()
        .into_diagnostic()
        .context("content_length was negative in head response")
        .map_err(BlobStorageError::SerializationError)?,

      content_type:  head.content_type,
      etag:          head.e_tag,
      last_modified: head
        .last_modified
        .map(|s| s.parse())
        .transpose()
        .into_diagnostic()
        .context("failed to parse last_modified from head response")
        .map_err(BlobStorageError::SerializationError)?,
      metadata:      head.metadata.unwrap_or_default(),
    })
  }

  #[instrument(skip(self))]
  async fn delete(&self, key: &BlobKey) -> StorageResult<()> {
    self
      .bucket
      .delete_object(key)
      .await
      .map_err(s3_error_to_blob_storage_error)?;

    Ok(())
  }

  #[instrument(skip(self))]
  async fn exists(&self, key: &BlobKey) -> StorageResult<bool> {
    self
      .bucket
      .object_exists(key)
      .await
      .map_err(s3_error_to_blob_storage_error)
  }

  #[instrument(skip(self))]
  async fn copy(
    &self,
    from_key: &BlobKey,
    to_key: &BlobKey,
  ) -> StorageResult<()> {
    self
      .bucket
      .copy_object_internal(from_key, to_key)
      .await
      .map(|_| ())
      .map_err(s3_error_to_blob_storage_error)
  }

  #[instrument(skip(self))]
  async fn get_presigned_url(
    &self,
    key: &BlobKey,
    expiry: std::time::Duration,
  ) -> StorageResult<String> {
    self
      .bucket
      .presign_get(
        key,
        expiry.as_secs().max(1).min(u32::MAX as u64) as u32,
        None,
      )
      .await
      .map_err(s3_error_to_blob_storage_error)
  }
}
