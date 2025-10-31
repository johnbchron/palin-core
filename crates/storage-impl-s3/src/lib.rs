//! An implementation of the storage interface for S3 compatible stores.

mod errors;

use futures::TryStreamExt;
use miette::{Context, IntoDiagnostic};
use s3::{Bucket, creds::Credentials};
use storage_core::{
  BlobKey, BlobMetadata, BlobStorageError, BlobStorageLike, BlobStorageResult,
  RequestStream, ResponseStream, UploadOptions,
};
use tokio_util::io::StreamReader;
use tracing::instrument;

use self::errors::s3_error_to_blob_storage_error;

/// [`BlobStorageLike`] implementer for S3-compatible backends.
#[derive(Debug)]
pub struct BlobStorageS3 {
  bucket: Bucket,
}

impl BlobStorageS3 {
  /// Creates a new [`BlobStorageS3`].
  pub fn new(
    bucket: &str,
    region: &str,
    endpoint: &str,
    access_key: Option<&str>,
    secret_access_key: Option<&str>,
  ) -> BlobStorageResult<Self> {
    let region = s3::Region::Custom {
      region:   region.to_owned(),
      endpoint: endpoint.to_owned(),
    };

    let credentials = Credentials {
      access_key:     access_key.map(ToOwned::to_owned),
      secret_key:     secret_access_key.map(ToOwned::to_owned),
      security_token: None,
      session_token:  None,
      expiration:     None,
    };

    Ok(BlobStorageS3 {
      bucket: *Bucket::new(bucket, region, credentials)
        .map_err(s3_error_to_blob_storage_error)?,
    })
  }
}

#[async_trait::async_trait]
impl BlobStorageLike for BlobStorageS3 {
  #[instrument(skip(self, data))]
  async fn put_stream(
    &self,
    key: &str,
    data: RequestStream,
    options: UploadOptions,
  ) -> BlobStorageResult<()> {
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
  async fn get_stream(
    &self,
    key: &BlobKey,
  ) -> BlobStorageResult<ResponseStream> {
    let data = self
      .bucket
      .get_object_stream(key)
      .await
      .map_err(s3_error_to_blob_storage_error)?;
    let data = Box::pin(data.bytes.map_err(s3_error_to_blob_storage_error));

    Ok(data)
  }

  #[instrument(skip(self))]
  async fn head(&self, key: &BlobKey) -> BlobStorageResult<BlobMetadata> {
    let (head, _code) = self
      .bucket
      .head_object(key)
      .await
      .map_err(s3_error_to_blob_storage_error)?;

    Ok(BlobMetadata {
      size:             head
        .content_length
        .ok_or(BlobStorageError::NetworkError(miette::miette!(
          "head response did not include content_length"
        )))?
        .try_into()
        .into_diagnostic()
        .context("content_length was negative in head response")
        .map_err(BlobStorageError::SerializationError)?,
      content_encoding: head.content_encoding,
      content_type:     head.content_type,
      etag:             head.e_tag,
      last_modified:    head.last_modified,
      // .map(|s| s.parse())
      // .transpose()
      // .into_diagnostic()
      // .context("failed to parse last_modified from head response")
      // .map_err(BlobStorageError::SerializationError)?,
      metadata:         head.metadata.unwrap_or_default(),
    })
  }

  #[instrument(skip(self))]
  async fn delete(&self, key: &BlobKey) -> BlobStorageResult<()> {
    self
      .bucket
      .delete_object(key)
      .await
      .map_err(s3_error_to_blob_storage_error)?;

    Ok(())
  }

  #[instrument(skip(self))]
  async fn exists(&self, key: &BlobKey) -> BlobStorageResult<bool> {
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
  ) -> BlobStorageResult<()> {
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
  ) -> BlobStorageResult<String> {
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
