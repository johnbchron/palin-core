//! An implementation of the storage interface for S3 compatible stores.

mod errors;

use futures::TryStreamExt;
use miette::{Context, IntoDiagnostic, miette};
use s3::{Bucket, creds::Credentials};
use storage_core::{
  BlobKey, BlobMetadata, BlobStorageError, BlobStorageLike, BlobStorageResult,
  RequestStream, ResponseStream, UploadOptions,
};
use tokio_util::io::StreamReader;
use tracing::{debug, error, info, instrument, warn};

use self::errors::s3_error_to_blob_storage_error;

/// [`BlobStorageLike`] implementer for S3-compatible backends.
#[derive(Debug)]
pub struct BlobStorageS3 {
  bucket: Bucket,
}

impl BlobStorageS3 {
  /// Creates a new [`BlobStorageS3`].
  #[instrument(
    skip(access_key, secret_access_key),
    fields(bucket, region, endpoint)
  )]
  pub fn new(
    bucket: &str,
    region: &str,
    endpoint: &str,
    access_key: Option<&str>,
    secret_access_key: Option<&str>,
  ) -> BlobStorageResult<Self> {
    debug!(
      bucket = bucket,
      region = region,
      endpoint = endpoint,
      has_access_key = access_key.is_some(),
      has_secret_key = secret_access_key.is_some(),
      "Initializing S3 blob storage"
    );

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

    let storage = BlobStorageS3 {
      bucket: *Bucket::new(bucket, region, credentials)
        .map_err(s3_error_to_blob_storage_error)?,
    };

    info!(bucket = bucket, "S3 blob storage initialized successfully");
    Ok(storage)
  }
}

#[async_trait::async_trait]
impl BlobStorageLike for BlobStorageS3 {
  #[instrument(
    skip(self, data),
    fields(
      key = %key,
      bucket = %self.bucket.name,
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

    // Check if object exists when overwrite is false
    if !options.overwrite {
      debug!("Checking if object already exists");
      let exists = self.head(key).await?.is_some();
      if exists {
        warn!("Object already exists and overwrite=false");
        return Err(BlobStorageError::AlreadyExists(key.clone()));
      }
      debug!("Object does not exist, proceeding with upload");
    }

    // adapt to AsyncReader
    let mut stream = StreamReader::new(data);

    // build request
    let req = self.bucket.put_object_stream_builder(key);

    // send the request
    debug!("Executing upload stream");
    let _resp = req.execute_stream(&mut stream).await.map_err(|e| {
      error!(error = ?e, "Failed to upload stream");
      s3_error_to_blob_storage_error(e)
    })?;

    info!("Stream uploaded successfully");
    Ok(())
  }

  #[instrument(
    skip(self),
    fields(
      key = %key,
      bucket = %self.bucket.name,
    ),
    err
  )]
  async fn get_stream(
    &self,
    key: &BlobKey,
  ) -> BlobStorageResult<ResponseStream> {
    debug!("Retrieving object stream");

    let data = self.bucket.get_object_stream(key).await.map_err(|e| {
      error!(error = ?e, "Failed to get object stream");
      s3_error_to_blob_storage_error(e)
    })?;
    let data = Box::pin(data.bytes.map_err(s3_error_to_blob_storage_error));

    info!("Object stream retrieved successfully");
    Ok(data)
  }

  #[instrument(
    skip(self),
    fields(
      key = %key,
      bucket = %self.bucket.name,
    ),
    err
  )]
  async fn head(
    &self,
    key: &BlobKey,
  ) -> BlobStorageResult<Option<BlobMetadata>> {
    debug!("Fetching object metadata");

    let (head, code) = self.bucket.head_object(key).await.map_err(|e| {
      error!(error = ?e, "Failed to fetch object metadata");
      s3_error_to_blob_storage_error(e)
    })?;

    debug!(status_code = code, "Received HEAD response");

    let err = miette!("got {code} response from API");
    match code {
      200 => (),
      200..300 | 300..400 | 412 => {
        warn!(
          code,
          response = ?head,
          "Got technically successful but curious response code from S3"
        );
      }

      400 => return Err(BlobStorageError::InvalidInput(err)),
      403 => return Err(BlobStorageError::PermissionDenied(err)),

      404 => return Ok(None),

      500..600 => return Err(BlobStorageError::NetworkError(err)),
      _ => return Err(BlobStorageError::Unknown(err)),
    }

    let size = head
      .content_length
      .ok_or_else(|| {
        error!("HEAD response missing content_length");
        BlobStorageError::NetworkError(miette!(
          "head response did not include content_length"
        ))
      })?
      .try_into()
      .into_diagnostic()
      .context("content_length was negative in head response")
      .map_err(|e| {
        error!(error = ?e, "Invalid content_length in HEAD response");
        BlobStorageError::SerializationError(e)
      })?;

    let metadata = BlobMetadata {
      size,
      etag: head.e_tag.clone(),
      last_modified: head.last_modified.clone(),
    };

    info!(
      size = size,
      etag = ?metadata.etag,
      "Object metadata retrieved successfully"
    );

    Ok(Some(metadata))
  }

  #[instrument(
    skip(self),
    fields(
      key = %key,
      bucket = %self.bucket.name,
    ),
    err
  )]
  async fn delete(&self, key: &BlobKey) -> BlobStorageResult<()> {
    debug!("Deleting object");

    self.bucket.delete_object(key).await.map_err(|e| {
      error!(error = ?e, "Failed to delete object");
      s3_error_to_blob_storage_error(e)
    })?;

    info!("Object deleted successfully");
    Ok(())
  }

  #[instrument(
    skip(self),
    fields(
      key = %key,
      bucket = %self.bucket.name,
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

    // Validate that expiry fits in u32 (S3 API limitation)
    if expiry_secs > u64::from(u32::MAX) {
      error!(
        expiry_secs = expiry_secs,
        max_secs = u32::MAX,
        "Expiry duration exceeds maximum"
      );
      return Err(BlobStorageError::InvalidInput(miette!(
        "Expiry duration of {} seconds exceeds maximum supported duration of \
         {} seconds (~136 years)",
        expiry_secs,
        u32::MAX
      )));
    }

    // Ensure at least 1 second
    let expiry_u32 = u32::try_from(expiry_secs.max(1)).unwrap();
    if expiry_secs < 1 {
      debug!("Adjusted expiry from 0 to 1 second");
    }

    let url = self
      .bucket
      .presign_get(key, expiry_u32, None)
      .await
      .map_err(|e| {
        error!(error = ?e, "Failed to generate presigned URL");
        s3_error_to_blob_storage_error(e)
      })?;

    info!(
      expiry_secs = expiry_u32,
      url_length = url.len(),
      "Presigned URL generated successfully"
    );
    Ok(url)
  }
}
