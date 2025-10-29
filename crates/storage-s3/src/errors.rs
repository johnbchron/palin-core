use miette::Report;
use s3::error::S3Error;
use storage_core::BlobStorageError;

pub(crate) fn s3_error_to_blob_storage_error(err: S3Error) -> BlobStorageError {
  match err {
    // invalid config
    e @ (S3Error::MaxExpiry(_)
    | S3Error::PostPolicyError(_)
    | S3Error::Credentials(_)
    | S3Error::Region(_)
    | S3Error::UrlParse(_)) => {
      BlobStorageError::InvalidConfig(Report::from_err(e))
    }

    // potentially retryable network error
    e @ (S3Error::HttpFailWithBody(_, _) | S3Error::HttpFail) => {
      BlobStorageError::NetworkError(
        Report::from_err(e).context("HTTP response code error"),
      )
    }
    // probably unrecoverable network error
    e @ (S3Error::Http(_)
    | S3Error::Reqwest(_)
    | S3Error::ReqwestHeaderToStr(_)
    | S3Error::InvalidHeaderValue(_)
    | S3Error::InvalidHeaderName(_)) => {
      BlobStorageError::NetworkError(Report::from_err(e))
    }

    // various data and serialization errors
    e @ (S3Error::Utf8(_)
    | S3Error::FromUtf8(_)
    | S3Error::SerdeXml(_)
    | S3Error::TimeFormatError(_)
    | S3Error::SerdeError(_)
    | S3Error::XmlSeError(_)) => {
      BlobStorageError::SerializationError(Report::from_err(e))
    }

    // an actual IO error
    S3Error::Io(err) => BlobStorageError::IoError(err),

    // unrecoverables
    e @ (S3Error::HmacInvalidLength(_)
    | S3Error::WLCredentials
    | S3Error::RLCredentials
    | S3Error::FmtError(_)
    | S3Error::CredentialsReadLock
    | S3Error::CredentialsWriteLock) => {
      BlobStorageError::Unknown(Report::from_err(e))
    }

    // wildcard
    e => {
      BlobStorageError::Unknown(Report::from_err(e).context("wildcard variant"))
    }
  }
}
