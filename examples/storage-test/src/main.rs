//! Tests the storage interface.

use std::path::Path;

use belt::Belt;
use futures::TryStreamExt;
use miette::{Context, IntoDiagnostic, Result};
use storage::{BlobKey, BlobStorage, BlobStorageError, UploadOptions};
use tokio::fs::File;
use tokio_util::io::ReaderStream;

#[tokio::main]
async fn main() -> Result<()> {
  let r2_access_key = std::env::var("R2_ACCESS_KEY")
    .into_diagnostic()
    .context("could not read `R2_ACCESS_KEY`")?;
  let r2_secret_access_key = std::env::var("R2_SECRET_ACCESS_KEY")
    .into_diagnostic()
    .context("could not read `R2_SECRET_ACCESS_KEY`")?;
  let r2_bucket = std::env::var("R2_BUCKET")
    .into_diagnostic()
    .context("could not read `R2_BUCKET`")?;
  let r2_account_id = std::env::var("R2_ACCOUNT_ID")
    .into_diagnostic()
    .context("could not read `R2_ACCOUNT_ID`")?;
  let r2_endpoint = format!("https://{r2_account_id}.r2.cloudflarestorage.com");

  // init bucket
  let bucket = BlobStorage::new_s3_bucket(
    &r2_bucket,
    "auto",
    &r2_endpoint,
    Some(&r2_access_key),
    Some(&r2_secret_access_key),
  )
  .context("failed to initialize bucket")?;

  let key =
    BlobKey::new("3flk9hbwfbixi5vybxlm5vsx8cqs0bi5-zenmap-7.98".to_owned());

  // get object head
  println!("fetch head of object at `{key}`");
  let head = bucket
    .head(&key)
    .await
    .with_context(|| format!("failed to get head of object at `{key}`"))?;
  println!("object head: {head:#?}");

  // fetch object
  println!("fetching object at key `{key}`");
  let stream = bucket
    .get_stream(&key)
    .await
    .with_context(|| format!("failed to get object at key `{key}`"))?;
  let data =
    Belt::new(Box::pin(stream.map_err(BlobStorageError::into_io_error)));
  let counter = data.counter();

  // copy to fs
  let path = Path::new("/tmp/test-object");
  println!(
    "copying object at key `{key}` to file at path \"{path}\"",
    path = path.display()
  );
  let mut file = File::create(path)
    .await
    .into_diagnostic()
    .context("failed to open temp file")?;
  let mut reader = data.into_async_read();
  tokio::io::copy(&mut reader, &mut file)
    .await
    .into_diagnostic()
    .context("failed to copy object to file")?;
  println!(
    "streamed {count} bytes from object at key `{key}`",
    count = counter.get()
  );

  // upload object
  println!(
    "uploading file at \"{path}\" to key `{key}`",
    path = path.display()
  );
  let file = File::open(path)
    .await
    .into_diagnostic()
    .context("failed to open temp file")?;
  let stream = Belt::new(ReaderStream::new(file));
  let counter = stream.counter();
  bucket
    .put_stream(&key, Box::pin(stream), UploadOptions { overwrite: true })
    .await
    .with_context(|| format!("failed to put object at key `{key}`"))?;
  println!(
    "streamed {count} bytes to object at key `{key}`",
    count = counter.get()
  );

  Ok(())
}
