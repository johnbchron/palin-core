//! Tests the storage interface.

use std::path::Path;

use bytes::BytesMut;
use futures::StreamExt;
use miette::{Context, IntoDiagnostic, Result};
use storage::{BlobKey, BlobStorage};
use tokio::io::BufReader;

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

  let bucket = BlobStorage::new_s3_bucket(
    &r2_bucket,
    "auto",
    &r2_endpoint,
    Some(&r2_access_key),
    Some(&r2_secret_access_key),
  )
  .await
  .context("failed to initialize bucket")?;

  let key =
    BlobKey::new("3flk9hbwfbixi5vybxlm5vsx8cqs0bi5-zenmap-7.98".to_owned());

  println!("fetch head of object at `{key}`");
  let head = bucket
    .head(&key)
    .await
    .with_context(|| format!("failed to get head of object at `{key}`"))?;
  println!("object head: {head:#?}");

  println!("fetching object at key `{key}`");
  let mut chunks = Vec::new();
  let mut stream = bucket
    .get_stream(&key)
    .await
    .with_context(|| format!("failed to get object at key `{key}`"))?;

  while let Some(result) = stream.next().await {
    match result {
      Ok(chunk) => {
        chunks.push(chunk);
      }
      Err(err) => {
        println!("got error from stream: {err}");
      }
    }
  }
  let data = chunks.into_iter().fold(BytesMut::new(), |mut acc, x| {
    acc.extend_from_slice(&x[..]);
    acc
  });
  println!("got {byte_count} bytes", byte_count = data.len(),);

  let path = Path::new("/tmp/test-object");
  println!("copying object at key `{key}` to file at path {path:?}");
  let mut file = tokio::fs::File::create(path)
    .await
    .into_diagnostic()
    .context("failed to open temp file")?;
  let mut reader = BufReader::new(&data[..]);
  tokio::io::copy(&mut reader, &mut file)
    .await
    .into_diagnostic()
    .context("failed to copy object to file")?;

  Ok(())
}
