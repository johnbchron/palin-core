use std::sync::Arc;

use storage_core::BlobStorageLike;
use storage_impl_memory::BlobStorageMemory;

trait StorageInstantiator {
  fn init() -> Arc<dyn BlobStorageLike>;
}

struct MemoryInstatiator;

impl StorageInstantiator for MemoryInstatiator {
  fn init() -> Arc<dyn BlobStorageLike> { Arc::new(BlobStorageMemory::new()) }
}

#[generic_tests::define(attrs(tokio::test))]
mod generic_testing {
  use std::time::Duration;

  use bytes::Bytes;
  use futures::{StreamExt, stream};
  use storage_core::BlobStorageLike;

  use super::{super::*, StorageInstantiator};
  use crate::tests::MemoryInstatiator;

  // Helper function to create a stream from bytes
  fn bytes_stream(data: Vec<u8>) -> RequestStream {
    Box::pin(stream::once(async move { Ok(Bytes::from(data)) }))
  }

  // Helper function to collect stream into bytes
  async fn collect_stream(
    mut stream: ResponseStream,
  ) -> Result<Vec<u8>, BlobStorageError> {
    let mut result = Vec::new();
    while let Some(chunk) = stream.next().await {
      result.extend_from_slice(&chunk?);
    }
    Ok(result)
  }

  // Test fixture that creates a fresh BlobStorage instance
  fn setup<I: StorageInstantiator>() -> Arc<dyn BlobStorageLike> {
    <I as StorageInstantiator>::init()
  }

  #[tokio::test]
  async fn test_put_and_get_basic<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let key = BlobKey::new("test-blob");
    let data = b"Hello, World!".to_vec();

    // Upload
    let options = UploadOptions {
      content_type: Some("text/plain".to_string()),
      overwrite:    true,
    };
    storage
      .put_stream(&key, bytes_stream(data.clone()), options)
      .await
      .unwrap();

    // Download
    let stream = storage.get_stream(&key).await.unwrap();
    let retrieved = collect_stream(stream).await.unwrap();

    assert_eq!(data, retrieved);
  }

  #[tokio::test]
  async fn test_put_overwrite_flag<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let key = BlobKey::new("overwrite-test");
    let data1 = b"first".to_vec();
    let data2 = b"second".to_vec();

    // First upload
    let options = UploadOptions {
      content_type: None,
      overwrite:    false,
    };
    storage
      .put_stream(&key, bytes_stream(data1.clone()), options.clone())
      .await
      .unwrap();

    // Second upload without overwrite should fail
    let result = storage
      .put_stream(&key, bytes_stream(data2.clone()), options)
      .await;
    assert!(matches!(result, Err(BlobStorageError::AlreadyExists(_))));

    // Third upload with overwrite should succeed
    let options_overwrite = UploadOptions {
      content_type: None,
      overwrite:    true,
    };
    storage
      .put_stream(&key, bytes_stream(data2.clone()), options_overwrite)
      .await
      .unwrap();

    // Verify the data was overwritten
    let stream = storage.get_stream(&key).await.unwrap();
    let retrieved = collect_stream(stream).await.unwrap();
    assert_eq!(data2, retrieved);
  }

  #[tokio::test]
  async fn test_get_nonexistent_blob<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let key = BlobKey::new("nonexistent");

    let result = storage.get_stream(&key).await;
    assert!(matches!(result, Err(BlobStorageError::NotFound(_))));
  }

  #[tokio::test]
  async fn test_head_metadata<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let key = BlobKey::new("metadata-test");
    let data = b"test data for metadata".to_vec();

    let options = UploadOptions {
      content_type: Some("application/octet-stream".to_string()),
      overwrite:    true,
    };
    storage
      .put_stream(&key, bytes_stream(data.clone()), options)
      .await
      .unwrap();

    let metadata = storage.head(&key).await.unwrap();

    assert_eq!(metadata.size, data.len() as u64);
    assert_eq!(
      metadata.content_type,
      Some("application/octet-stream".to_string())
    );
    assert!(metadata.etag.is_some());
  }

  #[tokio::test]
  async fn test_head_nonexistent_blob<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let key = BlobKey::new("nonexistent-head");

    let result = storage.head(&key).await;
    assert!(matches!(result, Err(BlobStorageError::NotFound(_))));
  }

  #[tokio::test]
  async fn test_delete_blob<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let key = BlobKey::new("delete-test");
    let data = b"to be deleted".to_vec();

    // Upload
    let options = UploadOptions {
      content_type: None,
      overwrite:    true,
    };
    storage
      .put_stream(&key, bytes_stream(data), options)
      .await
      .unwrap();

    // Verify it exists
    assert!(storage.exists(&key).await.unwrap());

    // Delete
    storage.delete(&key).await.unwrap();

    // Verify it's gone
    assert!(!storage.exists(&key).await.unwrap());
  }

  #[tokio::test]
  async fn test_delete_nonexistent_blob<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let key = BlobKey::new("nonexistent-delete");

    // Deleting nonexistent blob should either succeed or return NotFound
    // (implementation dependent)
    let result = storage.delete(&key).await;
    assert!(
      result.is_ok() || matches!(result, Err(BlobStorageError::NotFound(_)))
    );
  }

  #[tokio::test]
  async fn test_exists<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let key = BlobKey::new("exists-test");
    let data = b"exists check".to_vec();

    // Should not exist initially
    assert!(!storage.exists(&key).await.unwrap());

    // Upload
    let options = UploadOptions {
      content_type: None,
      overwrite:    true,
    };
    storage
      .put_stream(&key, bytes_stream(data), options)
      .await
      .unwrap();

    // Should exist now
    assert!(storage.exists(&key).await.unwrap());

    // Delete
    storage.delete(&key).await.unwrap();

    // Should not exist again
    assert!(!storage.exists(&key).await.unwrap());
  }

  #[tokio::test]
  async fn test_copy_blob<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let source_key = BlobKey::new("copy-source");
    let dest_key = BlobKey::new("copy-dest");
    let data = b"data to copy".to_vec();

    // Upload source
    let options = UploadOptions {
      content_type: Some("text/plain".to_string()),
      overwrite:    true,
    };
    storage
      .put_stream(&source_key, bytes_stream(data.clone()), options)
      .await
      .unwrap();

    // Copy
    storage.copy(&source_key, &dest_key).await.unwrap();

    // Verify destination exists and has same content
    let stream = storage.get_stream(&dest_key).await.unwrap();
    let retrieved = collect_stream(stream).await.unwrap();
    assert_eq!(data, retrieved);

    // Verify source still exists
    assert!(storage.exists(&source_key).await.unwrap());
  }

  #[tokio::test]
  async fn test_copy_nonexistent_source<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let source_key = BlobKey::new("nonexistent-source");
    let dest_key = BlobKey::new("copy-dest");

    let result = storage.copy(&source_key, &dest_key).await;
    assert!(matches!(result, Err(BlobStorageError::NotFound(_))));
  }

  #[tokio::test]
  async fn test_presigned_url<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let key = BlobKey::new("presigned-test");
    let data = b"presigned url test".to_vec();

    // Upload
    let options = UploadOptions {
      content_type: None,
      overwrite:    true,
    };
    storage
      .put_stream(&key, bytes_stream(data), options)
      .await
      .unwrap();

    // Get presigned URL
    let expiry = Duration::from_secs(3600);
    let url = storage.get_presigned_url(&key, expiry).await.unwrap();

    assert!(!url.is_empty());
    assert!(
      url.starts_with("http://")
        || url.starts_with("https://")
        || url.starts_with("memory://")
        || url.starts_with("file://")
    );
  }

  #[tokio::test]
  async fn test_presigned_url_nonexistent<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let key = BlobKey::new("nonexistent-presigned");

    let expiry = Duration::from_secs(3600);
    let result = storage.get_presigned_url(&key, expiry).await;

    // Implementation may return NotFound or succeed (some backends generate
    // URLs without checking existence)
    assert!(
      result.is_ok() || matches!(result, Err(BlobStorageError::NotFound(_)))
    );
  }

  #[tokio::test]
  async fn test_presigned_url_invalid_duration<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let key = BlobKey::new("duration-test");

    // Try with duration exceeding u32::MAX seconds
    let expiry = Duration::from_secs(u64::from(u32::MAX) + 1);
    let result = storage.get_presigned_url(&key, expiry).await;

    assert!(matches!(result, Err(BlobStorageError::InvalidInput(_))));
  }

  #[tokio::test]
  async fn test_large_blob<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let key = BlobKey::new("large-blob");

    // Create a 10MB blob
    let size = 10 * 1024 * 1024;
    let data = vec![42u8; size];

    let options = UploadOptions {
      content_type: None,
      overwrite:    true,
    };
    storage
      .put_stream(&key, bytes_stream(data.clone()), options)
      .await
      .unwrap();

    // Verify size in metadata
    let metadata = storage.head(&key).await.unwrap();
    assert_eq!(metadata.size, size as u64);

    // Verify content
    let stream = storage.get_stream(&key).await.unwrap();
    let retrieved = collect_stream(stream).await.unwrap();
    assert_eq!(data.len(), retrieved.len());
  }

  #[tokio::test]
  async fn test_empty_blob<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let key = BlobKey::new("empty-blob");
    let data = Vec::new();

    let options = UploadOptions {
      content_type: None,
      overwrite:    true,
    };
    storage
      .put_stream(&key, bytes_stream(data.clone()), options)
      .await
      .unwrap();

    // Verify metadata shows zero size
    let metadata = storage.head(&key).await.unwrap();
    assert_eq!(metadata.size, 0);

    // Verify retrieval
    let stream = storage.get_stream(&key).await.unwrap();
    let retrieved = collect_stream(stream).await.unwrap();
    assert_eq!(retrieved.len(), 0);
  }

  #[tokio::test]
  async fn test_special_characters_in_key<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let key = BlobKey::new("test/with/slashes-and_underscores");
    let data = b"special key test".to_vec();

    let options = UploadOptions {
      content_type: None,
      overwrite:    true,
    };
    storage
      .put_stream(&key, bytes_stream(data.clone()), options)
      .await
      .unwrap();

    let stream = storage.get_stream(&key).await.unwrap();
    let retrieved = collect_stream(stream).await.unwrap();
    assert_eq!(data, retrieved);
  }

  #[tokio::test]
  async fn test_concurrent_operations<I: StorageInstantiator>() {
    let storage = setup::<I>();
    let keys: Vec<_> = (0..10)
      .map(|i| BlobKey::new(format!("concurrent-{i}")))
      .collect();

    // Upload concurrently
    let mut handles = vec![];
    for (i, key) in keys.iter().enumerate() {
      let storage = storage.clone();
      let key = key.clone();
      let data = format!("data-{i}").into_bytes();

      let handle = tokio::spawn(async move {
        let options = UploadOptions {
          content_type: None,
          overwrite:    true,
        };
        storage
          .put_stream(&key, bytes_stream(data), options)
          .await
          .unwrap();
      });
      handles.push(handle);
    }

    for handle in handles {
      handle.await.unwrap();
    }

    // Verify all uploads succeeded
    for key in &keys {
      assert!(storage.exists(key).await.unwrap());
    }
  }

  #[instantiate_tests(<MemoryInstatiator>)]
  mod test_memory {}
}
