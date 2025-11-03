use std::io;

use bytes::Bytes;
use futures::stream::{self, StreamExt};
use tokio::io::AsyncReadExt;

use super::*;

#[tokio::test]
async fn test_empty_belt() {
  let belt = Belt::empty();
  let result = belt.collect_bytes().await.unwrap();
  assert_eq!(result, Bytes::new());
}

#[tokio::test]
async fn test_from_bytes() {
  let data = Bytes::from("hello world");
  let belt = Belt::from(data.clone());
  let result = belt.collect_bytes().await.unwrap();
  assert_eq!(result, data);
}

#[tokio::test]
async fn test_from_vec() {
  let data = vec![1u8, 2, 3, 4, 5];
  let expected = Bytes::from(data.clone());
  let belt = Belt::from(data);
  let result = belt.collect_bytes().await.unwrap();
  assert_eq!(result, expected);
}

#[tokio::test]
async fn test_from_static_slice() {
  let data: &'static [u8] = b"static data";
  let belt = Belt::from(data);
  let result = belt.collect_bytes().await.unwrap();
  assert_eq!(result, Bytes::from_static(data));
}

#[tokio::test]
async fn test_from_static_str() {
  let data: &'static str = "hello rust";
  let belt = Belt::from(data);
  let result = belt.collect_bytes().await.unwrap();
  assert_eq!(result, Bytes::from_static(data.as_bytes()));
}

#[tokio::test]
async fn test_new_with_stream() {
  let chunks = vec![
    Ok(Bytes::from("hello ")),
    Ok(Bytes::from("world")),
    Ok(Bytes::from("!")),
  ];
  let stream = stream::iter(chunks);
  let belt = Belt::new(stream);
  let result = belt.collect_bytes().await.unwrap();
  assert_eq!(result, Bytes::from("hello world!"));
}

#[tokio::test]
async fn test_new_with_error_stream() {
  let chunks = vec![
    Ok(Bytes::from("hello")),
    Err(io::Error::other("test error")),
  ];
  let stream = stream::iter(chunks);
  let belt = Belt::new(stream);
  let result = belt.collect_bytes().await;
  assert!(result.is_err());
  assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);
}

#[tokio::test]
async fn test_stream_poll_static() {
  let belt = Belt::from("test");
  let collected: Vec<_> = belt.collect::<Vec<_>>().await;
  assert_eq!(collected.len(), 1);
  assert_eq!(collected[0].as_ref().unwrap(), &Bytes::from("test"));
}

#[tokio::test]
async fn test_stream_poll_dynamic() {
  let chunks = vec![
    Ok(Bytes::from("a")),
    Ok(Bytes::from("b")),
    Ok(Bytes::from("c")),
  ];
  let stream = stream::iter(chunks);
  let belt = Belt::new(stream);
  let collected: Vec<_> = belt.collect::<Vec<_>>().await;
  assert_eq!(collected.len(), 3);
  assert_eq!(collected[0].as_ref().unwrap(), &Bytes::from("a"));
  assert_eq!(collected[1].as_ref().unwrap(), &Bytes::from("b"));
  assert_eq!(collected[2].as_ref().unwrap(), &Bytes::from("c"));
}

#[tokio::test]
async fn test_into_async_read() {
  let belt = Belt::from("hello async read");
  let mut reader = belt.into_async_read();
  let mut buffer = String::new();
  reader.read_to_string(&mut buffer).await.unwrap();
  assert_eq!(buffer, "hello async read");
}

#[tokio::test]
async fn test_into_async_read_with_stream() {
  let chunks = vec![
    Ok(Bytes::from("first ")),
    Ok(Bytes::from("second ")),
    Ok(Bytes::from("third")),
  ];
  let stream = stream::iter(chunks);
  let belt = Belt::new(stream);
  let mut reader = belt.into_async_read();
  let mut buffer = Vec::new();
  reader.read_to_end(&mut buffer).await.unwrap();
  assert_eq!(buffer, b"first second third");
}

#[tokio::test]
async fn test_into_async_read_error() {
  let chunks = vec![
    Ok(Bytes::from("start")),
    Err(io::Error::new(io::ErrorKind::ConnectionReset, "reset")),
  ];
  let stream = stream::iter(chunks);
  let belt = Belt::new(stream);
  let mut reader = belt.into_async_read();
  let mut buffer = Vec::new();
  let result = reader.read_to_end(&mut buffer).await;
  assert!(result.is_err());
  assert_eq!(result.unwrap_err().kind(), io::ErrorKind::ConnectionReset);
}

#[tokio::test]
async fn test_collect_bytes_empty_stream() {
  let stream = stream::iter(vec![]);
  let belt = Belt::new(stream);
  let result = belt.collect_bytes().await.unwrap();
  assert_eq!(result, Bytes::new());
}

#[tokio::test]
#[allow(clippy::cast_possible_truncation)]
async fn test_collect_bytes_large_data() {
  let chunk_size = 1024;
  let num_chunks = 100;
  let chunks: Vec<_> = (0..num_chunks)
    .map(|i| Ok(Bytes::from(vec![i as u8; chunk_size])))
    .collect();
  let stream = stream::iter(chunks);
  let belt = Belt::new(stream);
  let result = belt.collect_bytes().await.unwrap();
  assert_eq!(result.len(), chunk_size * num_chunks);
}

#[tokio::test]
async fn test_stream_consumed_once() {
  let belt = Belt::from("consume me");
  let mut stream = Box::pin(belt);

  // First poll should return data
  let first = stream.next().await;
  assert!(first.is_some());

  // Second poll should return None (static belt exhausted)
  let second = stream.next().await;
  assert!(second.is_none());
}
