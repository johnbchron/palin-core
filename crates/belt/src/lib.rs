//! Provides a streaming bytes container.

use std::{
  io,
  pin::Pin,
  task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures::{TryStreamExt, stream::Stream};
use tokio_util::io::StreamReader;

/// An opaque container for streaming bytes data.
pub struct Belt {
  inner: Inner,
}

enum Inner {
  Static(Option<Bytes>),
  Dynamic(Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>>),
}

impl Belt {
  /// Create from any stream of `Bytes`.
  pub fn new<S>(stream: S) -> Self
  where
    S: Stream<Item = Result<Bytes, io::Error>> + Send + 'static,
  {
    Self {
      inner: Inner::Dynamic(Box::pin(stream)),
    }
  }

  /// Create an empty stream.
  pub const fn empty() -> Self {
    Self {
      inner: Inner::Static(None),
    }
  }

  /// Collect a [`Belt`] into a single [`Bytes`].
  pub async fn collect_bytes(self) -> Result<Bytes, io::Error> {
    self
      .try_fold(BytesMut::new(), |mut acc, x| async move {
        acc.extend_from_slice(&x[..]);
        Ok(acc)
      })
      .await
      .map(BytesMut::freeze)
  }

  /// Convert into an [`AsyncRead`](tokio::io::AsyncRead) implementer.
  pub fn into_async_read(self) -> StreamReader<Belt, Bytes> {
    StreamReader::new(self)
  }
}

impl Stream for Belt {
  type Item = Result<Bytes, io::Error>;

  fn poll_next(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
  ) -> Poll<Option<Self::Item>> {
    match &mut self.inner {
      Inner::Static(bytes) => Poll::Ready(bytes.take().map(Ok)),
      Inner::Dynamic(stream) => stream.as_mut().poll_next(cx),
    }
  }
}

impl From<Bytes> for Belt {
  fn from(bytes: Bytes) -> Self {
    Self {
      inner: Inner::Static(Some(bytes)),
    }
  }
}

impl From<Vec<u8>> for Belt {
  fn from(vec: Vec<u8>) -> Self { Bytes::from(vec).into() }
}

impl From<&'static [u8]> for Belt {
  fn from(slice: &'static [u8]) -> Self { Bytes::from_static(slice).into() }
}

impl From<&'static str> for Belt {
  fn from(s: &'static str) -> Self { Bytes::from_static(s.as_bytes()).into() }
}
