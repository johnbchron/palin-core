//! Provides a streaming bytes container.

#[cfg(test)]
mod tests;

use std::{
  fmt, io,
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures::{TryStreamExt, stream::Stream};
use tokio::io::AsyncBufRead;
use tokio_util::io::{ReaderStream, StreamReader};

/// An opaque container for streaming bytes data.
pub struct Belt {
  inner: Inner,
  count: Arc<AtomicU64>,
}

impl fmt::Debug for Belt {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Belt").finish()
  }
}

enum Inner {
  Static(Option<Bytes>),
  Dynamic(Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>>),
}

impl Belt {
  /// Create from any stream of [`Bytes`].
  #[must_use]
  pub fn new<S>(stream: S) -> Self
  where
    S: Stream<Item = Result<Bytes, io::Error>> + Send + 'static,
  {
    Self {
      inner: Inner::Dynamic(Box::pin(stream)),
      count: Arc::new(AtomicU64::new(0)),
    }
  }

  /// Create from a [`Bytes`].
  #[must_use]
  pub fn new_from_bytes(input: Bytes) -> Self {
    Self {
      inner: Inner::Static(Some(input)),
      count: Arc::new(AtomicU64::new(0)),
    }
  }

  /// Create from a slice of bytes.
  #[must_use]
  pub fn new_from_slice(input: &[u8]) -> Self {
    Self::new_from_bytes(Bytes::copy_from_slice(input))
  }

  /// Create from a slice of bytes.
  #[must_use]
  pub fn new_from_static_slice(input: &'static [u8]) -> Self {
    Self::new_from_bytes(Bytes::from_static(input))
  }

  /// Create a stream from an [`AsyncBufRead`](tokio::io::AsyncBufRead)
  /// implementer.
  #[must_use]
  pub fn new_from_async_buf_read<R>(reader: R) -> Self
  where
    R: AsyncBufRead + Send + 'static,
  {
    Self::new(ReaderStream::new(reader))
  }

  /// Create an empty stream.
  #[must_use]
  pub fn empty() -> Self {
    Self {
      inner: Inner::Static(None),
      count: Arc::new(AtomicU64::new(0)),
    }
  }

  /// Get a counter that tracks the byte count traversing this [`Belt`] for
  /// its lifetime.
  pub fn counter(&self) -> Counter { Counter::new(self.count.clone()) }

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
    // poll stream
    let result = match &mut self.inner {
      Inner::Static(bytes) => Poll::Ready(bytes.take().map(Ok)),
      Inner::Dynamic(stream) => stream.as_mut().poll_next(cx),
    };

    // increment byte counter
    if let Poll::Ready(Some(Ok(ref bytes))) = result {
      self.count.fetch_add(bytes.len() as u64, Ordering::Relaxed);
    }

    result
  }
}

impl From<Bytes> for Belt {
  fn from(bytes: Bytes) -> Self { Self::new_from_bytes(bytes) }
}

impl From<Vec<u8>> for Belt {
  fn from(vec: Vec<u8>) -> Self { Self::new_from_bytes(Bytes::from(vec)) }
}

impl From<&'static [u8]> for Belt {
  fn from(slice: &'static [u8]) -> Self { Self::new_from_static_slice(slice) }
}

impl From<&'static str> for Belt {
  fn from(s: &'static str) -> Self { Belt::new_from_static_slice(s.as_bytes()) }
}

/// A counter returned by a [`Belt`]
pub struct Counter(Arc<AtomicU64>);

impl Counter {
  pub(crate) const fn new(count: Arc<AtomicU64>) -> Self { Self(count) }

  /// Gets the byte count contained in this counter.
  #[must_use]
  pub fn get(&self) -> u64 { self.0.load(Ordering::Relaxed) }
}
