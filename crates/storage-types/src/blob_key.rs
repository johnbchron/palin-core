use std::fmt;

use serde::{Deserialize, Serialize};

/// The key used for a blob.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BlobKey(String);

impl BlobKey {
  /// Create a new blob key
  pub fn new(key: impl Into<String>) -> Self { Self(key.into()) }
  /// Get the key as a string slice
  #[must_use]
  pub fn as_str(&self) -> &str { &self.0 }
  /// Convert into inner String
  #[must_use]
  pub fn into_inner(self) -> String { self.0 }
}

impl fmt::Display for BlobKey {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl From<String> for BlobKey {
  fn from(s: String) -> Self { Self(s) }
}

impl From<&str> for BlobKey {
  fn from(s: &str) -> Self { Self(s.to_string()) }
}

impl AsRef<str> for BlobKey {
  fn as_ref(&self) -> &str { &self.0 }
}
