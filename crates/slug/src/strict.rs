mod sanitize;

use std::fmt;

use serde::{Deserialize, Serialize};

use self::sanitize::strict_slugify;

/// A URL-safe slug generated from arbitrary text.
///
/// Slugs are sanitized using strict slugification rules:
/// - Converts to lowercase
/// - Replaces spaces and special characters with hyphens
/// - Removes consecutive hyphens
///
/// # Examples
/// ```
/// # use slug::Slug;
/// let slug = Slug::new("Hello World!");
/// assert_eq!(slug.as_str(), "hello-world");
/// ```
#[derive(
  Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Slug(Box<str>);

impl Slug {
  /// Slugifies the input and creates a new slug.
  #[must_use]
  pub fn new(input: &str) -> Self {
    Self(strict_slugify(input).into_boxed_str())
  }
  /// Creates a new slug without first slugifying the input.
  #[must_use]
  pub fn new_unchecked(input: &str) -> Self {
    Self(input.to_owned().into_boxed_str())
  }

  /// Validates that a string slice is already slugified.
  #[must_use]
  pub fn validate(input: &str) -> bool { strict_slugify(input) == input }

  /// Returns a reference to the slug contents.
  #[must_use]
  pub fn as_str(&self) -> &str { &self.0 }
}

// formatting
impl fmt::Display for Slug {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(&self.0)
  }
}
impl fmt::Debug for Slug {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_tuple("Slug").field(&self.0).finish()
  }
}

// referencing
impl AsRef<str> for Slug {
  fn as_ref(&self) -> &str { &self.0 }
}
impl std::ops::Deref for Slug {
  type Target = str;
  fn deref(&self) -> &Self::Target { &self.0 }
}

// conversions
impl From<String> for Slug {
  fn from(s: String) -> Self { Self::new(&s) }
}
impl From<&str> for Slug {
  fn from(s: &str) -> Self { Self::new(s) }
}
impl std::str::FromStr for Slug {
  type Err = std::convert::Infallible;
  fn from_str(s: &str) -> Result<Self, Self::Err> { Ok(Self::new(s)) }
}

// comparisons
impl PartialEq<str> for Slug {
  fn eq(&self, other: &str) -> bool { self.0.as_ref() == other }
}
impl PartialEq<&str> for Slug {
  fn eq(&self, other: &&str) -> bool { self.0.as_ref() == *other }
}
