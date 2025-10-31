mod sanitize;

use std::fmt;

use serde::{Deserialize, Serialize};

use self::sanitize::lax_slugify;

/// A laxer slug generated from arbitrary text.
///
/// "Lax slugs" can contain any combination of characters from the regex set
/// `[-.+_0-9a-zA-Z]`, which is:
/// - a-z (lowercase)
/// - A-Z (uppercase)
/// - 0-9 (numbers)
/// - '+' (plus)
/// - '.' (dot)
/// - '_' (underscore)
/// - '-' (dash)
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
pub struct LaxSlug(Box<str>);

impl LaxSlug {
  /// Slugifies the input and creates a new slug.
  pub fn new(input: &str) -> Self { Self(lax_slugify(input).into_boxed_str()) }
  /// Creates a new slug without first slugifying the input.
  ///
  /// # Safety
  /// Guarantees are broken if input is not first verified with
  /// [`LaxSlug::validate`] or otherwise known to be valid.
  pub unsafe fn new_unchecked(input: &str) -> Self {
    Self(input.to_owned().into_boxed_str())
  }

  /// Validates that a string slice is already slugified.
  pub fn validate(input: &str) -> bool { lax_slugify(input) == input }

  /// Returns a reference to the slug contents.
  pub fn as_str(&self) -> &str { &self.0 }
}

// formatting
impl fmt::Display for LaxSlug {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(&self.0)
  }
}
impl fmt::Debug for LaxSlug {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_tuple("Slug").field(&self.0).finish()
  }
}

// referencing
impl AsRef<str> for LaxSlug {
  fn as_ref(&self) -> &str { &self.0 }
}
impl std::ops::Deref for LaxSlug {
  type Target = str;
  fn deref(&self) -> &Self::Target { &self.0 }
}

// conversions
impl From<String> for LaxSlug {
  fn from(s: String) -> Self { Self::new(&s) }
}
impl From<&str> for LaxSlug {
  fn from(s: &str) -> Self { Self::new(s) }
}
impl std::str::FromStr for LaxSlug {
  type Err = std::convert::Infallible;
  fn from_str(s: &str) -> Result<Self, Self::Err> { Ok(Self::new(s)) }
}

// comparisons
impl PartialEq<str> for LaxSlug {
  fn eq(&self, other: &str) -> bool { self.0.as_ref() == other }
}
impl PartialEq<&str> for LaxSlug {
  fn eq(&self, other: &&str) -> bool { self.0.as_ref() == *other }
}
