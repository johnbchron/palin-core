use std::fmt;

/// An index value used in the [`Model`](super::Model) trait.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexValue(String);

impl fmt::Display for IndexValue {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(&self.0)
  }
}

impl IndexValue {
  /// Creates a new [`IndexValue`].
  pub fn new(input: impl AsRef<str>) -> Self {
    let input = input.as_ref();
    let mut output = String::with_capacity(input.len());

    for char in input.chars() {
      match char {
        ':' => {
          output.push('-');
        }
        c => {
          output.push(c);
        }
      }
    }

    IndexValue(output)
  }
}

impl From<String> for IndexValue {
  fn from(value: String) -> Self { IndexValue::new(value) }
}
