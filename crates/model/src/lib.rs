//! Provides the [`Model`] trait.
//!
//! The [`Model`] trait must be implemented for a type to be used as a domain
//! data model. Use the `#[derive(Model)]` macro to automatically implement it.

use std::fmt::{self, Debug, Display};

pub use model_derive::Model;
pub use record_id::RecordId;
use serde::{Serialize, de::DeserializeOwned};

/// Represents a model in the database.
pub trait Model:
  Clone + Debug + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static
{
  /// The table name in the database.
  const TABLE_NAME: &'static str;

  /// The index selector type for this model.
  type IndexSelector: Display + Debug + Clone + Copy + Send + Sync + 'static;

  /// Returns the registry of all indices (both unique and non-unique).
  fn indices() -> &'static IndexRegistry<Self>;

  /// Returns the model's ID.
  fn id(&self) -> RecordId<Self>;
}

/// Registry containing all index definitions for a model.
pub struct IndexRegistry<M: 'static> {
  /// The index definitions.
  pub definitions: &'static [IndexDefinition<M>],
}

impl<M> IndexRegistry<M> {
  /// Create a new index registry.
  #[must_use]
  pub const fn new(definitions: &'static [IndexDefinition<M>]) -> Self {
    Self { definitions }
  }

  /// Get an index definition by selector.
  pub fn get<S>(&self, selector: S) -> Option<&IndexDefinition<M>>
  where
    S: Display,
  {
    let selector_str = selector.to_string();
    self.definitions.iter().find(|def| def.name == selector_str)
  }

  /// Get all unique indices.
  pub fn unique_indices(&self) -> impl Iterator<Item = &IndexDefinition<M>> {
    self.definitions.iter().filter(|def| def.unique)
  }

  /// Get all non-unique indices.
  pub fn non_unique_indices(
    &self,
  ) -> impl Iterator<Item = &IndexDefinition<M>> {
    self.definitions.iter().filter(|def| !def.unique)
  }
}

/// An index value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexValue(Vec<String>);

impl fmt::Display for IndexValue {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let json = serde_json::to_string(&self.0).map_err(|_| fmt::Error)?;
    f.write_str(&json)
  }
}

impl IndexValue {
  /// Creates a new [`IndexValue`].
  pub fn new<I: IntoIterator<Item = T>, T: AsRef<str>>(input: I) -> Self {
    IndexValue(input.into_iter().map(|i| i.as_ref().to_owned()).collect())
  }

  /// Creates a new [`IndexValue`] from a single segment.
  pub fn new_single<T: AsRef<str>>(input: T) -> Self {
    IndexValue(vec![input.as_ref().to_owned()])
  }
}

/// Definition of a single index (can be simple or composite).
pub struct IndexDefinition<M> {
  /// The name of the index (matches the selector variant name in snake case).
  pub name:      &'static str,
  /// Whether this is a unique index.
  pub unique:    bool,
  /// Function to extract the index value(s) from a model instance.
  pub extractor: fn(&M) -> Vec<IndexValue>,
}

impl<M> IndexDefinition<M> {
  /// Create a new index definition.
  pub const fn new(
    name: &'static str,
    unique: bool,
    extractor: fn(&M) -> Vec<IndexValue>,
  ) -> Self {
    Self {
      name,
      unique,
      extractor,
    }
  }

  /// Extract the index value from a model instance.
  pub fn extract(&self, model: &M) -> Vec<IndexValue> {
    (self.extractor)(model)
  }
}

// #[cfg(test)]
// mod test {
//   #![allow(dead_code)]
//   use serde::{Deserialize, Serialize};

//   use super::*;

//   #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Model)]
//   #[model(table = "unit")]
//   struct Unit {
//     #[model(id)]
//     id: RecordId<Unit>,
//   }

//   #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Model)]
//   #[model(table = "users")]
//   #[model(composite_index(
//     name = "name_age",
//     extract = |m| vec![
//       IndexValue::new(m.name.clone()),
//       IndexValue::new(format!("{}", m.age))
//   ]))]
//   struct User {
//     #[model(id)]
//     id: RecordId<User>,

//     #[model(unique)]
//     email: String,

//     #[model(index)]
//     name: String,

//     age: u32,
//   }
// }
