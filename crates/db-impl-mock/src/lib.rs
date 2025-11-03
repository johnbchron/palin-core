//! Mock storage implementation for testing.

use std::{
  collections::HashMap,
  marker::PhantomData,
  sync::{Arc, RwLock},
};

use db_core::{DatabaseError, DatabaseLike, DatabaseResult};
use model::{IndexValue, Model, RecordId};

/// In-memory mock database for testing models implementing the [`Model`] trait.
#[derive(Clone)]
pub struct MockDatabase<M: Model> {
  inner:    Arc<RwLock<MockDatabaseInner<M>>>,
  _phantom: PhantomData<M>,
}

struct MockDatabaseInner<M: Model> {
  /// Main data storage: id -> model
  data:        HashMap<RecordId<M>, M>,
  /// Index storage: (`index_name`, `index_key`) -> `Vec<record_id>`
  indices:     HashMap<(String, String), Vec<RecordId<M>>>,
  /// Tracks whether schema has been initialized
  initialized: bool,
}

impl<M: Model> Default for MockDatabase<M> {
  fn default() -> Self { Self::new() }
}

impl<M: Model> MockDatabase<M> {
  /// Create a new `MockDatabase`.
  #[must_use]
  pub fn new() -> Self {
    Self {
      inner:    Arc::new(RwLock::new(MockDatabaseInner {
        data:        HashMap::new(),
        indices:     HashMap::new(),
        initialized: false,
      })),
      _phantom: PhantomData,
    }
  }

  /// Initialize the mock schema (marks as initialized).
  pub fn initialize_schema(&self) -> DatabaseResult<()> {
    let mut inner = self.inner.write().unwrap();
    inner.initialized = true;
    Ok(())
  }

  /// Insert a new model into the mock database.
  pub fn insert(&self, model: &M) -> DatabaseResult<()> {
    let mut inner = self.inner.write().unwrap();

    // Check if record already exists
    if inner.data.contains_key(&model.id()) {
      return Err(DatabaseError::Database(miette::miette!(
        "Record with id {} already exists",
        model.id()
      )));
    }

    // Check unique index violations before inserting
    Self::check_unique_violations(&inner, model, None)?;

    // Insert the model
    inner.data.insert(model.id(), model.clone());

    // Insert index entries
    Self::insert_indices_inner(&mut inner, model);

    Ok(())
  }

  /// Update an existing model in the mock database.
  pub fn update(&self, model: &M) -> DatabaseResult<()> {
    let mut inner = self.inner.write().unwrap();

    // Check if record exists
    if !inner.data.contains_key(&model.id()) {
      return Err(DatabaseError::NotFound(model.id().to_string()));
    }

    // Check unique index violations (excluding current record)
    Self::check_unique_violations(&inner, model, Some(model.id()))?;

    // Delete old index entries
    Self::delete_indices_inner(&mut inner, model.id());

    // Update the model
    inner.data.insert(model.id(), model.clone());

    // Insert new index entries
    Self::insert_indices_inner(&mut inner, model);

    Ok(())
  }

  /// Delete a model from the mock database by ID.
  pub fn delete(&self, id: RecordId<M>) -> DatabaseResult<()> {
    let mut inner = self.inner.write().unwrap();

    // Check if record exists
    if !inner.data.contains_key(&id) {
      return Err(DatabaseError::NotFound(id.to_string()));
    }

    // Remove from main storage
    inner.data.remove(&id);

    // Remove from indices
    Self::delete_indices_inner(&mut inner, id);

    Ok(())
  }

  /// Get a model by ID.
  pub fn get(&self, id: RecordId<M>) -> DatabaseResult<Option<M>> {
    let inner = self.inner.read().unwrap();
    Ok(inner.data.get(&id).cloned())
  }

  /// Get a model by ID, returning an error if not found.
  pub fn get_or_error(&self, id: RecordId<M>) -> DatabaseResult<M> {
    self
      .get(id)?
      .ok_or_else(|| DatabaseError::NotFound(id.to_string()))
  }

  /// Find a model by a unique index.
  pub fn find_by_unique_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<Option<M>> {
    let inner = self.inner.read().unwrap();

    let indices = M::indices();
    let index_def = indices
      .get(selector)
      .ok_or_else(|| DatabaseError::IndexNotFound(selector.to_string()))?;

    if !index_def.unique {
      return Err(DatabaseError::IndexNotUnique(selector.to_string()));
    }

    let index_key = (index_def.name.to_string(), key.to_string());

    if let Some(record_ids) = inner.indices.get(&index_key)
      && let Some(record_id) = record_ids.first()
    {
      return Ok(inner.data.get(record_id).cloned());
    }

    Ok(None)
  }

  /// Find a model by a unique index, returning an error if not found.
  pub fn find_by_unique_index_or_error(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<M> {
    self
      .find_by_unique_index(selector, key)?
      .ok_or_else(|| DatabaseError::NotFound(format!("{selector}={key}")))
  }

  /// Find all models matching a non-unique index.
  pub fn find_by_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<Vec<M>> {
    let inner = self.inner.read().unwrap();

    let indices = M::indices();
    let index_def = indices
      .get(selector)
      .ok_or_else(|| DatabaseError::IndexNotFound(selector.to_string()))?;

    let index_key = (index_def.name.to_string(), key.to_string());

    let mut results = Vec::new();

    if let Some(record_ids) = inner.indices.get(&index_key) {
      for record_id in record_ids {
        if let Some(model) = inner.data.get(record_id) {
          results.push(model.clone());
        }
      }
    }

    Ok(results)
  }

  /// List all models (no specific ordering in mock).
  pub fn list(&self, limit: u32, offset: u32) -> DatabaseResult<Vec<M>> {
    let inner = self.inner.read().unwrap();

    let results: Vec<M> = inner
      .data
      .values()
      .skip(offset as usize)
      .take(limit as usize)
      .cloned()
      .collect();

    Ok(results)
  }

  /// Count total number of records.
  pub fn count(&self) -> DatabaseResult<u64> {
    let inner = self.inner.read().unwrap();
    Ok(inner.data.len().try_into().unwrap())
  }

  /// Check if a record exists by ID.
  pub fn exists(&self, id: RecordId<M>) -> DatabaseResult<bool> {
    let inner = self.inner.read().unwrap();
    Ok(inner.data.contains_key(&id))
  }

  /// Clear all data (useful for test cleanup).
  pub fn clear(&self) {
    let mut inner = self.inner.write().unwrap();
    inner.data.clear();
    inner.indices.clear();
  }

  /// Get the number of records (synchronous version for testing).
  #[must_use]
  pub fn len(&self) -> usize {
    let inner = self.inner.read().unwrap();
    inner.data.len()
  }

  /// Check if the database is empty (synchronous version for testing).
  #[must_use]
  pub fn is_empty(&self) -> bool { self.len() == 0 }

  // Helper methods

  fn check_unique_violations(
    inner: &MockDatabaseInner<M>,
    model: &M,
    exclude_id: Option<RecordId<M>>,
  ) -> DatabaseResult<()> {
    let indices = M::indices();

    for def in indices.definitions {
      if !def.unique {
        continue;
      }

      let values = def.extract(model);
      let index_key_str = Self::format_index_key(&values);
      let index_key = (def.name.to_string(), index_key_str.clone());

      if let Some(existing_ids) = inner.indices.get(&index_key) {
        // Check if any existing ID is different from the one we're updating
        for existing_id in existing_ids {
          if Some(existing_id) != exclude_id.as_ref() {
            return Err(DatabaseError::UniqueViolation {
              index: def.name.to_string(),
              value: index_key_str,
            });
          }
        }
      }
    }

    Ok(())
  }

  fn insert_indices_inner(inner: &mut MockDatabaseInner<M>, model: &M) {
    let indices = M::indices();

    for def in indices.definitions {
      let values = def.extract(model);
      let index_key_str = Self::format_index_key(&values);
      let index_key = (def.name.to_string(), index_key_str);

      inner.indices.entry(index_key).or_default().push(model.id());
    }
  }

  fn delete_indices_inner(inner: &mut MockDatabaseInner<M>, id: RecordId<M>) {
    // Remove all index entries for this record
    inner.indices.retain(|_, record_ids| {
      record_ids.retain(|rid| *rid != id);
      !record_ids.is_empty()
    });
  }

  fn format_index_key(values: &[IndexValue]) -> String {
    values
      .iter()
      .map(ToString::to_string)
      .collect::<Vec<_>>()
      .join("\0")
  }
}

#[async_trait::async_trait]
impl<M: Model> DatabaseLike<M> for MockDatabase<M> {
  async fn initialize_schema(&self) -> DatabaseResult<()> {
    self.initialize_schema()
  }

  async fn insert(&self, model: &M) -> DatabaseResult<()> { self.insert(model) }

  async fn update(&self, model: &M) -> DatabaseResult<()> { self.update(model) }

  async fn delete(&self, id: RecordId<M>) -> DatabaseResult<()> {
    self.delete(id)
  }

  async fn delete_and_return(&self, id: RecordId<M>) -> DatabaseResult<M> {
    let model = self.get_or_error(id)?;
    self.delete(id)?;
    Ok(model)
  }

  async fn get(&self, id: RecordId<M>) -> DatabaseResult<Option<M>> {
    self.get(id)
  }

  async fn find_by_unique_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<Option<M>> {
    self.find_by_unique_index(selector, key)
  }

  async fn find_by_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<Vec<M>> {
    self.find_by_index(selector, key)
  }

  async fn list(&self, limit: u32, offset: u32) -> DatabaseResult<Vec<M>> {
    self.list(limit, offset)
  }

  async fn count(&self) -> DatabaseResult<u64> { self.count() }

  async fn exists(&self, id: RecordId<M>) -> DatabaseResult<bool> {
    self.exists(id)
  }
}
