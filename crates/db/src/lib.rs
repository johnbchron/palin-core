//! Provides a model database interface and implementers.

#[cfg(test)]
mod tests;

use core::fmt;
use std::sync::Arc;

pub use db_core::DatabaseError;
use db_core::{DatabaseLike, DatabaseResult};
use db_impl_mock::MockDatabase;
use db_impl_postgres::PostgresDatabase;
use model::{IndexValue, Model, RecordId};

/// A domain model database.
#[derive(Clone)]
pub struct Database<M> {
  inner: Arc<dyn DatabaseLike<M>>,
}

impl<M> fmt::Debug for Database<M> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Database")
      .field("inner", &format_args!("_"))
      .finish()
  }
}

impl<M: Model> Database<M> {
  /// Create a new database backed by a mock store.
  #[must_use]
  pub fn new_mock() -> Self {
    Self {
      inner: Arc::new(MockDatabase::new()),
    }
  }

  /// Create a new database backed by a `PostgreSQL` store.
  pub async fn new_postgres(url: &str) -> miette::Result<Self> {
    Ok(Self {
      inner: Arc::new(PostgresDatabase::new(url).await?),
    })
  }

  /// Initialize the storage schema for this model.
  pub async fn initialize_schema(&self) -> DatabaseResult<()> {
    self.inner.initialize_schema().await
  }
  /// Insert a new model into storage.
  pub async fn insert(&self, model: &M) -> DatabaseResult<()> {
    self.inner.insert(model).await
  }
  /// Update an existing model in storage.
  pub async fn update(&self, model: &M) -> DatabaseResult<()> {
    self.inner.update(model).await
  }
  /// Insert a model if it doesn't exist, or update it if it does.
  ///
  /// Returns `true` if inserted, `false` if updated.
  pub async fn upsert(&self, model: &M) -> DatabaseResult<bool> {
    self.inner.upsert(model).await
  }
  /// Delete a model from storage by ID.
  pub async fn delete(&self, id: RecordId<M>) -> DatabaseResult<()> {
    self.inner.delete(id).await
  }
  /// Delete a model from storage by ID, returning the deleted model.
  pub async fn delete_and_return(&self, id: RecordId<M>) -> DatabaseResult<M> {
    self.inner.delete_and_return(id).await
  }
  /// Retrieve a model by its ID.
  pub async fn get(&self, id: RecordId<M>) -> DatabaseResult<Option<M>> {
    self.inner.get(id).await
  }
  /// Retrieve a model by its ID, returning an error if not found.
  pub async fn get_or_error(&self, id: RecordId<M>) -> DatabaseResult<M> {
    self.inner.get_or_error(id).await
  }
  /// Retrieve multiple models by their IDs in a single operation.
  pub async fn get_many(
    &self,
    ids: &[RecordId<M>],
  ) -> DatabaseResult<Vec<Option<M>>> {
    self.inner.get_many(ids).await
  }
  /// Find a single model by a unique index.
  pub async fn find_by_unique_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<Option<M>> {
    self.inner.find_by_unique_index(selector, key).await
  }
  /// Find a model by a unique index, returning an error if not found.
  pub async fn find_by_unique_index_or_error(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<M> {
    self
      .inner
      .find_by_unique_index_or_error(selector, key)
      .await
  }
  /// Find all models matching a non-unique index.
  pub async fn find_by_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<Vec<M>> {
    self.inner.find_by_index(selector, key).await
  }
  /// Find the first model matching a non-unique index.
  pub async fn find_one_by_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<Option<M>> {
    self.inner.find_one_by_index(selector, key).await
  }
  /// List all models with pagination.
  pub async fn list(&self, limit: u32, offset: u32) -> DatabaseResult<Vec<M>> {
    self.inner.list(limit, offset).await
  }
  /// List all models without pagination.
  pub async fn list_all(&self) -> DatabaseResult<Vec<M>> {
    self.inner.list_all().await
  }
  /// Count the total number of records in storage.
  pub async fn count(&self) -> DatabaseResult<u64> { self.inner.count().await }
  /// Count records matching a non-unique index.
  pub async fn count_by_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<u64> {
    self.inner.count_by_index(selector, key).await
  }
  /// Check if a record exists by ID.
  pub async fn exists(&self, id: RecordId<M>) -> DatabaseResult<bool> {
    self.inner.exists(id).await
  }
  /// Check if any records match a unique index key.
  pub async fn exists_by_unique_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<bool> {
    self.inner.exists_by_unique_index(selector, key).await
  }
}
