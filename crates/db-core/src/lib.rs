//! Trait for a database-like interface for storing domain models.

mod error;

use model::{IndexValue, Model, RecordId};

pub use self::error::DatabaseError;

/// The specialized [`DatabaseLike`] result type.
pub type DatabaseResult<T> = Result<T, DatabaseError>;

/// A generic storage interface for models implementing the [`Model`] trait.
#[async_trait::async_trait]
pub trait DatabaseLike<M: Model>: Send + Sync {
  /// Initialize the storage schema for this model.
  async fn initialize_schema(&self) -> DatabaseResult<()>;

  /// Insert a new model into storage.
  async fn insert(&self, model: &M) -> DatabaseResult<()>;

  /// Update an existing model in storage.
  async fn update(&self, model: &M) -> DatabaseResult<()>;

  /// Insert a model if it doesn't exist, or update it if it does.
  ///
  /// Returns `true` if inserted, `false` if updated.
  async fn upsert(&self, model: &M) -> DatabaseResult<bool> {
    if self.exists(model.id()).await? {
      self.update(model).await?;
      Ok(false)
    } else {
      self.insert(model).await?;
      Ok(true)
    }
  }

  /// Delete a model from storage by ID.
  async fn delete(&self, id: RecordId<M>) -> DatabaseResult<()>;

  /// Delete a model from storage by ID, returning the deleted model.
  async fn delete_and_return(&self, id: RecordId<M>) -> DatabaseResult<M> {
    let model = self.get_or_error(id).await?;
    self.delete(id).await?;
    Ok(model)
  }

  /// Retrieve a model by its ID.
  async fn get(&self, id: RecordId<M>) -> DatabaseResult<Option<M>>;

  /// Retrieve a model by its ID, returning an error if not found.
  async fn get_or_error(&self, id: RecordId<M>) -> DatabaseResult<M> {
    self
      .get(id)
      .await?
      .ok_or_else(|| crate::DatabaseError::NotFound(id.to_string()))
  }

  /// Retrieve multiple models by their IDs in a single operation.
  async fn get_many(
    &self,
    ids: &[RecordId<M>],
  ) -> DatabaseResult<Vec<Option<M>>> {
    let mut results = Vec::with_capacity(ids.len());
    for id in ids {
      results.push(self.get(*id).await?);
    }
    Ok(results)
  }

  /// Find a single model by a unique index.
  async fn find_by_unique_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<Option<M>>;

  /// Find a model by a unique index, returning an error if not found.
  async fn find_by_unique_index_or_error(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<M> {
    self
      .find_by_unique_index(selector, key)
      .await?
      .ok_or_else(|| {
        crate::DatabaseError::NotFound(format!("{selector}={key}"))
      })
  }

  /// Find all models matching a non-unique index.
  async fn find_by_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<Vec<M>>;

  /// Find the first model matching a non-unique index.
  async fn find_one_by_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<Option<M>> {
    let mut results = self.find_by_index(selector, key).await?;
    Ok(results.drain(..).next())
  }

  /// List all models with pagination.
  async fn list(&self, limit: u32, offset: u32) -> DatabaseResult<Vec<M>>;

  /// List all models without pagination.
  async fn list_all(&self) -> DatabaseResult<Vec<M>> {
    self.list(u32::MAX, 0).await
  }

  /// Count the total number of records in storage.
  async fn count(&self) -> DatabaseResult<u64>;

  /// Count records matching a non-unique index.
  async fn count_by_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<u64> {
    Ok(
      self
        .find_by_index(selector, key)
        .await?
        .len()
        .try_into()
        .expect("failed to convert usize to u64"),
    )
  }

  /// Check if a record exists by ID.
  async fn exists(&self, id: RecordId<M>) -> DatabaseResult<bool>;

  /// Check if any records match a unique index key.
  async fn exists_by_unique_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<bool> {
    Ok(self.find_by_unique_index(selector, key).await?.is_some())
  }
}
