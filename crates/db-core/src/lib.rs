//! Trait for a database-like interface for storing domain models.
//! Generic store trait for model persistence.

mod error;

use model::{IndexValue, Model, RecordId};

pub use self::error::StoreError;

/// The specialized [`Store`] result type.
pub type StoreResult<T> = Result<T, StoreError>;

/// A generic storage interface for models implementing the `Model` trait.
#[async_trait::async_trait]
pub trait Store<M: Model>: Send + Sync {
  /// Initialize the storage schema for this model.
  async fn initialize_schema(&self) -> StoreResult<()>;

  /// Insert a new model into storage.
  async fn insert(&self, model: &M) -> StoreResult<()>;

  /// Update an existing model in storage.
  async fn update(&self, model: &M) -> StoreResult<()>;

  /// Insert a model if it doesn't exist, or update it if it does.
  ///
  /// Returns `true` if inserted, `false` if updated.
  async fn upsert(&self, model: &M) -> StoreResult<bool> {
    match self.exists(model.id()).await? {
      true => {
        self.update(model).await?;
        Ok(false)
      }
      false => {
        self.insert(model).await?;
        Ok(true)
      }
    }
  }

  /// Delete a model from storage by ID.
  async fn delete(&self, id: RecordId<M>) -> StoreResult<()>;

  /// Delete a model from storage by ID, returning the deleted model.
  async fn delete_and_return(&self, id: RecordId<M>) -> StoreResult<M> {
    let model = self.get_or_error(id).await?;
    self.delete(id).await?;
    Ok(model)
  }

  /// Retrieve a model by its ID.
  async fn get(&self, id: RecordId<M>) -> StoreResult<Option<M>>;

  /// Retrieve a model by its ID, returning an error if not found.
  async fn get_or_error(&self, id: RecordId<M>) -> StoreResult<M> {
    self
      .get(id)
      .await?
      .ok_or_else(|| crate::StoreError::NotFound(id.to_string()))
  }

  /// Retrieve multiple models by their IDs in a single operation.
  async fn get_many(&self, ids: &[RecordId<M>]) -> StoreResult<Vec<Option<M>>> {
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
  ) -> StoreResult<Option<M>>;

  /// Find a model by a unique index, returning an error if not found.
  async fn find_by_unique_index_or_error(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> StoreResult<M> {
    self
      .find_by_unique_index(selector, key)
      .await?
      .ok_or_else(|| {
        crate::StoreError::NotFound(format!("{}={}", selector, key))
      })
  }

  /// Find all models matching a non-unique index.
  async fn find_by_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> StoreResult<Vec<M>>;

  /// Find the first model matching a non-unique index.
  async fn find_one_by_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> StoreResult<Option<M>> {
    let mut results = self.find_by_index(selector, key).await?;
    Ok(results.drain(..).next())
  }

  /// List all models with pagination.
  async fn list(&self, limit: u32, offset: u32) -> StoreResult<Vec<M>>;

  /// List all models without pagination.
  async fn list_all(&self) -> StoreResult<Vec<M>> {
    self.list(u32::MAX, 0).await
  }

  /// Count the total number of records in storage.
  async fn count(&self) -> StoreResult<i64>;

  /// Count records matching a non-unique index.
  async fn count_by_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> StoreResult<i64> {
    Ok(self.find_by_index(selector, key).await?.len() as i64)
  }

  /// Check if a record exists by ID.
  async fn exists(&self, id: RecordId<M>) -> StoreResult<bool>;

  /// Check if any records match a unique index key.
  async fn exists_by_unique_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> StoreResult<bool> {
    Ok(self.find_by_unique_index(selector, key).await?.is_some())
  }
}
