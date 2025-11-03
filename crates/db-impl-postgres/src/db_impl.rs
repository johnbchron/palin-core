use db_core::{DatabaseLike, DatabaseResult};
use model::{IndexValue, Model, RecordId};

use crate::PostgresDatabase;

#[async_trait::async_trait]
impl<M: Model> DatabaseLike<M> for PostgresDatabase<M> {
  async fn initialize_schema(&self) -> DatabaseResult<()> {
    self.initialize_schema().await
  }

  async fn insert(&self, model: &M) -> DatabaseResult<()> {
    self.insert(model).await
  }

  async fn update(&self, model: &M) -> DatabaseResult<()> {
    self.update(model).await
  }

  async fn delete(&self, id: RecordId<M>) -> DatabaseResult<()> {
    self.delete(id).await
  }

  async fn delete_and_return(&self, id: RecordId<M>) -> DatabaseResult<M> {
    let model = self.get_or_error(id).await?;
    self.delete(id).await?;
    Ok(model)
  }

  async fn get(&self, id: RecordId<M>) -> DatabaseResult<Option<M>> {
    self.get(id).await
  }

  async fn find_by_unique_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<Option<M>> {
    self.find_by_unique_index(selector, key).await
  }

  async fn find_by_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<Vec<M>> {
    self.find_by_index(selector, key).await
  }

  async fn list(&self, limit: u32, offset: u32) -> DatabaseResult<Vec<M>> {
    self.list(limit, offset).await
  }

  async fn count(&self) -> DatabaseResult<u64> { self.count().await }

  async fn exists(&self, id: RecordId<M>) -> DatabaseResult<bool> {
    Ok(self.get(id).await?.is_some())
  }
}
