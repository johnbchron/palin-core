//! Postgres storage implementation for models.

use std::marker::PhantomData;

use db_core::{DatabaseError, DatabaseLike, DatabaseResult};
use miette::{Context, IntoDiagnostic, Report};
use model::{IndexDefinition, IndexValue, Model, RecordId};
use sqlx::{PgPool, Postgres, Row, postgres::PgRow};
use tracing::{debug, instrument, warn};

/// Postgres-backed storage for models implementing the [`Model`] trait.
#[derive(Clone)]
pub struct PostgresDatabase<M: Model> {
  pool:     PgPool,
  _phantom: PhantomData<M>,
}

impl<M: Model> PostgresDatabase<M> {
  /// Create a new PostgresDatabase with the given connection pool.
  #[instrument(fields(model = M::TABLE_NAME))]
  pub async fn new(url: &str) -> miette::Result<Self> {
    debug!("Creating PostgresDatabase for model");
    Ok(Self {
      pool:     PgPool::connect(url)
        .await
        .into_diagnostic()
        .context("failed to connect to database")?,
      _phantom: PhantomData,
    })
  }

  /// Initialize the database schema for this model.
  /// Creates the main table and all index tables.
  #[instrument(skip(self), fields(model = M::TABLE_NAME))]
  async fn initialize_schema(&self) -> DatabaseResult<()> {
    debug!("Initializing database schema...");

    self.create_main_table().await?;
    self.create_index_tables().await?;

    debug!("Schema initialization complete");
    Ok(())
  }

  /// Create the main data table.
  #[instrument(skip(self), fields(model = M::TABLE_NAME))]
  async fn create_main_table(&self) -> DatabaseResult<()> {
    debug!("Creating main table: {}", M::TABLE_NAME);

    let query = format!(
      "CREATE TABLE IF NOT EXISTS {table_name} (
          id TEXT PRIMARY KEY,
          data JSONB NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )",
      table_name = M::TABLE_NAME
    );

    sqlx::query(&query)
      .execute(&self.pool)
      .await
      .into_diagnostic()
      .context("failed to create main table")
      .map_err(DatabaseError::Other)?;

    // Create index on updated_at for efficient queries
    let index_query = format!(
      "CREATE INDEX IF NOT EXISTS idx_{table_name}_updated_at ON \
       {table_name}(updated_at)",
      table_name = M::TABLE_NAME
    );
    sqlx::query(&index_query)
      .execute(&self.pool)
      .await
      .into_diagnostic()
      .context("failed to create updated_at index")
      .map_err(DatabaseError::Other)?;

    debug!("Main table created successfully");
    Ok(())
  }

  /// Create index tables for all indices defined in the model.
  #[instrument(skip(self), fields(model = M::TABLE_NAME))]
  async fn create_index_tables(&self) -> DatabaseResult<()> {
    let indices = M::indices();
    let index_count = indices.definitions.len();

    debug!("Creating {} index tables", index_count);

    for def in indices.definitions {
      let index_table = Self::calculate_index_table_name(def);
      let query = format!(
        "CREATE TABLE IF NOT EXISTS {index_table} (
            index_key TEXT NOT NULL,
            record_id TEXT NOT NULL REFERENCES {table_name}(id) ON DELETE \
         CASCADE,
            {unique_constraint}
        )",
        table_name = M::TABLE_NAME,
        unique_constraint = if def.unique { "UNIQUE (index_key)" } else { "" },
      );

      sqlx::query(&query)
        .execute(&self.pool)
        .await
        .into_diagnostic()
        .with_context(|| {
          format!("Failed to create index table: {}", index_table)
        })
        .map_err(DatabaseError::Other)?;

      // Create index on index_key for efficient lookups
      let btree_index = format!(
        "CREATE INDEX IF NOT EXISTS idx_{index_table}_key ON \
         {index_table}(index_key)"
      );
      sqlx::query(&btree_index)
        .execute(&self.pool)
        .await
        .into_diagnostic()
        .with_context(|| {
          format!("Failed to create btree index on: {}", index_table)
        })
        .map_err(DatabaseError::Other)?;

      // For non-unique indices, also index by record_id for efficient deletion
      if !def.unique {
        let record_id_index = format!(
          "CREATE INDEX IF NOT EXISTS idx_{index_table}_record ON \
           {index_table}(record_id)"
        );
        sqlx::query(&record_id_index)
          .execute(&self.pool)
          .await
          .into_diagnostic()
          .with_context(|| {
            format!("Failed to create record_id index on: {}", index_table)
          })
          .map_err(DatabaseError::Other)?;
      }

      debug!(index_name = def.name, "Index table created");
    }

    debug!("All index tables created successfully");
    Ok(())
  }

  /// Insert a new model into the database.
  #[instrument(skip(self, model), fields(model = M::TABLE_NAME, id = %model.id()))]
  async fn insert(&self, model: &M) -> DatabaseResult<()> {
    debug!("Inserting model");

    let mut tx = self
      .pool
      .begin()
      .await
      .into_diagnostic()
      .map_err(DatabaseError::Database)?;

    let id = model.id().to_string();
    let data = serde_json::to_value(model)
      .into_diagnostic()
      .map_err(DatabaseError::Serialization)?;

    // Insert into main table
    let table_name = M::TABLE_NAME;
    let query =
      format!("INSERT INTO {} (id, data) VALUES ($1, $2)", table_name);

    sqlx::query(&query)
      .bind(&id)
      .bind(&data)
      .execute(&mut *tx)
      .await
      .into_diagnostic()
      .with_context(|| {
        format!("failed to insert into main table: {table_name}")
      })
      .map_err(DatabaseError::Database)?;

    self.insert_indices(&mut tx, model).await?;

    tx.commit()
      .await
      .into_diagnostic()
      .map_err(DatabaseError::Database)?;
    debug!("Model inserted successfully");
    Ok(())
  }

  /// Update an existing model in the database.
  #[instrument(skip(self, model), fields(model = M::TABLE_NAME, id = %model.id()))]
  async fn update(&self, model: &M) -> DatabaseResult<()> {
    debug!("Updating model");

    let mut tx = self
      .pool
      .begin()
      .await
      .into_diagnostic()
      .map_err(DatabaseError::Database)?;

    let id = model.id();
    let data = serde_json::to_value(model)
      .into_diagnostic()
      .map_err(DatabaseError::Serialization)?;

    // Update main table
    let table_name = M::TABLE_NAME;
    let query = format!(
      "UPDATE {} SET data = $1, updated_at = NOW() WHERE id = $2",
      table_name
    );

    let result = sqlx::query(&query)
      .bind(&data)
      .bind(id.to_string())
      .execute(&mut *tx)
      .await
      .into_diagnostic()
      .map_err(DatabaseError::Database)?;

    if result.rows_affected() == 0 {
      warn!("Update failed: record not found");
      return Err(DatabaseError::NotFound(id.to_string()));
    }

    // Delete old index entries
    self.delete_indices(&mut tx, id).await?;

    // Insert new index entries
    self.insert_indices(&mut tx, model).await?;

    tx.commit()
      .await
      .into_diagnostic()
      .map_err(DatabaseError::Database)?;
    debug!("Model updated successfully");
    Ok(())
  }

  /// Delete a model from the database by ID.
  #[instrument(skip(self), fields(model = M::TABLE_NAME, id = %id))]
  async fn delete(&self, id: RecordId<M>) -> DatabaseResult<()> {
    debug!("Deleting model");

    let table_name = M::TABLE_NAME;
    let query = format!("DELETE FROM {} WHERE id = $1", table_name);

    let result = sqlx::query(&query)
      .bind(id.to_string())
      .execute(&self.pool)
      .await
      .into_diagnostic()
      .map_err(DatabaseError::Database)?;

    if result.rows_affected() == 0 {
      warn!("Delete failed: record not found");
      return Err(DatabaseError::NotFound(id.to_string()));
    }

    // Index entries are automatically deleted via CASCADE
    debug!(
      rows_affected = result.rows_affected(),
      "Model deleted successfully"
    );
    Ok(())
  }

  /// Get a model by ID.
  #[instrument(skip(self), fields(model = M::TABLE_NAME, id = %id))]
  async fn get(&self, id: RecordId<M>) -> DatabaseResult<Option<M>> {
    debug!("Getting model by ID");

    let table_name = M::TABLE_NAME;
    let query = format!("SELECT data FROM {} WHERE id = $1", table_name);

    let row: Option<PgRow> = sqlx::query(&query)
      .bind(id.to_string())
      .fetch_optional(&self.pool)
      .await
      .into_diagnostic()
      .map_err(DatabaseError::Database)?;

    match row {
      Some(row) => {
        let data: serde_json::Value = row
          .try_get("data")
          .into_diagnostic()
          .map_err(DatabaseError::Serialization)?;
        let model: M = serde_json::from_value(data)
          .into_diagnostic()
          .map_err(DatabaseError::Serialization)?;

        debug!("Model found");
        Ok(Some(model))
      }
      None => {
        debug!("Model not found");
        Ok(None)
      }
    }
  }

  /// Find a model by a unique index.
  #[instrument(skip(self), fields(model = M::TABLE_NAME, selector = %selector, key = %key))]
  async fn find_by_unique_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<Option<M>> {
    debug!("Finding by unique index");

    let indices = M::indices();
    let index_def = indices
      .get(selector)
      .ok_or_else(|| DatabaseError::IndexNotFound(selector.to_string()))?;

    if !index_def.unique {
      return Err(DatabaseError::IndexNotUnique(selector.to_string()));
    }

    let table_name = M::TABLE_NAME;
    let index_table = Self::calculate_index_table_name(index_def);

    let query = format!(
      "SELECT m.data FROM {} m 
             INNER JOIN {} i ON m.id = i.record_id 
             WHERE i.index_key = $1",
      table_name, index_table
    );

    let row: Option<PgRow> = sqlx::query(&query)
      .bind(key.to_string())
      .fetch_optional(&self.pool)
      .await
      .into_diagnostic()
      .map_err(DatabaseError::Database)?;

    match row {
      Some(row) => {
        let data: serde_json::Value = row
          .try_get("data")
          .into_diagnostic()
          .map_err(DatabaseError::Serialization)?;
        let model: M = serde_json::from_value(data)
          .into_diagnostic()
          .map_err(DatabaseError::Serialization)?;
        debug!("Model found by unique index");
        Ok(Some(model))
      }
      None => {
        debug!("Model not found by unique index");
        Ok(None)
      }
    }
  }

  /// Find all models matching a non-unique index.
  #[instrument(skip(self), fields(model = M::TABLE_NAME, selector = %selector, key = %key))]
  async fn find_by_index(
    &self,
    selector: M::IndexSelector,
    key: &IndexValue,
  ) -> DatabaseResult<Vec<M>> {
    debug!("Finding by index");

    let indices = M::indices();
    let index_def = indices
      .get(selector)
      .ok_or_else(|| DatabaseError::IndexNotFound(selector.to_string()))?;

    let query = format!(
      "SELECT m.data FROM {table_name} m 
             INNER JOIN {index_table} i ON m.id = i.record_id 
             WHERE i.index_key = $1
             ORDER BY m.updated_at DESC",
      table_name = M::TABLE_NAME,
      index_table = Self::calculate_index_table_name(index_def)
    );

    let rows: Vec<PgRow> = sqlx::query(&query)
      .bind(key.to_string())
      .fetch_all(&self.pool)
      .await
      .into_diagnostic()
      .map_err(DatabaseError::Database)?;

    let count = rows.len();
    let mut results = Vec::with_capacity(count);

    for row in rows {
      let data: serde_json::Value = row
        .try_get("data")
        .into_diagnostic()
        .map_err(DatabaseError::Serialization)?;

      let model: M = serde_json::from_value(data)
        .into_diagnostic()
        .map_err(DatabaseError::Serialization)?;

      results.push(model);
    }

    debug!(count = count, "Found models by index");
    Ok(results)
  }

  /// List all models, ordered by updated_at descending.
  #[instrument(skip(self), fields(model = M::TABLE_NAME, limit = limit, offset = offset))]
  async fn list(&self, limit: u32, offset: u32) -> DatabaseResult<Vec<M>> {
    debug!("Listing models");

    let table_name = M::TABLE_NAME;
    let query = format!(
      "SELECT data FROM {} ORDER BY updated_at DESC LIMIT $1 OFFSET $2",
      table_name
    );

    let rows: Vec<PgRow> = sqlx::query(&query)
      .bind(limit as i64)
      .bind(offset as i64)
      .fetch_all(&self.pool)
      .await
      .into_diagnostic()
      .map_err(DatabaseError::Database)?;

    let count = rows.len();
    let mut results = Vec::with_capacity(count);

    for row in rows {
      let data: serde_json::Value = row
        .try_get("data")
        .into_diagnostic()
        .map_err(DatabaseError::Serialization)?;

      let model: M = serde_json::from_value(data)
        .into_diagnostic()
        .map_err(DatabaseError::Serialization)?;

      results.push(model);
    }

    debug!(count = count, "Listed models");
    Ok(results)
  }

  /// Count total number of records.
  #[instrument(skip(self), fields(model = M::TABLE_NAME))]
  async fn count(&self) -> DatabaseResult<i64> {
    debug!("Counting models");

    let table_name = M::TABLE_NAME;
    let query = format!("SELECT COUNT(*) as count FROM {}", table_name);

    let row: PgRow = sqlx::query(&query)
      .fetch_one(&self.pool)
      .await
      .into_diagnostic()
      .map_err(DatabaseError::Database)?;

    let count: i64 = row
      .try_get("count")
      .into_diagnostic()
      .map_err(DatabaseError::Serialization)?;

    debug!(count = count, "Model count retrieved");
    Ok(count)
  }

  /// Insert index entries for a model.
  #[instrument(skip(self, tx, model), fields(id = %model.id()))]
  async fn insert_indices(
    &self,
    tx: &mut sqlx::Transaction<'_, Postgres>,
    model: &M,
  ) -> DatabaseResult<()> {
    let id = model.id().to_string();
    let indices = M::indices();

    for def in indices.definitions {
      let index_table = Self::calculate_index_table_name(def);
      let values = def.extract(model);

      // For composite indices, concatenate values with a delimiter
      let index_key = Self::format_index_key(&values);

      let query = format!(
        "INSERT INTO {} (index_key, record_id) VALUES ($1, $2)",
        index_table
      );

      match sqlx::query(&query)
        .bind(&index_key)
        .bind(&id)
        .execute(&mut **tx)
        .await
      {
        Ok(_) => {}
        Err(e) if Self::is_unique_violation(&e) => {
          return Err(DatabaseError::UniqueViolation {
            index: def.name.to_string(),
            value: index_key,
          });
        }
        Err(e) => {
          return Err(DatabaseError::Database(Report::from_err(e)));
        }
      }
    }

    Ok(())
  }

  /// Delete all index entries for a record.
  #[instrument(skip(self, tx), fields(id = %id))]
  async fn delete_indices(
    &self,
    tx: &mut sqlx::Transaction<'_, Postgres>,
    id: RecordId<M>,
  ) -> DatabaseResult<()> {
    let indices = M::indices();

    for def in indices.definitions {
      let index_table = Self::calculate_index_table_name(def);
      let query = format!("DELETE FROM {} WHERE record_id = $1", index_table);

      sqlx::query(&query)
        .bind(id.to_string())
        .execute(&mut **tx)
        .await
        .into_diagnostic()
        .map_err(DatabaseError::Database)?;
    }

    Ok(())
  }

  /// Calculate table name for a given index.
  fn calculate_index_table_name(index_def: &IndexDefinition<M>) -> String {
    format!("{}__idx_{}", M::TABLE_NAME, index_def.name)
  }

  /// Format index values into a single key string.
  /// For composite indices, values are separated by a null byte.
  fn format_index_key(values: &[IndexValue]) -> String {
    values
      .iter()
      .map(|v| v.to_string())
      .collect::<Vec<_>>()
      .join("\0")
  }

  /// Check if an error is a unique constraint violation.
  fn is_unique_violation(error: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = error {
      // Postgres unique violation error code is 23505
      if let Some(code) = db_err.code() {
        return code.as_ref() == "23505";
      }
    }
    false
  }
}

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

  async fn count(&self) -> DatabaseResult<i64> { self.count().await }

  async fn exists(&self, id: RecordId<M>) -> DatabaseResult<bool> {
    Ok(self.get(id).await?.is_some())
  }
}
