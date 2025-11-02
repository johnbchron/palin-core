use db_core::{DatabaseError, DatabaseResult};
use miette::{Context, IntoDiagnostic, Report};
use model::{IndexDefinition, IndexValue, Model, RecordId};
use sqlx::Postgres;
use tracing::{debug, instrument};

use crate::PostgresDatabase;

impl<M: Model> PostgresDatabase<M> {
  /// Create index tables for all indices defined in the model.
  #[instrument(skip(tx), fields(model = M::TABLE_NAME))]
  pub(crate) async fn create_index_tables(
    tx: &mut sqlx::Transaction<'_, Postgres>,
  ) -> DatabaseResult<()> {
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
        .execute(&mut **tx)
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
        .execute(&mut **tx)
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
          .execute(&mut **tx)
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

  /// Insert index entries for a model.
  #[instrument(skip(self, tx, model), fields(id = %model.id()))]
  pub(crate) async fn insert_indices(
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
  pub(crate) async fn delete_indices(
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
  pub(crate) fn calculate_index_table_name(
    index_def: &IndexDefinition<M>,
  ) -> String {
    format!("{}__idx_{}", M::TABLE_NAME, index_def.name)
  }

  /// Format index values into a single key string.
  /// For composite indices, values are separated by a null byte.
  pub(crate) fn format_index_key(values: &[IndexValue]) -> String {
    values
      .iter()
      .map(|v| v.to_string())
      .collect::<Vec<_>>()
      .join("\0")
  }
}
