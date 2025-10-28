//! Provides a model database interface and implementers.

#[cfg(test)]
mod tests;

pub use db_core::{DatabaseError, DatabaseLike, DatabaseResult};
pub use db_impl_mock::MockDatabase;
pub use db_impl_postgres::PostgresDatabase;
