//! Provides a model store interface and implementers.

#[cfg(test)]
mod tests;

pub use db_core::{Store, StoreError, StoreResult};
pub use db_impl_mock::MockStore;
pub use db_impl_postgres::PostgresStore;
