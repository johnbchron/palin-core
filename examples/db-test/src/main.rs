//! Tests the `db` interface.

use db::Database;
use miette::{Context, IntoDiagnostic, Result};
use model::{IndexValue, Model, RecordId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Model)]
#[model(
  table = "users",
  index(name = "name_age", extract =
    |m| vec![IndexValue::new([m.name.clone(), m.age.to_string()])]
  ),
  index(name = "email", unique, extract =
    |m| vec![IndexValue::new_single(&m.email)]
  ),
  index(name = "name", extract =
    |m| vec![IndexValue::new_single(&m.name)]
  )
)]
struct User {
  #[model(id)]
  id:    RecordId<User>,
  email: String,
  name:  String,
  age:   u32,
}

#[tokio::main]
async fn main() -> Result<()> {
  let db = Database::<User>::new_postgres(
    &std::env::var("POSTGRES_URL")
      .into_diagnostic()
      .context("could not read `POSTGRES_URL` var")?,
  )
  .await
  .context("failed to connect to database")?;

  db.initialize_schema().await?;

  let user = User {
    id:    RecordId::new(),
    email: "jpicard@federation.gov".to_owned(),
    name:  "Jean-Luc Picard".to_owned(),
    age:   54,
  };

  db.insert(&user).await?;

  let retrieved_user = db.get(user.id).await?.unwrap();
  assert_eq!(user, retrieved_user);

  let retrieved_user = db
    .find_by_unique_index(
      UserIndexSelector::Email,
      &IndexValue::new_single(&user.email),
    )
    .await?
    .unwrap();
  assert_eq!(user, retrieved_user);

  let retrieved_user = db
    .find_one_by_index(
      UserIndexSelector::NameAge,
      &IndexValue::new([user.name.clone(), user.age.to_string()]),
    )
    .await?
    .unwrap();
  assert_eq!(user, retrieved_user);

  // increment age
  let user = User {
    age: user.age + 1,
    ..user
  };

  db.update(&user).await?;

  let retrieved_user = db.get(user.id).await?.unwrap();
  assert_eq!(user, retrieved_user);

  Ok(())
}
