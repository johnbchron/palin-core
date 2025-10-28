use model::{IndexValue, Model, RecordId};
use serde::{Deserialize, Serialize};

use super::*;

// Test models
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Model)]
#[model(table = "unit")]
struct Unit {
  #[model(id)]
  id: RecordId<Unit>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Model)]
#[model(table = "users")]
#[model(composite_index(
    name = "name_age",
    extract = |m| vec![
      IndexValue::new(m.name.clone()),
      IndexValue::new(format!("{}", m.age))
  ]))]
struct User {
  #[model(id)]
  id: RecordId<User>,

  #[model(unique)]
  email: String,

  #[model(index)]
  name: String,

  age: u32,
}

// Helper to create test users
fn create_user(id: u128, email: &str, name: &str, age: u32) -> User {
  User {
    id: RecordId::from_ulid_u128(id),
    email: email.to_string(),
    name: name.to_string(),
    age,
  }
}

// --- Schema & Initialization ---

#[tokio::test]
async fn test_initialize_schema() {
  let db = MockDatabase::<Unit>::new();
  let result = db.initialize_schema().await;
  assert!(result.is_ok());
}

// --- Basic CRUD Operations ---

#[tokio::test]
async fn test_insert_and_get() {
  let db = MockDatabase::<Unit>::new();
  let unit = Unit {
    id: RecordId::from_ulid_u128(1),
  };

  db.insert(&unit).await.unwrap();
  let retrieved = db.get(unit.id).await.unwrap();

  assert_eq!(retrieved, Some(unit));
}

#[tokio::test]
async fn test_insert_duplicate_id_fails() {
  let db = MockDatabase::<Unit>::new();
  let unit = Unit {
    id: RecordId::from_ulid_u128(1),
  };

  db.insert(&unit).await.unwrap();
  let result = db.insert(&unit).await;

  assert!(result.is_err());
}

#[tokio::test]
async fn test_insert_duplicate_unique_index_fails() {
  let db = MockDatabase::<User>::new();
  let user1 = create_user(1, "alice@example.com", "Alice", 30);
  let user2 = create_user(2, "alice@example.com", "Alice Clone", 25);

  db.insert(&user1).await.unwrap();
  let result = db.insert(&user2).await;

  assert!(matches!(result, Err(DatabaseError::UniqueViolation { .. })));
}

#[tokio::test]
async fn test_update_existing_record() {
  let db = MockDatabase::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  db.insert(&user).await.unwrap();

  let updated = create_user(1, "alice@example.com", "Alice Updated", 31);
  db.update(&updated).await.unwrap();

  let retrieved = db.get(user.id).await.unwrap().unwrap();
  assert_eq!(retrieved.name, "Alice Updated");
  assert_eq!(retrieved.age, 31);
}

#[tokio::test]
async fn test_update_nonexistent_record_fails() {
  let db = MockDatabase::<Unit>::new();
  let unit = Unit {
    id: RecordId::from_ulid_u128(999),
  };

  let result = db.update(&unit).await;
  assert!(matches!(result, Err(DatabaseError::NotFound(_))));
}

#[tokio::test]
async fn test_delete_existing_record() {
  let db = MockDatabase::<Unit>::new();
  let unit = Unit {
    id: RecordId::from_ulid_u128(1),
  };
  db.insert(&unit).await.unwrap();

  db.delete(unit.id).await.unwrap();

  let retrieved = db.get(unit.id).await.unwrap();
  assert_eq!(retrieved, None);
}

#[tokio::test]
async fn test_delete_nonexistent_record_fails() {
  let db = MockDatabase::<Unit>::new();
  let result = db.delete(RecordId::from_ulid_u128(999)).await;

  assert!(matches!(result, Err(DatabaseError::NotFound(_))));
}

#[tokio::test]
async fn test_get_nonexistent_record() {
  let db = MockDatabase::<Unit>::new();
  let result = db.get(RecordId::from_ulid_u128(999)).await.unwrap();

  assert_eq!(result, None);
}

// --- Upsert Operations ---

#[tokio::test]
async fn test_upsert_insert_path() {
  let db = MockDatabase::<Unit>::new();
  let unit = Unit {
    id: RecordId::from_ulid_u128(1),
  };

  let inserted = db.upsert(&unit).await.unwrap();

  assert!(inserted); // Should return true for insert
  let retrieved = db.get(unit.id).await.unwrap();
  assert_eq!(retrieved, Some(unit));
}

#[tokio::test]
async fn test_upsert_update_path() {
  let db = MockDatabase::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  db.insert(&user).await.unwrap();

  let updated = create_user(1, "alice@example.com", "Alice Updated", 31);
  let inserted = db.upsert(&updated).await.unwrap();

  assert!(!inserted); // Should return false for update
  let retrieved = db.get(user.id).await.unwrap().unwrap();
  assert_eq!(retrieved.name, "Alice Updated");
}

// --- Batch Operations ---

#[tokio::test]
async fn test_get_many() {
  let db = MockDatabase::<Unit>::new();
  let unit1 = Unit {
    id: RecordId::from_ulid_u128(1),
  };
  let unit2 = Unit {
    id: RecordId::from_ulid_u128(2),
  };
  let unit3 = Unit {
    id: RecordId::from_ulid_u128(3),
  };

  db.insert(&unit1).await.unwrap();
  db.insert(&unit2).await.unwrap();
  // unit3 not inserted

  let ids = vec![unit1.id, unit2.id, unit3.id];
  let results = db.get_many(&ids).await.unwrap();

  assert_eq!(results.len(), 3);
  assert_eq!(results[0], Some(unit1));
  assert_eq!(results[1], Some(unit2));
  assert_eq!(results[2], None);
}

#[tokio::test]
async fn test_get_many_empty() {
  let db = MockDatabase::<Unit>::new();
  let results = db.get_many(&[]).await.unwrap();

  assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_list_with_pagination() {
  let db = MockDatabase::<User>::new();
  for i in 1..=5 {
    let user = create_user(
      i,
      &format!("user{}@example.com", i),
      &format!("User{}", i),
      20 + i as u32,
    );
    db.insert(&user).await.unwrap();
  }

  let page1 = db.list(2, 0).await.unwrap();
  assert_eq!(page1.len(), 2);

  let page2 = db.list(2, 2).await.unwrap();
  assert_eq!(page2.len(), 2);

  let page3 = db.list(2, 4).await.unwrap();
  assert_eq!(page3.len(), 1);
}

#[tokio::test]
async fn test_list_all() {
  let db = MockDatabase::<User>::new();
  for i in 1..=5 {
    let user = create_user(
      i,
      &format!("user{}@example.com", i),
      &format!("User{}", i),
      20 + i as u32,
    );
    db.insert(&user).await.unwrap();
  }

  let all = db.list_all().await.unwrap();
  assert_eq!(all.len(), 5);
}

#[tokio::test]
async fn test_list_empty_db() {
  let db = MockDatabase::<Unit>::new();
  let results = db.list(10, 0).await.unwrap();

  assert_eq!(results.len(), 0);
}

// --- Index Queries ---

#[tokio::test]
async fn test_find_by_unique_index() {
  let db = MockDatabase::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  db.insert(&user).await.unwrap();

  let found = db
    .find_by_unique_index(
      UserIndexSelector::Email,
      &IndexValue::new("alice@example.com"),
    )
    .await
    .unwrap();

  assert_eq!(found, Some(user));
}

#[tokio::test]
async fn test_find_by_unique_index_not_found() {
  let db = MockDatabase::<User>::new();

  let found = db
    .find_by_unique_index(
      UserIndexSelector::Email,
      &IndexValue::new("nonexistent@example.com"),
    )
    .await
    .unwrap();

  assert_eq!(found, None);
}

#[tokio::test]
async fn test_find_by_index() {
  let db = MockDatabase::<User>::new();
  let user1 = create_user(1, "alice1@example.com", "Alice", 30);
  let user2 = create_user(2, "alice2@example.com", "Alice", 25);
  let user3 = create_user(3, "bob@example.com", "Bob", 35);

  db.insert(&user1).await.unwrap();
  db.insert(&user2).await.unwrap();
  db.insert(&user3).await.unwrap();

  let found = db
    .find_by_index(UserIndexSelector::Name, &IndexValue::new("Alice"))
    .await
    .unwrap();

  assert_eq!(found.len(), 2);
  assert!(found.contains(&user1));
  assert!(found.contains(&user2));
}

#[tokio::test]
async fn test_find_by_index_empty() {
  let db = MockDatabase::<User>::new();

  let found = db
    .find_by_index(UserIndexSelector::Name, &IndexValue::new("NonExistent"))
    .await
    .unwrap();

  assert_eq!(found.len(), 0);
}

#[tokio::test]
async fn test_find_one_by_index() {
  let db = MockDatabase::<User>::new();
  let user1 = create_user(1, "alice1@example.com", "Alice", 30);
  let user2 = create_user(2, "alice2@example.com", "Alice", 25);

  db.insert(&user1).await.unwrap();
  db.insert(&user2).await.unwrap();

  let found = db
    .find_one_by_index(UserIndexSelector::Name, &IndexValue::new("Alice"))
    .await
    .unwrap();

  assert!(found.is_some());
  let found_user = found.unwrap();
  assert!(found_user == user1 || found_user == user2);
}

#[tokio::test]
async fn test_find_one_by_index_not_found() {
  let db = MockDatabase::<User>::new();

  let found = db
    .find_one_by_index(UserIndexSelector::Name, &IndexValue::new("NonExistent"))
    .await
    .unwrap();

  assert_eq!(found, None);
}

// --- Counting ---

#[tokio::test]
async fn test_count() {
  let db = MockDatabase::<User>::new();
  for i in 1..=5 {
    let user = create_user(
      i,
      &format!("user{}@example.com", i),
      &format!("User{}", i),
      20 + i as u32,
    );
    db.insert(&user).await.unwrap();
  }

  let count = db.count().await.unwrap();
  assert_eq!(count, 5);
}

#[tokio::test]
async fn test_count_empty() {
  let db = MockDatabase::<Unit>::new();
  let count = db.count().await.unwrap();

  assert_eq!(count, 0);
}

#[tokio::test]
async fn test_count_by_index() {
  let db = MockDatabase::<User>::new();
  let user1 = create_user(1, "alice1@example.com", "Alice", 30);
  let user2 = create_user(2, "alice2@example.com", "Alice", 25);
  let user3 = create_user(3, "bob@example.com", "Bob", 35);

  db.insert(&user1).await.unwrap();
  db.insert(&user2).await.unwrap();
  db.insert(&user3).await.unwrap();

  let count = db
    .count_by_index(UserIndexSelector::Name, &IndexValue::new("Alice"))
    .await
    .unwrap();

  assert_eq!(count, 2);
}

#[tokio::test]
async fn test_count_by_index_zero() {
  let db = MockDatabase::<User>::new();

  let count = db
    .count_by_index(UserIndexSelector::Name, &IndexValue::new("NonExistent"))
    .await
    .unwrap();

  assert_eq!(count, 0);
}

// --- Existence Checks ---

#[tokio::test]
async fn test_exists_true() {
  let db = MockDatabase::<Unit>::new();
  let unit = Unit {
    id: RecordId::from_ulid_u128(1),
  };
  db.insert(&unit).await.unwrap();

  let exists = db.exists(unit.id).await.unwrap();
  assert!(exists);
}

#[tokio::test]
async fn test_exists_false() {
  let db = MockDatabase::<Unit>::new();
  let exists = db.exists(RecordId::from_ulid_u128(999)).await.unwrap();

  assert!(!exists);
}

#[tokio::test]
async fn test_exists_by_unique_index_true() {
  let db = MockDatabase::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  db.insert(&user).await.unwrap();

  let exists = db
    .exists_by_unique_index(
      UserIndexSelector::Email,
      &IndexValue::new("alice@example.com"),
    )
    .await
    .unwrap();

  assert!(exists);
}

#[tokio::test]
async fn test_exists_by_unique_index_false() {
  let db = MockDatabase::<User>::new();

  let exists = db
    .exists_by_unique_index(
      UserIndexSelector::Email,
      &IndexValue::new("nonexistent@example.com"),
    )
    .await
    .unwrap();

  assert!(!exists);
}

// --- Helper Methods ---

#[tokio::test]
async fn test_get_or_error_success() {
  let db = MockDatabase::<Unit>::new();
  let unit = Unit {
    id: RecordId::from_ulid_u128(1),
  };
  db.insert(&unit).await.unwrap();

  let retrieved = db.get_or_error(unit.id).await.unwrap();
  assert_eq!(retrieved, unit);
}

#[tokio::test]
async fn test_get_or_error_not_found() {
  let db = MockDatabase::<Unit>::new();
  let result = db.get_or_error(RecordId::from_ulid_u128(999)).await;

  assert!(matches!(result, Err(DatabaseError::NotFound(_))));
}

#[tokio::test]
async fn test_delete_and_return() {
  let db = MockDatabase::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  db.insert(&user).await.unwrap();

  let deleted = db.delete_and_return(user.id).await.unwrap();
  assert_eq!(deleted, user);

  let exists = db.exists(user.id).await.unwrap();
  assert!(!exists);
}

#[tokio::test]
async fn test_delete_and_return_not_found() {
  let db = MockDatabase::<Unit>::new();
  let result = db.delete_and_return(RecordId::from_ulid_u128(999)).await;

  assert!(matches!(result, Err(DatabaseError::NotFound(_))));
}

#[tokio::test]
async fn test_find_by_unique_index_or_error_success() {
  let db = MockDatabase::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  db.insert(&user).await.unwrap();

  let found = db
    .find_by_unique_index_or_error(
      UserIndexSelector::Email,
      &IndexValue::new("alice@example.com"),
    )
    .await
    .unwrap();

  assert_eq!(found, user);
}

#[tokio::test]
async fn test_find_by_unique_index_or_error_not_found() {
  let db = MockDatabase::<User>::new();

  let result = db
    .find_by_unique_index_or_error(
      UserIndexSelector::Email,
      &IndexValue::new("nonexistent@example.com"),
    )
    .await;

  assert!(matches!(result, Err(DatabaseError::NotFound(_))));
}

// --- Edge Cases & Complex Scenarios ---

#[tokio::test]
async fn test_update_changes_index() {
  let db = MockDatabase::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  db.insert(&user).await.unwrap();

  // Update changes the indexed field
  let updated = create_user(1, "alice@example.com", "Alicia", 30);
  db.update(&updated).await.unwrap();

  // Old index should not find it
  let found_old = db
    .find_by_index(UserIndexSelector::Name, &IndexValue::new("Alice"))
    .await
    .unwrap();
  assert_eq!(found_old.len(), 0);

  // New index should find it
  let found_new = db
    .find_by_index(UserIndexSelector::Name, &IndexValue::new("Alicia"))
    .await
    .unwrap();
  assert_eq!(found_new.len(), 1);
}

#[tokio::test]
async fn test_delete_removes_from_indexes() {
  let db = MockDatabase::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  db.insert(&user).await.unwrap();

  db.delete(user.id).await.unwrap();

  let found_by_email = db
    .find_by_unique_index(
      UserIndexSelector::Email,
      &IndexValue::new("alice@example.com"),
    )
    .await
    .unwrap();
  assert_eq!(found_by_email, None);

  let found_by_name = db
    .find_by_index(UserIndexSelector::Name, &IndexValue::new("Alice"))
    .await
    .unwrap();
  assert_eq!(found_by_name.len(), 0);
}

#[tokio::test]
async fn test_concurrent_operations() {
  let db = std::sync::Arc::new(MockDatabase::<User>::new());

  let mut handles = vec![];
  for i in 0..10 {
    let db_clone = db.clone();
    let handle = tokio::spawn(async move {
      let user = create_user(
        i,
        &format!("user{}@example.com", i),
        &format!("User{}", i),
        20 + i as u32,
      );
      db_clone.insert(&user).await
    });
    handles.push(handle);
  }

  for handle in handles {
    handle.await.unwrap().unwrap();
  }

  let count = db.count().await.unwrap();
  assert_eq!(count, 10);
}
