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
  let store = MockStore::<Unit>::new();
  let result = store.initialize_schema().await;
  assert!(result.is_ok());
}

// --- Basic CRUD Operations ---

#[tokio::test]
async fn test_insert_and_get() {
  let store = MockStore::<Unit>::new();
  let unit = Unit {
    id: RecordId::from_ulid_u128(1),
  };

  store.insert(&unit).await.unwrap();
  let retrieved = store.get(unit.id).await.unwrap();

  assert_eq!(retrieved, Some(unit));
}

#[tokio::test]
async fn test_insert_duplicate_id_fails() {
  let store = MockStore::<Unit>::new();
  let unit = Unit {
    id: RecordId::from_ulid_u128(1),
  };

  store.insert(&unit).await.unwrap();
  let result = store.insert(&unit).await;

  assert!(result.is_err());
}

#[tokio::test]
async fn test_insert_duplicate_unique_index_fails() {
  let store = MockStore::<User>::new();
  let user1 = create_user(1, "alice@example.com", "Alice", 30);
  let user2 = create_user(2, "alice@example.com", "Alice Clone", 25);

  store.insert(&user1).await.unwrap();
  let result = store.insert(&user2).await;

  assert!(matches!(result, Err(StoreError::UniqueViolation { .. })));
}

#[tokio::test]
async fn test_update_existing_record() {
  let store = MockStore::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  store.insert(&user).await.unwrap();

  let updated = create_user(1, "alice@example.com", "Alice Updated", 31);
  store.update(&updated).await.unwrap();

  let retrieved = store.get(user.id).await.unwrap().unwrap();
  assert_eq!(retrieved.name, "Alice Updated");
  assert_eq!(retrieved.age, 31);
}

#[tokio::test]
async fn test_update_nonexistent_record_fails() {
  let store = MockStore::<Unit>::new();
  let unit = Unit {
    id: RecordId::from_ulid_u128(999),
  };

  let result = store.update(&unit).await;
  assert!(matches!(result, Err(StoreError::NotFound(_))));
}

#[tokio::test]
async fn test_delete_existing_record() {
  let store = MockStore::<Unit>::new();
  let unit = Unit {
    id: RecordId::from_ulid_u128(1),
  };
  store.insert(&unit).await.unwrap();

  store.delete(unit.id).await.unwrap();

  let retrieved = store.get(unit.id).await.unwrap();
  assert_eq!(retrieved, None);
}

#[tokio::test]
async fn test_delete_nonexistent_record_fails() {
  let store = MockStore::<Unit>::new();
  let result = store.delete(RecordId::from_ulid_u128(999)).await;

  assert!(matches!(result, Err(StoreError::NotFound(_))));
}

#[tokio::test]
async fn test_get_nonexistent_record() {
  let store = MockStore::<Unit>::new();
  let result = store.get(RecordId::from_ulid_u128(999)).await.unwrap();

  assert_eq!(result, None);
}

// --- Upsert Operations ---

#[tokio::test]
async fn test_upsert_insert_path() {
  let store = MockStore::<Unit>::new();
  let unit = Unit {
    id: RecordId::from_ulid_u128(1),
  };

  let inserted = store.upsert(&unit).await.unwrap();

  assert!(inserted); // Should return true for insert
  let retrieved = store.get(unit.id).await.unwrap();
  assert_eq!(retrieved, Some(unit));
}

#[tokio::test]
async fn test_upsert_update_path() {
  let store = MockStore::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  store.insert(&user).await.unwrap();

  let updated = create_user(1, "alice@example.com", "Alice Updated", 31);
  let inserted = store.upsert(&updated).await.unwrap();

  assert!(!inserted); // Should return false for update
  let retrieved = store.get(user.id).await.unwrap().unwrap();
  assert_eq!(retrieved.name, "Alice Updated");
}

// --- Batch Operations ---

#[tokio::test]
async fn test_get_many() {
  let store = MockStore::<Unit>::new();
  let unit1 = Unit {
    id: RecordId::from_ulid_u128(1),
  };
  let unit2 = Unit {
    id: RecordId::from_ulid_u128(2),
  };
  let unit3 = Unit {
    id: RecordId::from_ulid_u128(3),
  };

  store.insert(&unit1).await.unwrap();
  store.insert(&unit2).await.unwrap();
  // unit3 not inserted

  let ids = vec![unit1.id, unit2.id, unit3.id];
  let results = store.get_many(&ids).await.unwrap();

  assert_eq!(results.len(), 3);
  assert_eq!(results[0], Some(unit1));
  assert_eq!(results[1], Some(unit2));
  assert_eq!(results[2], None);
}

#[tokio::test]
async fn test_get_many_empty() {
  let store = MockStore::<Unit>::new();
  let results = store.get_many(&[]).await.unwrap();

  assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_list_with_pagination() {
  let store = MockStore::<User>::new();
  for i in 1..=5 {
    let user = create_user(
      i,
      &format!("user{}@example.com", i),
      &format!("User{}", i),
      20 + i as u32,
    );
    store.insert(&user).await.unwrap();
  }

  let page1 = store.list(2, 0).await.unwrap();
  assert_eq!(page1.len(), 2);

  let page2 = store.list(2, 2).await.unwrap();
  assert_eq!(page2.len(), 2);

  let page3 = store.list(2, 4).await.unwrap();
  assert_eq!(page3.len(), 1);
}

#[tokio::test]
async fn test_list_all() {
  let store = MockStore::<User>::new();
  for i in 1..=5 {
    let user = create_user(
      i,
      &format!("user{}@example.com", i),
      &format!("User{}", i),
      20 + i as u32,
    );
    store.insert(&user).await.unwrap();
  }

  let all = store.list_all().await.unwrap();
  assert_eq!(all.len(), 5);
}

#[tokio::test]
async fn test_list_empty_store() {
  let store = MockStore::<Unit>::new();
  let results = store.list(10, 0).await.unwrap();

  assert_eq!(results.len(), 0);
}

// --- Index Queries ---

#[tokio::test]
async fn test_find_by_unique_index() {
  let store = MockStore::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  store.insert(&user).await.unwrap();

  let found = store
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
  let store = MockStore::<User>::new();

  let found = store
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
  let store = MockStore::<User>::new();
  let user1 = create_user(1, "alice1@example.com", "Alice", 30);
  let user2 = create_user(2, "alice2@example.com", "Alice", 25);
  let user3 = create_user(3, "bob@example.com", "Bob", 35);

  store.insert(&user1).await.unwrap();
  store.insert(&user2).await.unwrap();
  store.insert(&user3).await.unwrap();

  let found = store
    .find_by_index(UserIndexSelector::Name, &IndexValue::new("Alice"))
    .await
    .unwrap();

  assert_eq!(found.len(), 2);
  assert!(found.contains(&user1));
  assert!(found.contains(&user2));
}

#[tokio::test]
async fn test_find_by_index_empty() {
  let store = MockStore::<User>::new();

  let found = store
    .find_by_index(UserIndexSelector::Name, &IndexValue::new("NonExistent"))
    .await
    .unwrap();

  assert_eq!(found.len(), 0);
}

#[tokio::test]
async fn test_find_one_by_index() {
  let store = MockStore::<User>::new();
  let user1 = create_user(1, "alice1@example.com", "Alice", 30);
  let user2 = create_user(2, "alice2@example.com", "Alice", 25);

  store.insert(&user1).await.unwrap();
  store.insert(&user2).await.unwrap();

  let found = store
    .find_one_by_index(UserIndexSelector::Name, &IndexValue::new("Alice"))
    .await
    .unwrap();

  assert!(found.is_some());
  let found_user = found.unwrap();
  assert!(found_user == user1 || found_user == user2);
}

#[tokio::test]
async fn test_find_one_by_index_not_found() {
  let store = MockStore::<User>::new();

  let found = store
    .find_one_by_index(UserIndexSelector::Name, &IndexValue::new("NonExistent"))
    .await
    .unwrap();

  assert_eq!(found, None);
}

// --- Counting ---

#[tokio::test]
async fn test_count() {
  let store = MockStore::<User>::new();
  for i in 1..=5 {
    let user = create_user(
      i,
      &format!("user{}@example.com", i),
      &format!("User{}", i),
      20 + i as u32,
    );
    store.insert(&user).await.unwrap();
  }

  let count = store.count().await.unwrap();
  assert_eq!(count, 5);
}

#[tokio::test]
async fn test_count_empty() {
  let store = MockStore::<Unit>::new();
  let count = store.count().await.unwrap();

  assert_eq!(count, 0);
}

#[tokio::test]
async fn test_count_by_index() {
  let store = MockStore::<User>::new();
  let user1 = create_user(1, "alice1@example.com", "Alice", 30);
  let user2 = create_user(2, "alice2@example.com", "Alice", 25);
  let user3 = create_user(3, "bob@example.com", "Bob", 35);

  store.insert(&user1).await.unwrap();
  store.insert(&user2).await.unwrap();
  store.insert(&user3).await.unwrap();

  let count = store
    .count_by_index(UserIndexSelector::Name, &IndexValue::new("Alice"))
    .await
    .unwrap();

  assert_eq!(count, 2);
}

#[tokio::test]
async fn test_count_by_index_zero() {
  let store = MockStore::<User>::new();

  let count = store
    .count_by_index(UserIndexSelector::Name, &IndexValue::new("NonExistent"))
    .await
    .unwrap();

  assert_eq!(count, 0);
}

// --- Existence Checks ---

#[tokio::test]
async fn test_exists_true() {
  let store = MockStore::<Unit>::new();
  let unit = Unit {
    id: RecordId::from_ulid_u128(1),
  };
  store.insert(&unit).await.unwrap();

  let exists = store.exists(unit.id).await.unwrap();
  assert!(exists);
}

#[tokio::test]
async fn test_exists_false() {
  let store = MockStore::<Unit>::new();
  let exists = store.exists(RecordId::from_ulid_u128(999)).await.unwrap();

  assert!(!exists);
}

#[tokio::test]
async fn test_exists_by_unique_index_true() {
  let store = MockStore::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  store.insert(&user).await.unwrap();

  let exists = store
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
  let store = MockStore::<User>::new();

  let exists = store
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
  let store = MockStore::<Unit>::new();
  let unit = Unit {
    id: RecordId::from_ulid_u128(1),
  };
  store.insert(&unit).await.unwrap();

  let retrieved = store.get_or_error(unit.id).await.unwrap();
  assert_eq!(retrieved, unit);
}

#[tokio::test]
async fn test_get_or_error_not_found() {
  let store = MockStore::<Unit>::new();
  let result = store.get_or_error(RecordId::from_ulid_u128(999)).await;

  assert!(matches!(result, Err(StoreError::NotFound(_))));
}

#[tokio::test]
async fn test_delete_and_return() {
  let store = MockStore::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  store.insert(&user).await.unwrap();

  let deleted = store.delete_and_return(user.id).await.unwrap();
  assert_eq!(deleted, user);

  let exists = store.exists(user.id).await.unwrap();
  assert!(!exists);
}

#[tokio::test]
async fn test_delete_and_return_not_found() {
  let store = MockStore::<Unit>::new();
  let result = store.delete_and_return(RecordId::from_ulid_u128(999)).await;

  assert!(matches!(result, Err(StoreError::NotFound(_))));
}

#[tokio::test]
async fn test_find_by_unique_index_or_error_success() {
  let store = MockStore::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  store.insert(&user).await.unwrap();

  let found = store
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
  let store = MockStore::<User>::new();

  let result = store
    .find_by_unique_index_or_error(
      UserIndexSelector::Email,
      &IndexValue::new("nonexistent@example.com"),
    )
    .await;

  assert!(matches!(result, Err(StoreError::NotFound(_))));
}

// --- Edge Cases & Complex Scenarios ---

#[tokio::test]
async fn test_update_changes_index() {
  let store = MockStore::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  store.insert(&user).await.unwrap();

  // Update changes the indexed field
  let updated = create_user(1, "alice@example.com", "Alicia", 30);
  store.update(&updated).await.unwrap();

  // Old index should not find it
  let found_old = store
    .find_by_index(UserIndexSelector::Name, &IndexValue::new("Alice"))
    .await
    .unwrap();
  assert_eq!(found_old.len(), 0);

  // New index should find it
  let found_new = store
    .find_by_index(UserIndexSelector::Name, &IndexValue::new("Alicia"))
    .await
    .unwrap();
  assert_eq!(found_new.len(), 1);
}

#[tokio::test]
async fn test_delete_removes_from_indexes() {
  let store = MockStore::<User>::new();
  let user = create_user(1, "alice@example.com", "Alice", 30);
  store.insert(&user).await.unwrap();

  store.delete(user.id).await.unwrap();

  let found_by_email = store
    .find_by_unique_index(
      UserIndexSelector::Email,
      &IndexValue::new("alice@example.com"),
    )
    .await
    .unwrap();
  assert_eq!(found_by_email, None);

  let found_by_name = store
    .find_by_index(UserIndexSelector::Name, &IndexValue::new("Alice"))
    .await
    .unwrap();
  assert_eq!(found_by_name.len(), 0);
}

#[tokio::test]
async fn test_concurrent_operations() {
  let store = std::sync::Arc::new(MockStore::<User>::new());

  let mut handles = vec![];
  for i in 0..10 {
    let store_clone = store.clone();
    let handle = tokio::spawn(async move {
      let user = create_user(
        i,
        &format!("user{}@example.com", i),
        &format!("User{}", i),
        20 + i as u32,
      );
      store_clone.insert(&user).await
    });
    handles.push(handle);
  }

  for handle in handles {
    handle.await.unwrap().unwrap();
  }

  let count = store.count().await.unwrap();
  assert_eq!(count, 10);
}
