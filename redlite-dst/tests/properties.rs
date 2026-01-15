//! Property-based tests for Redlite using proptest
//!
//! These tests verify invariants that must hold for any input.
//! Run with: `PROPTEST_CASES=10000 cargo test properties`

use proptest::prelude::*;
use redlite::{Db, SetOptions, ZMember};

/// Generate a valid Redis key (alphanumeric + common separators)
fn arb_key() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9:_-]{0,63}".prop_filter("key must not be empty", |s| !s.is_empty())
}

/// Generate arbitrary binary data for values
fn arb_value() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..1024)
}

/// Generate a valid hash field name
fn arb_field() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_]{0,31}".prop_filter("field must not be empty", |s| !s.is_empty())
}

/// Generate a valid sorted set member
fn arb_member() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_-]{1,64}"
}

/// Generate a valid score for sorted sets
fn arb_score() -> impl Strategy<Value = f64> {
    prop_oneof![
        // Normal range
        (-1e10..1e10f64),
        // Edge cases
        Just(0.0),
        Just(-0.0),
    ]
}

// ============================================================================
// Property: SET/GET Roundtrip
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// SET k v; GET k => v (values are preserved exactly)
    #[test]
    fn prop_set_get_roundtrip(key in arb_key(), value in arb_value()) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        db.set(&key, &value, None).expect("SET failed");
        let retrieved = db.get(&key).expect("GET failed");

        prop_assert_eq!(retrieved, Some(value), "SET/GET roundtrip failed");
    }

    /// SET with NX only sets if key doesn't exist
    #[test]
    fn prop_set_nx_behavior(key in arb_key(), v1 in arb_value(), v2 in arb_value()) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        // First SET with NX should succeed
        let opts1 = SetOptions::new().nx();
        let result1 = db.set_opts(&key, &v1, opts1).expect("SET NX failed");
        prop_assert!(result1, "First SET NX should succeed");

        // Second SET with NX should fail (key exists)
        let opts2 = SetOptions::new().nx();
        let result2 = db.set_opts(&key, &v2, opts2).expect("SET NX failed");
        prop_assert!(!result2, "Second SET NX should fail");

        // Value should be the first one
        let retrieved = db.get(&key).expect("GET failed");
        prop_assert_eq!(retrieved, Some(v1));
    }

    /// SET with XX only sets if key exists
    #[test]
    fn prop_set_xx_behavior(key in arb_key(), v1 in arb_value(), v2 in arb_value()) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        // First SET with XX should fail (key doesn't exist)
        let opts1 = SetOptions::new().xx();
        let result1 = db.set_opts(&key, &v1, opts1).expect("SET XX failed");
        prop_assert!(!result1, "SET XX should fail when key doesn't exist");

        // Regular SET to create key
        db.set(&key, &v1, None).expect("SET failed");

        // Second SET with XX should succeed (key exists)
        let opts2 = SetOptions::new().xx();
        let result2 = db.set_opts(&key, &v2, opts2).expect("SET XX failed");
        prop_assert!(result2, "SET XX should succeed when key exists");

        // Value should be the second one
        let retrieved = db.get(&key).expect("GET failed");
        prop_assert_eq!(retrieved, Some(v2));
    }
}

// ============================================================================
// Property: INCR is Atomic and Monotonic
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// INCR always increases the value by exactly 1
    #[test]
    fn prop_incr_atomic(key in arb_key(), initial in -1000i64..1000i64, iterations in 1usize..50) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        // Set initial value
        db.set(&key, initial.to_string().as_bytes(), None).expect("SET failed");

        let mut last_value = initial;
        for _ in 0..iterations {
            let new_value = db.incr(&key).expect("INCR failed");
            prop_assert_eq!(new_value, last_value + 1, "INCR should increase by exactly 1");
            last_value = new_value;
        }

        // Final value should be initial + iterations
        prop_assert_eq!(last_value, initial + iterations as i64);
    }

    /// DECR always decreases the value by exactly 1
    #[test]
    fn prop_decr_atomic(key in arb_key(), initial in -1000i64..1000i64, iterations in 1usize..50) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        db.set(&key, initial.to_string().as_bytes(), None).expect("SET failed");

        let mut last_value = initial;
        for _ in 0..iterations {
            let new_value = db.decr(&key).expect("DECR failed");
            prop_assert_eq!(new_value, last_value - 1, "DECR should decrease by exactly 1");
            last_value = new_value;
        }

        prop_assert_eq!(last_value, initial - iterations as i64);
    }

    /// INCRBY adds the exact delta
    #[test]
    fn prop_incrby_exact(key in arb_key(), initial in -1000i64..1000i64, delta in -100i64..100i64) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        db.set(&key, initial.to_string().as_bytes(), None).expect("SET failed");
        let result = db.incrby(&key, delta).expect("INCRBY failed");

        prop_assert_eq!(result, initial + delta);
    }
}

// ============================================================================
// Property: List Ordering
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// RPUSH preserves insertion order (FIFO from left)
    #[test]
    fn prop_list_rpush_order(key in arb_key(), items in prop::collection::vec(arb_value(), 1..20)) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        // RPUSH all items one by one
        for item in &items {
            db.rpush(&key, &[item.as_slice()]).expect("RPUSH failed");
        }

        // LRANGE 0 -1 should return items in insertion order
        let retrieved = db.lrange(&key, 0, -1).expect("LRANGE failed");
        prop_assert_eq!(retrieved, items, "RPUSH should preserve order");
    }

    /// LPUSH preserves reverse insertion order (LIFO from left)
    #[test]
    fn prop_list_lpush_order(key in arb_key(), items in prop::collection::vec(arb_value(), 1..20)) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        // LPUSH all items one by one
        for item in &items {
            db.lpush(&key, &[item.as_slice()]).expect("LPUSH failed");
        }

        // LRANGE 0 -1 should return items in reverse insertion order
        let retrieved = db.lrange(&key, 0, -1).expect("LRANGE failed");
        let expected: Vec<_> = items.into_iter().rev().collect();
        prop_assert_eq!(retrieved, expected, "LPUSH should reverse order");
    }

    /// LPOP returns the leftmost element
    #[test]
    fn prop_list_lpop_left(key in arb_key(), items in prop::collection::vec(arb_value(), 2..10)) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        for item in &items {
            db.rpush(&key, &[item.as_slice()]).expect("RPUSH failed");
        }

        let popped = db.lpop(&key, None).expect("LPOP failed");
        prop_assert_eq!(popped, vec![items[0].clone()], "LPOP should return leftmost");
    }

    /// RPOP returns the rightmost element
    #[test]
    fn prop_list_rpop_right(key in arb_key(), items in prop::collection::vec(arb_value(), 2..10)) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        for item in &items {
            db.rpush(&key, &[item.as_slice()]).expect("RPUSH failed");
        }

        let popped = db.rpop(&key, None).expect("RPOP failed");
        prop_assert_eq!(popped, vec![items.last().unwrap().clone()], "RPOP should return rightmost");
    }

    /// LLEN returns correct count
    #[test]
    fn prop_list_llen(key in arb_key(), items in prop::collection::vec(arb_value(), 0..50)) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        for item in &items {
            db.rpush(&key, &[item.as_slice()]).expect("RPUSH failed");
        }

        let len = db.llen(&key).expect("LLEN failed");
        prop_assert_eq!(len as usize, items.len());
    }
}

// ============================================================================
// Property: Set Uniqueness
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// SADD maintains uniqueness (duplicates are ignored)
    #[test]
    fn prop_set_uniqueness(key in arb_key(), members in prop::collection::vec(arb_member(), 1..30)) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        for member in &members {
            db.sadd(&key, &[member.as_bytes()]).expect("SADD failed");
        }

        // Adding duplicates shouldn't increase cardinality
        for member in &members {
            db.sadd(&key, &[member.as_bytes()]).expect("SADD duplicate failed");
        }

        let card = db.scard(&key).expect("SCARD failed");
        let unique_count = members.iter().collect::<std::collections::HashSet<_>>().len();
        prop_assert_eq!(card as usize, unique_count, "Set should contain unique members only");
    }

    /// SISMEMBER correctly reports membership
    #[test]
    fn prop_set_ismember(key in arb_key(), members in prop::collection::vec(arb_member(), 1..20)) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        for member in &members {
            db.sadd(&key, &[member.as_bytes()]).expect("SADD failed");
        }

        // All added members should be present
        for member in &members {
            let is_member = db.sismember(&key, member.as_bytes()).expect("SISMEMBER failed");
            prop_assert!(is_member, "Added member should be in set");
        }

        // Non-existent member should not be present
        let is_member = db.sismember(&key, b"__nonexistent__").expect("SISMEMBER failed");
        prop_assert!(!is_member, "Non-existent member should not be in set");
    }

    /// SREM removes only the specified member
    #[test]
    fn prop_set_srem(key in arb_key(), members in prop::collection::vec(arb_member(), 2..10)) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        let unique: Vec<_> = members.iter().collect::<std::collections::HashSet<_>>().into_iter().collect();
        if unique.len() < 2 {
            return Ok(());
        }

        for member in &unique {
            db.sadd(&key, &[member.as_bytes()]).expect("SADD failed");
        }

        let initial_card = db.scard(&key).expect("SCARD failed");

        // Remove first member
        db.srem(&key, &[unique[0].as_bytes()]).expect("SREM failed");

        let final_card = db.scard(&key).expect("SCARD failed");
        prop_assert_eq!(final_card, initial_card - 1);

        // Removed member should not be present
        let is_member = db.sismember(&key, unique[0].as_bytes()).expect("SISMEMBER failed");
        prop_assert!(!is_member);
    }
}

// ============================================================================
// Property: Sorted Set Score Ordering
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// ZRANGE returns elements in ascending score order
    #[test]
    fn prop_zset_score_ordering(
        key in arb_key(),
        items in prop::collection::vec((arb_member(), arb_score()), 2..20)
    ) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        // Add all items
        for (member, score) in &items {
            if score.is_finite() {
                let zmember = ZMember::new(*score, member.as_bytes().to_vec());
                db.zadd(&key, &[zmember]).expect("ZADD failed");
            }
        }

        // Get all in ascending order
        let result = db.zrange(&key, 0, -1, false).expect("ZRANGE failed");

        // Verify ordering by checking scores
        let mut prev_score = f64::NEG_INFINITY;
        for zmember in &result {
            prop_assert!(zmember.score >= prev_score, "Scores should be in ascending order");
            prev_score = zmember.score;
        }
    }

    /// ZREVRANGE returns elements in descending score order
    #[test]
    fn prop_zset_reverse_ordering(
        key in arb_key(),
        items in prop::collection::vec((arb_member(), arb_score()), 2..20)
    ) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        for (member, score) in &items {
            if score.is_finite() {
                let zmember = ZMember::new(*score, member.as_bytes().to_vec());
                db.zadd(&key, &[zmember]).expect("ZADD failed");
            }
        }

        let result = db.zrevrange(&key, 0, -1, false).expect("ZREVRANGE failed");

        let mut prev_score = f64::INFINITY;
        for zmember in &result {
            prop_assert!(zmember.score <= prev_score, "Scores should be in descending order");
            prev_score = zmember.score;
        }
    }

    /// ZSCORE returns the exact score that was set
    #[test]
    fn prop_zset_score_exact(key in arb_key(), member in arb_member(), score in arb_score()) {
        prop_assume!(score.is_finite());

        let db = Db::open_memory().expect("Failed to open in-memory db");

        let zmember = ZMember::new(score, member.as_bytes().to_vec());
        db.zadd(&key, &[zmember]).expect("ZADD failed");
        let retrieved = db.zscore(&key, member.as_bytes()).expect("ZSCORE failed");

        prop_assert_eq!(retrieved, Some(score), "ZSCORE should return exact score");
    }

    /// ZINCRBY adds the exact delta to the score
    #[test]
    fn prop_zset_zincrby(key in arb_key(), member in arb_member(), initial in -100.0f64..100.0, delta in -50.0f64..50.0) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        let zmember = ZMember::new(initial, member.as_bytes().to_vec());
        db.zadd(&key, &[zmember]).expect("ZADD failed");
        let new_score = db.zincrby(&key, delta, member.as_bytes()).expect("ZINCRBY failed");

        let expected = initial + delta;
        prop_assert!((new_score - expected).abs() < 1e-10, "ZINCRBY should add exact delta");
    }
}

// ============================================================================
// Property: Hash Field Roundtrip
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// HSET/HGET roundtrip preserves values
    #[test]
    fn prop_hash_field_roundtrip(
        key in arb_key(),
        field in arb_field(),
        value in arb_value()
    ) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        db.hset(&key, &[(&field, value.as_slice())]).expect("HSET failed");
        let retrieved = db.hget(&key, &field).expect("HGET failed");

        prop_assert_eq!(retrieved, Some(value), "HSET/HGET roundtrip failed");
    }

    /// HSET updates existing field
    #[test]
    fn prop_hash_field_update(
        key in arb_key(),
        field in arb_field(),
        v1 in arb_value(),
        v2 in arb_value()
    ) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        db.hset(&key, &[(&field, v1.as_slice())]).expect("HSET v1 failed");
        db.hset(&key, &[(&field, v2.as_slice())]).expect("HSET v2 failed");

        let retrieved = db.hget(&key, &field).expect("HGET failed");
        prop_assert_eq!(retrieved, Some(v2), "HSET should update field");

        // Hash should have exactly one field
        let len = db.hlen(&key).expect("HLEN failed");
        prop_assert_eq!(len, 1, "Hash should have one field after update");
    }

    /// HDEL removes only the specified field
    #[test]
    fn prop_hash_hdel(
        key in arb_key(),
        fields in prop::collection::vec(arb_field(), 2..5),
        value in arb_value()
    ) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        let unique: Vec<_> = fields.iter().collect::<std::collections::HashSet<_>>().into_iter().collect();
        if unique.len() < 2 {
            return Ok(());
        }

        for field in &unique {
            db.hset(&key, &[(field.as_str(), value.as_slice())]).expect("HSET failed");
        }

        let initial_len = db.hlen(&key).expect("HLEN failed");

        db.hdel(&key, &[unique[0].as_str()]).expect("HDEL failed");

        let final_len = db.hlen(&key).expect("HLEN failed");
        prop_assert_eq!(final_len, initial_len - 1);

        // Deleted field should not exist
        let retrieved = db.hget(&key, unique[0]).expect("HGET failed");
        prop_assert_eq!(retrieved, None);
    }

    /// HGETALL returns all fields
    #[test]
    fn prop_hash_hgetall(
        key in arb_key(),
        fields in prop::collection::vec((arb_field(), arb_value()), 1..10)
    ) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        let mut expected = std::collections::HashMap::new();
        for (field, value) in &fields {
            db.hset(&key, &[(field.as_str(), value.as_slice())]).expect("HSET failed");
            expected.insert(field.clone(), value.clone());
        }

        let all = db.hgetall(&key).expect("HGETALL failed");
        prop_assert_eq!(all.len(), expected.len());

        for (field, value) in &all {
            let expected_val = expected.get(field);
            prop_assert_eq!(Some(value), expected_val.map(|v| v));
        }
    }

    /// HINCRBY adds the exact delta
    #[test]
    fn prop_hash_hincrby(key in arb_key(), field in arb_field(), initial in -1000i64..1000i64, delta in -100i64..100i64) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        db.hset(&key, &[(&field, initial.to_string().as_bytes())]).expect("HSET failed");
        let result = db.hincrby(&key, &field, delta).expect("HINCRBY failed");

        prop_assert_eq!(result, initial + delta);
    }
}

// ============================================================================
// Property: Expiration
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// TTL is set correctly
    #[test]
    fn prop_expire_ttl(key in arb_key(), value in arb_value(), ttl in 10i64..3600) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        db.set(&key, &value, None).expect("SET failed");
        let success = db.expire(&key, ttl).expect("EXPIRE failed");
        prop_assert!(success, "EXPIRE should succeed");

        let retrieved_ttl = db.ttl(&key).expect("TTL failed");
        // TTL should be close to what we set (within a second or two)
        prop_assert!(retrieved_ttl > 0, "TTL should be positive");
        prop_assert!(retrieved_ttl <= ttl, "TTL should not exceed set value");
    }

    /// PERSIST removes expiration
    #[test]
    fn prop_persist_removes_ttl(key in arb_key(), value in arb_value()) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        db.set(&key, &value, None).expect("SET failed");
        db.expire(&key, 3600).expect("EXPIRE failed");

        let success = db.persist(&key).expect("PERSIST failed");
        prop_assert!(success, "PERSIST should succeed");

        let ttl = db.ttl(&key).expect("TTL failed");
        prop_assert_eq!(ttl, -1, "TTL should be -1 (no expiration) after PERSIST");
    }
}

// ============================================================================
// Property: DEL and EXISTS
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// DEL removes key
    #[test]
    fn prop_del_removes_key(key in arb_key(), value in arb_value()) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        db.set(&key, &value, None).expect("SET failed");
        let deleted = db.del(&[&key]).expect("DEL failed");
        prop_assert_eq!(deleted, 1, "DEL should return 1");

        let exists = db.exists(&[&key]).expect("EXISTS failed");
        prop_assert_eq!(exists, 0, "Key should not exist after DEL");
    }

    /// EXISTS returns correct count
    #[test]
    fn prop_exists_count(keys in prop::collection::vec(arb_key(), 1..10), value in arb_value()) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        let unique: Vec<_> = keys.iter().collect::<std::collections::HashSet<_>>().into_iter().cloned().collect();

        // Create half of the keys
        let to_create = unique.len() / 2;
        for key in unique.iter().take(to_create) {
            db.set(key, &value, None).expect("SET failed");
        }

        let key_refs: Vec<&str> = unique.iter().map(|s| s.as_str()).collect();
        let count = db.exists(&key_refs).expect("EXISTS failed");
        prop_assert_eq!(count as usize, to_create);
    }
}

// ============================================================================
// Property: TYPE command
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// TYPE returns correct type for string
    #[test]
    fn prop_type_string(key in arb_key(), value in arb_value()) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        db.set(&key, &value, None).expect("SET failed");
        let key_type = db.key_type(&key).expect("TYPE failed");

        prop_assert_eq!(key_type, Some(redlite::KeyType::String));
    }

    /// TYPE returns correct type for list
    #[test]
    fn prop_type_list(key in arb_key(), value in arb_value()) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        db.rpush(&key, &[value.as_slice()]).expect("RPUSH failed");
        let key_type = db.key_type(&key).expect("TYPE failed");

        prop_assert_eq!(key_type, Some(redlite::KeyType::List));
    }

    /// TYPE returns correct type for set
    #[test]
    fn prop_type_set(key in arb_key(), member in arb_member()) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        db.sadd(&key, &[member.as_bytes()]).expect("SADD failed");
        let key_type = db.key_type(&key).expect("TYPE failed");

        prop_assert_eq!(key_type, Some(redlite::KeyType::Set));
    }

    /// TYPE returns correct type for zset
    #[test]
    fn prop_type_zset(key in arb_key(), member in arb_member(), score in arb_score()) {
        prop_assume!(score.is_finite());

        let db = Db::open_memory().expect("Failed to open in-memory db");

        let zmember = ZMember::new(score, member.as_bytes().to_vec());
        db.zadd(&key, &[zmember]).expect("ZADD failed");
        let key_type = db.key_type(&key).expect("TYPE failed");

        prop_assert_eq!(key_type, Some(redlite::KeyType::ZSet));
    }

    /// TYPE returns correct type for hash
    #[test]
    fn prop_type_hash(key in arb_key(), field in arb_field(), value in arb_value()) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        db.hset(&key, &[(&field, value.as_slice())]).expect("HSET failed");
        let key_type = db.key_type(&key).expect("TYPE failed");

        prop_assert_eq!(key_type, Some(redlite::KeyType::Hash));
    }

    /// TYPE returns None for non-existent key
    #[test]
    fn prop_type_nonexistent(key in arb_key()) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        let key_type = db.key_type(&key).expect("TYPE failed");
        prop_assert_eq!(key_type, None);
    }
}

// ============================================================================
// Property: APPEND
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// APPEND concatenates values
    #[test]
    fn prop_append_concat(key in arb_key(), v1 in arb_value(), v2 in arb_value()) {
        let db = Db::open_memory().expect("Failed to open in-memory db");

        db.set(&key, &v1, None).expect("SET failed");
        let new_len = db.append(&key, &v2).expect("APPEND failed");

        let expected_len = v1.len() + v2.len();
        prop_assert_eq!(new_len as usize, expected_len);

        let retrieved = db.get(&key).expect("GET failed").unwrap();
        let mut expected = v1;
        expected.extend(v2);
        prop_assert_eq!(retrieved, expected);
    }
}
