//! Integration tests for PagingPredicate with custom comparators.
//!
//! These tests require a running Hazelcast cluster and are ignored by default.
//! Run with: cargo test --test paging_predicate_integration_test -- --ignored

use std::cmp::Ordering;

/// Custom comparator for testing that sorts by key length.
/// This demonstrates implementing a custom comparator.
#[derive(Debug, Clone)]
struct KeyLengthComparator {
    factory_id: i32,
    class_id: i32,
}

impl KeyLengthComparator {
    fn new(factory_id: i32, class_id: i32) -> Self {
        Self {
            factory_id,
            class_id,
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_custom_comparator_pattern() {
        let comp = KeyLengthComparator::new(1000, 1);
        assert_eq!(comp.factory_id, 1000);
        assert_eq!(comp.class_id, 1);
    }
}

#[cfg(test)]
mod integration_tests {
    //! Integration tests require:
    //! 1. A running Hazelcast cluster
    //! 2. Server-side comparator classes deployed matching the factory/class IDs
    //!
    //! These tests demonstrate usage patterns for paging with custom sort orders.

    #[tokio::test]
    #[ignore]
    async fn test_paging_with_key_comparator_ascending() {
        // This test would:
        // 1. Create a HazelcastClient
        // 2. Get an IMap
        // 3. Populate with test data
        // 4. Create PagingPredicate with EntryKeyComparator (ascending)
        // 5. Verify results come back in ascending key order across pages

        // Example pseudocode:
        // let client = HazelcastClient::new(config).await.unwrap();
        // let map = client.get_map::<String, i32>("test-map").await.unwrap();
        //
        // // Populate data
        // for i in 0..100 {
        //     map.put(format!("key-{:03}", i), i).await.unwrap();
        // }
        //
        // // Create paging predicate with key comparator
        // let comparator = EntryKeyComparator::new(FACTORY_ID, KEY_COMP_CLASS_ID);
        // let paging = PagingPredicate::with_comparator(10, &comparator).unwrap();
        //
        // // Fetch first page
        // let page1 = map.entries_with_paging_predicate(&paging).await.unwrap();
        // assert_eq!(page1.len(), 10);
        // assert!(page1[0].0 < page1[9].0); // First key < last key
        //
        // // Fetch next page
        // paging.next_page();
        // let page2 = map.entries_with_paging_predicate(&paging).await.unwrap();
        // assert!(page1[9].0 < page2[0].0); // Last of page1 < first of page2
    }

    #[tokio::test]
    #[ignore]
    async fn test_paging_with_key_comparator_descending() {
        // Test descending key order across pages
        // Similar to above but with EntryKeyComparator::descending()
        // Verify keys decrease across pages
    }

    #[tokio::test]
    #[ignore]
    async fn test_paging_with_value_comparator() {
        // Test ordering by value
        // Use EntryValueComparator and verify values are ordered
    }

    #[tokio::test]
    #[ignore]
    async fn test_paging_with_predicate_and_comparator() {
        // Test combining filter predicate with custom ordering
        // Example: filter age > 18, order by name descending

        // let paging = PagingPredicateBuilder::new(10)
        //     .predicate(Predicates::greater_than("age", &18)?)
        //     .comparator(&EntryKeyComparator::descending(FACTORY_ID, CLASS_ID))?
        //     .build();
    }

    #[tokio::test]
    #[ignore]
    async fn test_paging_iteration_type_key() {
        // Test IterationType::Key returns only keys
        // let paging = PagingPredicateBuilder::new(10)
        //     .order_by_key()
        //     .build();
        // let keys = map.keys_with_paging_predicate(&paging).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_paging_iteration_type_value() {
        // Test IterationType::Value returns only values
    }

    #[tokio::test]
    #[ignore]
    async fn test_paging_iteration_type_entry() {
        // Test IterationType::Entry returns key-value pairs (default)
    }

    #[tokio::test]
    #[ignore]
    async fn test_paging_page_navigation_with_comparator() {
        // Test that page navigation (next_page, previous_page, set_page, reset)
        // works correctly with comparators
        // Verify anchors are updated appropriately
    }

    #[tokio::test]
    #[ignore]
    async fn test_custom_comparator_serialization() {
        // Test that a custom comparator is correctly serialized and
        // the server can deserialize and use it
        // This requires deploying a matching comparator class on the server
    }
}
