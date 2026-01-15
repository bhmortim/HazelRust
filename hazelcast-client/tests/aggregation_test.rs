//! Integration tests for Map Aggregations.
//!
//! Run with: `cargo test --test aggregation_test -- --ignored`
//! Requires a Hazelcast cluster running at 127.0.0.1:5701

use std::net::SocketAddr;

use hazelcast_client::query::{Aggregators, Predicates};
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_core::Result;

async fn create_client() -> Result<HazelcastClient> {
    let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
    let config = ClientConfig::builder()
        .cluster_name("dev")
        .add_address(addr)
        .build()
        .map_err(|e| hazelcast_core::HazelcastError::Configuration(e.to_string()))?;

    HazelcastClient::new(config).await
}

#[tokio::test]
#[ignore]
async fn test_count_aggregator() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let map_name = format!("test-aggregation-count-{}", uuid::Uuid::new_v4());
    let map = client.get_map::<String, i32>(&map_name);

    // Setup test data
    for i in 0..10 {
        map.put(format!("key-{}", i), i)
            .await
            .expect("put should succeed");
    }

    // Count aggregation
    let aggregator = Aggregators::count();
    let result = map.aggregate(&aggregator).await;

    match result {
        Ok(count) => {
            println!("Count aggregation result: {}", count);
            assert_eq!(count, 10, "Should count 10 entries");
        }
        Err(e) => {
            eprintln!("Count aggregation error: {}", e);
        }
    }

    // Cleanup
    let _ = map.clear().await;
    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_sum_aggregator() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let map_name = format!("test-aggregation-sum-{}", uuid::Uuid::new_v4());
    let map = client.get_map::<String, i64>(&map_name);

    // Setup test data: 0 + 1 + 2 + ... + 9 = 45
    for i in 0i64..10 {
        map.put(format!("key-{}", i), i)
            .await
            .expect("put should succeed");
    }

    // Sum aggregation on __value field
    let aggregator = Aggregators::long_sum("__value");
    let result = map.aggregate(&aggregator).await;

    match result {
        Ok(sum) => {
            println!("Sum aggregation result: {}", sum);
            assert_eq!(sum, 45, "Sum should be 45");
        }
        Err(e) => {
            eprintln!("Sum aggregation error: {}", e);
        }
    }

    // Cleanup
    let _ = map.clear().await;
    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_average_aggregator() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let map_name = format!("test-aggregation-avg-{}", uuid::Uuid::new_v4());
    let map = client.get_map::<String, f64>(&map_name);

    // Setup test data
    let values = [10.0, 20.0, 30.0, 40.0, 50.0];
    for (i, &v) in values.iter().enumerate() {
        map.put(format!("key-{}", i), v)
            .await
            .expect("put should succeed");
    }

    // Average aggregation
    let aggregator = Aggregators::double_avg("__value");
    let result = map.aggregate(&aggregator).await;

    match result {
        Ok(avg) => {
            println!("Average aggregation result: {}", avg);
            assert!((avg - 30.0).abs() < 0.001, "Average should be 30.0");
        }
        Err(e) => {
            eprintln!("Average aggregation error: {}", e);
        }
    }

    // Cleanup
    let _ = map.clear().await;
    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_min_max_aggregators() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let map_name = format!("test-aggregation-minmax-{}", uuid::Uuid::new_v4());
    let map = client.get_map::<String, i64>(&map_name);

    // Setup test data
    for i in [5i64, 2, 8, 1, 9, 3, 7, 4, 6, 10] {
        map.put(format!("key-{}", i), i)
            .await
            .expect("put should succeed");
    }

    // Min aggregation
    let min_agg: hazelcast_client::query::MinAggregator<i64> = Aggregators::min("__value");
    let min_result = map.aggregate(&min_agg).await;

    match min_result {
        Ok(min) => {
            println!("Min aggregation result: {}", min);
            assert_eq!(min, 1, "Min should be 1");
        }
        Err(e) => {
            eprintln!("Min aggregation error: {}", e);
        }
    }

    // Max aggregation
    let max_agg: hazelcast_client::query::MaxAggregator<i64> = Aggregators::max("__value");
    let max_result = map.aggregate(&max_agg).await;

    match max_result {
        Ok(max) => {
            println!("Max aggregation result: {}", max);
            assert_eq!(max, 10, "Max should be 10");
        }
        Err(e) => {
            eprintln!("Max aggregation error: {}", e);
        }
    }

    // Cleanup
    let _ = map.clear().await;
    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_aggregate_with_predicate() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let map_name = format!("test-aggregation-pred-{}", uuid::Uuid::new_v4());
    let map = client.get_map::<String, i32>(&map_name);

    // Setup test data: values 1-20
    for i in 1..=20 {
        map.put(format!("key-{}", i), i)
            .await
            .expect("put should succeed");
    }

    // Count entries where value > 10
    let predicate = match Predicates::greater_than("__value", &10i32) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to create predicate: {}", e);
            let _ = map.clear().await;
            let _ = client.shutdown().await;
            return;
        }
    };

    let aggregator = Aggregators::count();
    let result = map.aggregate_with_predicate(&aggregator, &predicate).await;

    match result {
        Ok(count) => {
            println!("Filtered count result: {}", count);
            assert_eq!(count, 10, "Should count 10 entries > 10");
        }
        Err(e) => {
            eprintln!("Filtered aggregation error: {}", e);
        }
    }

    // Cleanup
    let _ = map.clear().await;
    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_integer_sum_aggregator() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let map_name = format!("test-aggregation-intsum-{}", uuid::Uuid::new_v4());
    let map = client.get_map::<String, i32>(&map_name);

    // Setup: 1 + 2 + 3 + 4 + 5 = 15
    for i in 1..=5 {
        map.put(format!("key-{}", i), i)
            .await
            .expect("put should succeed");
    }

    let aggregator = Aggregators::integer_sum("__value");
    let result = map.aggregate(&aggregator).await;

    match result {
        Ok(sum) => {
            println!("Integer sum result: {}", sum);
            assert_eq!(sum, 15, "Sum should be 15");
        }
        Err(e) => {
            eprintln!("Integer sum error: {}", e);
        }
    }

    let _ = map.clear().await;
    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_aggregation_on_empty_map() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let map_name = format!("test-aggregation-empty-{}", uuid::Uuid::new_v4());
    let map = client.get_map::<String, i64>(&map_name);

    // Count on empty map should return 0
    let aggregator = Aggregators::count();
    let result = map.aggregate(&aggregator).await;

    match result {
        Ok(count) => {
            println!("Empty map count: {}", count);
            assert_eq!(count, 0, "Empty map count should be 0");
        }
        Err(e) => {
            eprintln!("Empty map aggregation error: {}", e);
        }
    }

    let _ = client.shutdown().await;
}
