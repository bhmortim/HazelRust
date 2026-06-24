//! Authentication-failure surfacing tests (CBDC item A-3 / R8).
//!
//! Before the fix the client did not inspect the authentication response status
//! byte: a rejected auth was only logged and the half-authenticated connection
//! was still registered, so `HazelcastClient::new` could return `Ok` against a
//! server that rejected the client. These tests assert a rejected auth surfaces
//! as an `Err` and that no usable client is produced.
//!
//! Requires a running Hazelcast cluster; ignored by default. The negative case
//! forces rejection with a wrong cluster name (the member answers with a
//! non-AUTHENTICATED status, or closes the connection — either way the client
//! must fail, never silently succeed).
//!
//! Run with: `CLUSTER_ADDRESS=127.0.0.1:5701 cargo test --test auth_failure_test -- --ignored`

use std::env;
use std::time::Duration;

use hazelcast_client::{ClientConfig, ClientConfigBuilder, HazelcastClient};

fn address() -> String {
    env::var("CLUSTER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:5701".to_string())
}

fn config(cluster_name: &str) -> ClientConfig {
    ClientConfigBuilder::new()
        .cluster_name(cluster_name)
        .add_address(address().parse().expect("valid CLUSTER_ADDRESS"))
        .connection_timeout(Duration::from_secs(8))
        .build()
        .expect("config")
}

/// Negative: connecting with a cluster name the member does not serve must FAIL,
/// never return an `Ok` client with a half-authenticated connection.
#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_auth_wrong_cluster_name_is_rejected() {
    let result = HazelcastClient::new(config("WRONG-CLUSTER-NAME-cbdc-a3")).await;
    assert!(
        result.is_err(),
        "authentication against a wrong cluster name must return Err, got Ok (half-authenticated client)"
    );
}

/// Positive control: the correct cluster name still authenticates and the client
/// is usable (proves the status-byte check reads the right offset and does not
/// reject valid auth).
#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_auth_correct_cluster_name_succeeds() {
    let cluster = env::var("CLUSTER_NAME").unwrap_or_else(|_| "dev".to_string());
    let client = HazelcastClient::new(config(&cluster))
        .await
        .expect("authentication with the correct cluster name must succeed");

    let map = client.get_map::<String, String>("auth-failure-test-smoke");
    map.put("k".to_string(), "v".to_string())
        .await
        .expect("data op after successful auth must work");
    assert_eq!(
        map.get(&"k".to_string()).await.expect("get"),
        Some("v".to_string())
    );

    client.shutdown().await.expect("shutdown");
}
