//! Live end-to-end verification of client credential authentication (cbdc).
//!
//! Requires a security-enabled EE member: cluster "sec" on 127.0.0.1:5720 with a
//! simple password realm (user `ledger-admin` / `s3cret`, client-permissions all).
//! See the pass-10 setup (`~/hz/hz-sec.yaml` on the instance). These tests prove
//! the credential fix: configured username/password are actually sent and honored
//! (correct creds authenticate; wrong/absent creds are rejected).

mod common;

use std::time::Duration;

use hazelcast_client::{ClientConfigBuilder, HazelcastClient};

const SEC_ADDRESS: &str = "127.0.0.1:5720";

#[tokio::test]
#[ignore = "requires the security-enabled EE member (cluster 'sec' :5720)"]
async fn test_auth_with_correct_credentials_succeeds() {
    let config = ClientConfigBuilder::new()
        .cluster_name("sec")
        .add_address(SEC_ADDRESS.parse().unwrap())
        .credentials("ledger-admin", "s3cret")
        .build()
        .expect("config");

    let client = HazelcastClient::new(config)
        .await
        .expect("correct credentials must authenticate against the secured cluster");

    // Prove the authenticated session can actually operate (auth + authz).
    let map = client.get_map::<String, String>(&common::unique_name("sec-map"));
    map.put("acct-1".to_string(), "bal-100".to_string())
        .await
        .expect("put on secured cluster");
    assert_eq!(
        map.get(&"acct-1".to_string()).await.expect("get"),
        Some("bal-100".to_string())
    );
    client.shutdown().await.ok();
}

#[tokio::test]
#[ignore = "requires the security-enabled EE member (cluster 'sec' :5720)"]
async fn test_auth_with_wrong_password_rejected() {
    let config = ClientConfigBuilder::new()
        .cluster_name("sec")
        .add_address(SEC_ADDRESS.parse().unwrap())
        .credentials("ledger-admin", "WRONG-PASSWORD")
        .build()
        .expect("config");

    // The server replies with auth status 1 (credentials failed); the client must
    // surface that as an error, not connect anonymously.
    let result = tokio::time::timeout(Duration::from_secs(15), HazelcastClient::new(config)).await;
    match result {
        Ok(Ok(client)) => {
            client.shutdown().await.ok();
            panic!("wrong credentials must be REJECTED, but the client connected");
        }
        Ok(Err(_)) => { /* expected: authentication error */ }
        Err(_) => panic!("connect with wrong credentials hung instead of being rejected"),
    }
}

#[tokio::test]
#[ignore = "requires the security-enabled EE member (cluster 'sec' :5720)"]
async fn test_auth_with_no_credentials_rejected() {
    let config = ClientConfigBuilder::new()
        .cluster_name("sec")
        .add_address(SEC_ADDRESS.parse().unwrap())
        .build()
        .expect("config");

    let result = tokio::time::timeout(Duration::from_secs(15), HazelcastClient::new(config)).await;
    match result {
        Ok(Ok(client)) => {
            client.shutdown().await.ok();
            panic!("anonymous auth must be REJECTED on a secured cluster, but it connected");
        }
        Ok(Err(_)) => { /* expected */ }
        Err(_) => panic!("anonymous connect hung instead of being rejected"),
    }
}
