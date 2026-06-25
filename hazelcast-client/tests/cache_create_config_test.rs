//! Live verification of server-side ICache creation via CacheCreateConfig (cbdc).
//!
//! Before this codec, ICache ops failed with CacheNotExistsException because the
//! client never created the cache server-side. create_config() sends a full
//! CacheConfigHolder (CacheCreateConfig, 0x130600). This proves a fresh cache can
//! be created and then read/written — the cache is functional.

mod common;

use hazelcast_client::HazelcastClient;

#[tokio::test]
#[ignore = "requires the dev cluster"]
async fn test_cache_create_config_and_ops() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect");

    let cache = client.get_cache::<String, String>(&common::unique_name("ct-rust"));

    // Server-side cache creation via the CacheConfigHolder / CacheCreateConfig
    // codec. Without it the put/get below fail with CacheNotExistsException.
    cache
        .create_config()
        .await
        .expect("create cache config (CacheCreateConfig)");

    cache
        .put("acct-1".to_string(), "bal-100".to_string())
        .await
        .expect("put after create");
    assert_eq!(
        cache.get(&"acct-1".to_string()).await.expect("get"),
        Some("bal-100".to_string()),
        "cache get must return the put value"
    );
    assert_eq!(
        cache.get(&"nope".to_string()).await.expect("get absent"),
        None,
        "absent key must return None"
    );
    cache
        .create_config()
        .await
        .expect("re-create is idempotent");

    client.shutdown().await.ok();
}
