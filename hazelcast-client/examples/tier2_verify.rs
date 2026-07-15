//! Live-cluster verification for the Tier-2 performance changes:
//! MAP_SET / MAP_SET_WITH_MAX_IDLE opcodes (void responses, maxIdle actually
//! encoded), sharded near-cache correctness (wire-bytes on miss, invalidation
//! on write), `any_address` round-robin, and the PreparedMessage retry path
//! (exercised implicitly — idempotent reads default to 3 retries).
//!
//! Run against a real member: `cargo run --example tier2_verify -- 192.168.0.250:5701 dev`

use hazelcast_client::{ClientConfig, EvictionPolicy, HazelcastClient, NearCacheConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:5701".to_string());
    let cluster = args.next().unwrap_or_else(|| "dev".to_string());
    let map_name = "tier2-verify";

    let near_cache_config = NearCacheConfig::builder(map_name)
        .max_size(1024)
        .eviction_policy(EvictionPolicy::Lru)
        .build()?;
    let config = ClientConfig::builder()
        .cluster_name(&cluster)
        .add_address(addr.parse()?)
        .add_near_cache_config(near_cache_config)
        .build()?;
    let client = HazelcastClient::new(config).await?;
    let map = client.get_map::<String, String>(map_name);
    map.clear().await?;
    let mut failures = 0u32;
    let mut check = |name: &str, ok: bool| {
        println!("{} {}", if ok { "PASS" } else { "FAIL" }, name);
        if !ok {
            failures += 1;
        }
    };

    // --- 1. MAP_SET: fresh key, overwrite of existing key, cross-partition ---
    map.set("k1".into(), "v1".into()).await?;
    check(
        "set(fresh key) + get",
        map.get(&"k1".into()).await? == Some("v1".into()),
    );
    map.set("k1".into(), "v2".into()).await?;
    check(
        "set(overwrite, old value exists server-side)",
        map.get(&"k1".into()).await? == Some("v2".into()),
    );
    let mut all_ok = true;
    for i in 0..50 {
        let (k, v) = (format!("spread-{i}"), format!("val-{i}"));
        map.set(k.clone(), v.clone()).await?;
        all_ok &= map.get(&k).await? == Some(v);
    }
    check("set x50 across partitions (all 3 members)", all_ok);
    check("size after writes", map.size().await? as usize == 51);

    // --- 2. MAP_SET_WITH_MAX_IDLE: ttl + maxIdle actually applied ---
    map.set_with_ttl_and_max_idle(
        "idle-key".into(),
        "idle-val".into(),
        Duration::from_secs(60),
        Duration::from_secs(2),
    )
    .await?;
    map.clear_near_cache(); // force the reads below onto the member
    check(
        "set_with_ttl_and_max_idle readable immediately",
        map.get(&"idle-key".into()).await? == Some("idle-val".into()),
    );
    tokio::time::sleep(Duration::from_millis(4500)).await;
    map.clear_near_cache();
    check(
        "maxIdle=2s expires an idle entry (was silently dropped before)",
        map.get(&"idle-key".into()).await?.is_none(),
    );

    map.set_with_ttl_and_max_idle(
        "ttl-key".into(),
        "ttl-val".into(),
        Duration::from_secs(2),
        Duration::ZERO,
    )
    .await?;
    map.clear_near_cache();
    check(
        "ttl entry readable immediately",
        map.get(&"ttl-key".into()).await? == Some("ttl-val".into()),
    );
    tokio::time::sleep(Duration::from_millis(3500)).await;
    map.clear_near_cache();
    check(
        "ttl=2s expires",
        map.get(&"ttl-key".into()).await?.is_none(),
    );

    map.set_with_ttl_and_max_idle(
        "forever".into(),
        "fv".into(),
        Duration::ZERO,
        Duration::ZERO,
    )
    .await?;
    map.clear_near_cache();
    check(
        "ttl=0/maxIdle=0 persists",
        map.get(&"forever".into()).await? == Some("fv".into()),
    );

    // --- 3. Near-cache: wire-bytes populate + hit + write invalidation ---
    map.set("nc".into(), "first".into()).await?;
    let miss = map.get(&"nc".into()).await?; // miss -> populate from wire bytes
    let hit = map.get(&"nc".into()).await?; // must be served locally
    check(
        "near-cache miss->hit returns identical value",
        miss == Some("first".into()) && hit == Some("first".into()),
    );
    let stats = map.near_cache_stats().expect("near cache configured");
    check("near-cache recorded a hit", stats.hits() >= 1);
    map.set("nc".into(), "second".into()).await?; // MAP_SET invalidates
    check(
        "set() invalidates near-cache (no stale read)",
        map.get(&"nc".into()).await? == Some("second".into()),
    );

    // --- 4. any_address round-robin: non-partition ops keep working ---
    let mut sizes_ok = true;
    for _ in 0..9 {
        sizes_ok &= map.size().await? > 0; // rotates across the 3 members
    }
    check("size() x9 while rotating members", sizes_ok);
    check("is_empty() over rotated member", !map.is_empty().await?);

    // --- 5. put still returns the old value (MAP_PUT untouched) ---
    let old = map.put("k1".into(), "v3".into()).await?;
    check("put() still returns old value", old == Some("v2".into()));

    map.clear().await?;
    println!(
        "\n{}",
        if failures == 0 {
            "ALL CHECKS PASSED".to_string()
        } else {
            format!("{failures} CHECK(S) FAILED")
        }
    );
    std::process::exit(if failures == 0 { 0 } else { 1 });
}
