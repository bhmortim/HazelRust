//! Live fault-injection regression for cbdc lead #3 — after a heartbeat-driven
//! connection drop + reconnect, data operations must resume (the
//! `InvocationService` pool must be rebuilt with a freshly *authenticated*
//! connection, and the dead write-halves evicted).
//!
//! Mechanism: connect to a dedicated single-member EE cluster ("solo", :5710) so
//! no other member can mask the failure via dumb-routing fallback, then restart
//! that member (`docker restart hzsolo`) to force the client's heartbeat to
//! detect the drop and reconnect. Before the fix, the heartbeat-driven
//! `attempt_reconnect` opened a raw *unauthenticated* socket and never
//! repopulated the invocation pool, so the pool kept only dead write-halves and
//! every post-reconnect op failed/timed-out — this test caught that
//! (`resumed == false`).
//!
//! Gated on HZ_FAULT_INJECTION=1 because it restarts a cluster member. Runs on
//! the AWS instance where `sudo docker` is available passwordless.

mod common;

use std::time::Duration;

use hazelcast_client::{ClientConfigBuilder, HazelcastClient};

#[tokio::test]
#[ignore = "fault injection: restarts the solo EE member; set HZ_FAULT_INJECTION=1"]
async fn test_data_ops_resume_after_member_reconnect() {
    if std::env::var("HZ_FAULT_INJECTION").ok().as_deref() != Some("1") {
        eprintln!("skipping: set HZ_FAULT_INJECTION=1 to run (restarts hzsolo)");
        return;
    }

    let config = ClientConfigBuilder::new()
        .cluster_name("solo")
        .add_address("127.0.0.1:5710".parse().unwrap())
        .network(|n| n.heartbeat_interval(Duration::from_secs(1)))
        .retry(|r| {
            r.max_retries(120)
                .initial_backoff(Duration::from_millis(200))
                .max_backoff(Duration::from_secs(2))
        })
        .build()
        .expect("build config");

    let client = HazelcastClient::new(config)
        .await
        .expect("connect to solo member");
    let map = client.get_map::<String, String>("fault-reconnect-test");

    // Sanity: data ops work before the fault.
    map.put("k0".to_string(), "v0".to_string())
        .await
        .expect("initial put");
    assert_eq!(
        map.get(&"k0".to_string()).await.unwrap(),
        Some("v0".to_string())
    );

    // Induce the fault: forcibly kill the client's TCP connection(s) to the
    // member with `ss -K` (the member itself stays up), so the heartbeat detects
    // a dead socket and triggers a reconnect. Killing the socket rather than
    // rebooting the member isolates the pool-rebuild behaviour from member boot
    // time and keeps the test well under nextest's 40s slow-timeout.
    eprintln!("killing client->member sockets to force a heartbeat-driven reconnect...");
    let status = std::process::Command::new("sudo")
        .args(["ss", "-K", "dst", "127.0.0.1:5710"])
        .status()
        .expect("run ss -K");
    assert!(status.success(), "ss -K failed");

    // Poll until data ops resume (heartbeat detected the drop + reconnect rebuilt
    // the authenticated invocation pool, evicting the dead write halves). A fixed
    // client resumes within a few seconds; before the fix the pool kept only dead
    // write halves and this loop never succeeds.
    let mut resumed = false;
    for attempt in 0..25 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let k = format!("k{attempt}");
        if map.put(k.clone(), "v".to_string()).await.is_ok()
            && map.get(&k).await.ok().flatten() == Some("v".to_string())
        {
            resumed = true;
            eprintln!("data ops resumed at attempt {attempt}");
            break;
        }
    }

    assert!(
        resumed,
        "data ops did NOT resume after member restart — lead #3: the invocation \
         pool was not repopulated with an authenticated connection on the \
         heartbeat-driven reconnect"
    );

    client.shutdown().await.ok();
}
