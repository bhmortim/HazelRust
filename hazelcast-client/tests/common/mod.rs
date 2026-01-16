//! Common test utilities for integration tests.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hazelcast_client::{
    ClientConfig, ClientConfigBuilder, HazelcastClient, NearCacheConfig,
};

static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);
static CLUSTER_STARTED: AtomicBool = AtomicBool::new(false);

pub const DEFAULT_CLUSTER_ADDRESS: &str = "127.0.0.1:5701";
pub const SECONDARY_CLUSTER_ADDRESS: &str = "127.0.0.1:5702";
pub const TERTIARY_CLUSTER_ADDRESS: &str = "127.0.0.1:5703";

pub fn unique_name(prefix: &str) -> String {
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}-{}-{}", prefix, std::process::id(), id)
}

pub fn default_config() -> ClientConfig {
    ClientConfigBuilder::new()
        .cluster_name("dev")
        .address(DEFAULT_CLUSTER_ADDRESS)
        .build()
        .expect("failed to build config")
}

pub fn config_with_retry() -> ClientConfig {
    ClientConfigBuilder::new()
        .cluster_name("dev")
        .address(DEFAULT_CLUSTER_ADDRESS)
        .retry(|r| {
            r.max_attempts(5)
                .initial_backoff(Duration::from_millis(100))
                .max_backoff(Duration::from_secs(2))
        })
        .build()
        .expect("failed to build config")
}

pub fn config_with_near_cache(map_name: &str) -> ClientConfig {
    let near_cache = NearCacheConfig::builder(map_name)
        .max_size(1000)
        .time_to_live(Duration::from_secs(300))
        .build()
        .expect("failed to build near cache config");

    ClientConfigBuilder::new()
        .cluster_name("dev")
        .address(DEFAULT_CLUSTER_ADDRESS)
        .add_near_cache_config(near_cache)
        .build()
        .expect("failed to build config")
}

pub fn multi_member_config() -> ClientConfig {
    ClientConfigBuilder::new()
        .cluster_name("dev")
        .address(DEFAULT_CLUSTER_ADDRESS)
        .address(SECONDARY_CLUSTER_ADDRESS)
        .address(TERTIARY_CLUSTER_ADDRESS)
        .build()
        .expect("failed to build config")
}

pub async fn create_client() -> HazelcastClient {
    create_client_with_config(default_config()).await
}

pub async fn create_client_with_config(config: ClientConfig) -> HazelcastClient {
    HazelcastClient::new(config)
        .await
        .expect("failed to create client")
}

pub async fn wait_for_cluster_ready() {
    if CLUSTER_STARTED.load(Ordering::SeqCst) {
        return;
    }

    let addr: SocketAddr = DEFAULT_CLUSTER_ADDRESS.parse().unwrap();
    let mut attempts = 0;
    let max_attempts = 30;

    while attempts < max_attempts {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(_) => {
                CLUSTER_STARTED.store(true, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(500)).await;
                return;
            }
            Err(_) => {
                attempts += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    panic!(
        "Hazelcast cluster not available at {} after {} attempts. \
         Please start a Hazelcast cluster using: \
         docker run -d --name hazelcast -p 5701:5701 hazelcast/hazelcast:5.3",
        DEFAULT_CLUSTER_ADDRESS, max_attempts
    );
}

pub fn skip_if_no_cluster() -> bool {
    let addr: SocketAddr = DEFAULT_CLUSTER_ADDRESS.parse().unwrap();
    std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_err()
}

#[macro_export]
macro_rules! require_cluster {
    () => {
        if $crate::common::skip_if_no_cluster() {
            eprintln!(
                "Skipping test: Hazelcast cluster not available at {}",
                $crate::common::DEFAULT_CLUSTER_ADDRESS
            );
            return;
        }
        $crate::common::wait_for_cluster_ready().await;
    };
}

pub struct TestCleanup {
    client: Arc<HazelcastClient>,
    map_names: Vec<String>,
    queue_names: Vec<String>,
    set_names: Vec<String>,
    list_names: Vec<String>,
}

impl TestCleanup {
    pub fn new(client: Arc<HazelcastClient>) -> Self {
        Self {
            client,
            map_names: Vec::new(),
            queue_names: Vec::new(),
            set_names: Vec::new(),
            list_names: Vec::new(),
        }
    }

    pub fn track_map(&mut self, name: String) {
        self.map_names.push(name);
    }

    pub fn track_queue(&mut self, name: String) {
        self.queue_names.push(name);
    }

    pub fn track_set(&mut self, name: String) {
        self.set_names.push(name);
    }

    pub fn track_list(&mut self, name: String) {
        self.list_names.push(name);
    }

    pub async fn cleanup(&self) {
        for name in &self.map_names {
            if let Ok(map) = self.client.get_map::<String, String>(name).await {
                let _ = map.clear().await;
            }
        }
        for name in &self.queue_names {
            if let Ok(queue) = self.client.get_queue::<String>(name).await {
                while queue.poll().await.ok().flatten().is_some() {}
            }
        }
        for name in &self.set_names {
            if let Ok(set) = self.client.get_set::<String>(name).await {
                let _ = set.clear().await;
            }
        }
        for name in &self.list_names {
            if let Ok(list) = self.client.get_list::<String>(name).await {
                let _ = list.clear().await;
            }
        }
    }
}
