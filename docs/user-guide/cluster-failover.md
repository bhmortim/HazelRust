# Cluster Failover and Resilience

This guide covers configuring the Hazelcast client for high availability, including connection resilience, failover strategies, and split-brain protection.

## Connection Retry Configuration

### Basic Retry Settings

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::config::ConnectionRetryConfig;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let retry_config = ConnectionRetryConfig::new()
        // Initial delay before first retry
        .initial_backoff(Duration::from_millis(100))
        // Maximum delay between retries
        .max_backoff(Duration::from_secs(30))
        // Backoff multiplier (exponential backoff)
        .multiplier(2.0)
        // Add randomness to prevent thundering herd
        .jitter(0.2)
        // Total time to keep retrying (0 = infinite)
        .cluster_connect_timeout(Duration::from_secs(300));

    let config = ClientConfig::new()
        .addresses(vec!["127.0.0.1:5701"])
        .connection_retry(retry_config);

    let client = HazelcastClient::new(config).await?;
    client.shutdown().await?;
    Ok(())
}
```

### Aggressive Reconnection

For latency-sensitive applications:

```rust
use hazelcast_client::config::ConnectionRetryConfig;
use std::time::Duration;

let aggressive_retry = ConnectionRetryConfig::new()
    .initial_backoff(Duration::from_millis(50))
    .max_backoff(Duration::from_secs(2))
    .multiplier(1.5)
    .jitter(0.1);
```

### Conservative Reconnection

For reducing load during extended outages:

```rust
use hazelcast_client::config::ConnectionRetryConfig;
use std::time::Duration;

let conservative_retry = ConnectionRetryConfig::new()
    .initial_backoff(Duration::from_secs(1))
    .max_backoff(Duration::from_secs(120))
    .multiplier(2.0)
    .jitter(0.3);
```

## Multi-Cluster Failover

Configure failover to alternate clusters:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::config::ClientFailoverConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Primary cluster configuration
    let primary = ClientConfig::new()
        .cluster_name("primary")
        .addresses(vec![
            "10.0.1.10:5701",
            "10.0.1.11:5701",
            "10.0.1.12:5701",
        ]);

    // Secondary (DR) cluster configuration
    let secondary = ClientConfig::new()
        .cluster_name("secondary")
        .addresses(vec![
            "10.0.2.10:5701",
            "10.0.2.11:5701",
            "10.0.2.12:5701",
        ]);

    let failover_config = ClientFailoverConfig::new()
        .add_client_config(primary)
        .add_client_config(secondary)
        // Number of times to try each cluster before moving to next
        .try_count(3);

    let client = HazelcastClient::new_with_failover(failover_config).await?;

    println!("Connected to cluster: {}", client.cluster_name());

    client.shutdown().await?;
    Ok(())
}
```

## Heartbeat Configuration

Fine-tune connection health monitoring:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new()
        .addresses(vec!["127.0.0.1:5701"])
        // How often to send heartbeat pings
        .heartbeat_interval(Duration::from_secs(5))
        // Time to wait for heartbeat response before declaring connection dead
        .heartbeat_timeout(Duration::from_secs(60));

    let client = HazelcastClient::new(config).await?;
    client.shutdown().await?;
    Ok(())
}
```

## Split-Brain Protection (Quorum)

Prevent operations during network partitions:

### Configuring Quorum

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::config::{QuorumConfig, QuorumType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Quorum requires minimum cluster size for operations
    let quorum = QuorumConfig::new("write-quorum")
        // Minimum members required
        .size(3)
        // Apply to write operations only
        .quorum_type(QuorumType::Write);

    let read_quorum = QuorumConfig::new("read-quorum")
        .size(2)
        .quorum_type(QuorumType::Read);

    let config = ClientConfig::new()
        .addresses(vec!["127.0.0.1:5701"])
        .add_quorum_config(quorum)
        .add_quorum_config(read_quorum);

    let client = HazelcastClient::new(config).await?;
    client.shutdown().await?;
    Ok(())
}
```

### Quorum Types

| Type | Description | Protected Operations |
|------|-------------|---------------------|
| `Read` | Requires quorum for reads | `get`, `contains_key`, `size` |
| `Write` | Requires quorum for writes | `put`, `remove`, `clear` |
| `ReadWrite` | Requires quorum for all operations | All map operations |

### Handling Quorum Exceptions

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::error::HazelcastError;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HazelcastClient::new(
        ClientConfig::new().addresses(vec!["127.0.0.1:5701"])
    ).await?;

    let map = client.get_map::<String, String>("protected-map").await?;

    match map.put("key".to_string(), "value".to_string()).await {
        Ok(_) => println!("Write successful"),
        Err(HazelcastError::QuorumNotSatisfied { quorum_name, required, actual }) => {
            eprintln!(
                "Quorum '{}' not satisfied: need {} members, have {}",
                quorum_name, required, actual
            );
            // Implement fallback logic
        }
        Err(e) => return Err(e.into()),
    }

    client.shutdown().await?;
    Ok(())
}
```

## Cluster Listeners

Monitor cluster state changes:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::cluster::{MembershipListener, MembershipEvent, InitialMembershipEvent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new()
        .addresses(vec!["127.0.0.1:5701"]);

    let client = HazelcastClient::new(config).await?;

    let listener = MembershipListener::new()
        .on_init(|event: InitialMembershipEvent| {
            println!("Initial cluster members:");
            for member in event.members() {
                println!("  - {}", member.address());
            }
        })
        .on_member_added(|event: MembershipEvent| {
            println!("Member joined: {}", event.member().address());
        })
        .on_member_removed(|event: MembershipEvent| {
            println!("Member left: {}", event.member().address());
        });

    let registration_id = client.cluster().add_membership_listener(listener).await?;
    println!("Listener registered: {}", registration_id);

    // Keep running to observe cluster changes
    tokio::time::sleep(std::time::Duration::from_secs(300)).await;

    client.shutdown().await?;
    Ok(())
}
```

## Lifecycle Listeners

React to client connection state changes:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::lifecycle::{LifecycleListener, LifecycleEvent, LifecycleState};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let lifecycle_listener = LifecycleListener::new(|event: LifecycleEvent| {
        match event.state() {
            LifecycleState::Starting => println!("Client starting..."),
            LifecycleState::Started => println!("Client connected!"),
            LifecycleState::ShuttingDown => println!("Client shutting down..."),
            LifecycleState::Shutdown => println!("Client shutdown complete"),
            LifecycleState::ClientConnected => println!("Reconnected to cluster"),
            LifecycleState::ClientDisconnected => {
                println!("Disconnected from cluster, attempting reconnect...");
            }
        }
    });

    let config = ClientConfig::new()
        .addresses(vec!["127.0.0.1:5701"])
        .add_lifecycle_listener(lifecycle_listener);

    let client = HazelcastClient::new(config).await?;

    // Simulate work
    tokio::time::sleep(std::time::Duration::from_secs(60)).await;

    client.shutdown().await?;
    Ok(())
}
```

## Smart Routing

Optimize request routing for partition-aware operations:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new()
        .addresses(vec!["127.0.0.1:5701"])
        // Enable smart routing (default: true)
        // Routes requests directly to partition owner
        .smart_routing(true);

    let client = HazelcastClient::new(config).await?;

    // With smart routing, get/put operations go directly
    // to the member owning the key's partition
    let map = client.get_map::<String, String>("smart-map").await?;
    map.put("key".to_string(), "value".to_string()).await?;

    client.shutdown().await?;
    Ok(())
}
```

## Load Balancing

Configure how operations are distributed across cluster members:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::config::LoadBalancer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Round-robin across all members
    let round_robin = ClientConfig::new()
        .addresses(vec!["127.0.0.1:5701"])
        .load_balancer(LoadBalancer::RoundRobin);

    // Random member selection
    let random = ClientConfig::new()
        .addresses(vec!["127.0.0.1:5701"])
        .load_balancer(LoadBalancer::Random);

    let client = HazelcastClient::new(round_robin).await?;
    client.shutdown().await?;
    Ok(())
}
```

## TLS/SSL Configuration

Secure cluster connections:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::config::TlsConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tls = TlsConfig::new()
        .enabled(true)
        // Path to CA certificate
        .ca_certificate_path("/etc/hazelcast/ca.pem")
        // Client certificate for mTLS
        .client_certificate_path("/etc/hazelcast/client.pem")
        .client_key_path("/etc/hazelcast/client-key.pem")
        // Verify server hostname
        .hostname_verification(true);

    let config = ClientConfig::new()
        .cluster_name("secure-cluster")
        .addresses(vec!["hazelcast.example.com:5701"])
        .tls(tls);

    let client = HazelcastClient::new(config).await?;
    client.shutdown().await?;
    Ok(())
}
```

## Complete Resilient Configuration

Production-ready configuration combining all resilience features:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::config::{
    ConnectionRetryConfig, QuorumConfig, QuorumType, TlsConfig, LoadBalancer,
};
use hazelcast_client::lifecycle::{LifecycleListener, LifecycleEvent, LifecycleState};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let lifecycle_listener = LifecycleListener::new(|event: LifecycleEvent| {
        match event.state() {
            LifecycleState::ClientDisconnected => {
                log::warn!("Lost connection to cluster");
            }
            LifecycleState::ClientConnected => {
                log::info!("Reconnected to cluster");
            }
            _ => {}
        }
    });

    let config = ClientConfig::new()
        .cluster_name("production")
        .addresses(vec![
            "hazelcast-1.prod.internal:5701",
            "hazelcast-2.prod.internal:5701",
            "hazelcast-3.prod.internal:5701",
        ])
        // Connection resilience
        .connection_retry(
            ConnectionRetryConfig::new()
                .initial_backoff(Duration::from_millis(100))
                .max_backoff(Duration::from_secs(30))
                .multiplier(2.0)
                .jitter(0.2)
                .cluster_connect_timeout(Duration::from_secs(600))
        )
        // Heartbeat monitoring
        .heartbeat_interval(Duration::from_secs(5))
        .heartbeat_timeout(Duration::from_secs(30))
        // Smart routing for optimal performance
        .smart_routing(true)
        .load_balancer(LoadBalancer::RoundRobin)
        // Split-brain protection
        .add_quorum_config(
            QuorumConfig::new("critical-writes")
                .size(2)
                .quorum_type(QuorumType::Write)
        )
        // TLS encryption
        .tls(
            TlsConfig::new()
                .enabled(true)
                .ca_certificate_path("/etc/ssl/hazelcast/ca.pem")
                .hostname_verification(true)
        )
        // Lifecycle monitoring
        .add_lifecycle_listener(lifecycle_listener);

    let client = HazelcastClient::new(config).await?;

    log::info!("Connected to Hazelcast cluster");

    // Application logic here...

    client.shutdown().await?;
    Ok(())
}
```

## Best Practices

| Practice | Rationale |
|----------|-----------|
| Use multiple seed addresses | Prevents single point of failure during initial connection |
| Configure appropriate timeouts | Balance between fast failure detection and tolerance for transient issues |
| Enable smart routing | Reduces latency by routing directly to data owners |
| Use quorum for critical data | Prevents inconsistency during network partitions |
| Monitor lifecycle events | Enables application-level resilience logic |
| Use TLS in production | Protects data in transit |

## Next Steps

- [Getting Started](getting-started.md) - Basic client setup
- [Transactions](transactions.md) - Durable transactions across failures
- [Distributed Caching](distributed-caching.md) - Local caching for resilience
