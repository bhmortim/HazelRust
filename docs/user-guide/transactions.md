# Transactions

Hazelcast supports distributed transactions across multiple data structures. This guide covers transaction patterns including XA transactions for two-phase commit scenarios.

## Basic Transactions

### Simple Transaction

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::transaction::TransactionOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HazelcastClient::new(
        ClientConfig::new().addresses(vec!["127.0.0.1:5701"])
    ).await?;

    // Begin a transaction
    let tx = client.new_transaction(TransactionOptions::default()).await?;

    // Get transactional map handles
    let accounts = tx.get_map::<String, i64>("accounts").await?;

    // Perform operations within the transaction
    let from_balance = accounts.get(&"alice".to_string()).await?.unwrap_or(0);
    let to_balance = accounts.get(&"bob".to_string()).await?.unwrap_or(0);

    let transfer_amount = 100_i64;

    if from_balance >= transfer_amount {
        accounts.put("alice".to_string(), from_balance - transfer_amount).await?;
        accounts.put("bob".to_string(), to_balance + transfer_amount).await?;

        // Commit the transaction
        tx.commit().await?;
        println!("Transfer completed successfully");
    } else {
        // Rollback on insufficient funds
        tx.rollback().await?;
        println!("Insufficient funds, transaction rolled back");
    }

    client.shutdown().await?;
    Ok(())
}
```

### Transaction Options

```rust
use hazelcast_client::transaction::{TransactionOptions, TransactionType, Durability};
use std::time::Duration;

// Configure transaction behavior
let options = TransactionOptions::new()
    // Transaction type
    .transaction_type(TransactionType::TwoPhase)
    // Timeout for the entire transaction
    .timeout(Duration::from_secs(60))
    // Number of backup copies for durability
    .durability(Durability::new(1));
```

## Transaction Types

| Type | Description | Use Case |
|------|-------------|----------|
| `OnePhase` | Single-phase commit, faster but less durable | Low-latency, tolerates rare failures |
| `TwoPhase` | Two-phase commit, full ACID guarantees | Financial transactions, strict consistency |

```rust
use hazelcast_client::transaction::{TransactionOptions, TransactionType};

// One-phase: faster, used when durability is less critical
let fast_tx = TransactionOptions::new()
    .transaction_type(TransactionType::OnePhase);

// Two-phase: slower but guaranteed consistency
let safe_tx = TransactionOptions::new()
    .transaction_type(TransactionType::TwoPhase);
```

## Multi-Map Transactions

Atomic operations across multiple maps:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::transaction::TransactionOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HazelcastClient::new(
        ClientConfig::new().addresses(vec!["127.0.0.1:5701"])
    ).await?;

    let tx = client.new_transaction(TransactionOptions::default()).await?;

    // Multiple maps in the same transaction
    let orders = tx.get_map::<String, String>("orders").await?;
    let inventory = tx.get_map::<String, i32>("inventory").await?;
    let audit_log = tx.get_map::<String, String>("audit_log").await?;

    let product_id = "PROD-001";
    let order_id = "ORD-12345";

    // Check inventory
    let stock = inventory.get(&product_id.to_string()).await?.unwrap_or(0);

    if stock > 0 {
        // Decrement inventory
        inventory.put(product_id.to_string(), stock - 1).await?;

        // Create order
        orders.put(order_id.to_string(), product_id.to_string()).await?;

        // Log the transaction
        audit_log.put(
            format!("{}:{}", order_id, chrono::Utc::now().timestamp()),
            format!("Order {} placed for product {}", order_id, product_id),
        ).await?;

        tx.commit().await?;
        println!("Order placed successfully");
    } else {
        tx.rollback().await?;
        println!("Out of stock");
    }

    client.shutdown().await?;
    Ok(())
}
```

## XA Transactions

XA transactions enable coordination with external transaction managers for two-phase commit across multiple systems.

### Basic XA Pattern

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::transaction::{XAResource, Xid};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HazelcastClient::new(
        ClientConfig::new().addresses(vec!["127.0.0.1:5701"])
    ).await?;

    // Get the XA resource
    let xa_resource = client.get_xa_resource();

    // Create a transaction ID (typically from your TM)
    let xid = Xid::new(
        1,                          // format ID
        b"global-tx-123".to_vec(),  // global transaction ID
        b"branch-001".to_vec(),     // branch qualifier
    );

    // Start the XA transaction
    xa_resource.start(&xid).await?;

    // Perform operations
    let map = client.get_map::<String, String>("xa-data").await?;
    map.put("key".to_string(), "value".to_string()).await?;

    // End the transaction branch
    xa_resource.end(&xid).await?;

    // Prepare phase
    let prepare_result = xa_resource.prepare(&xid).await?;

    if prepare_result.is_ok() {
        // Commit phase
        xa_resource.commit(&xid, false).await?;
        println!("XA transaction committed");
    } else {
        // Rollback on prepare failure
        xa_resource.rollback(&xid).await?;
        println!("XA transaction rolled back");
    }

    client.shutdown().await?;
    Ok(())
}
```

### Integration with External Transaction Manager

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::transaction::XAResource;

/// Example showing integration pattern with an external TM
struct DistributedTransaction {
    hazelcast_resource: XAResource,
    // database_resource: DatabaseXAResource,
    // message_queue_resource: MQXAResource,
}

impl DistributedTransaction {
    async fn execute<F, R>(&self, xid: &hazelcast_client::transaction::Xid, work: F) -> Result<R, Box<dyn std::error::Error>>
    where
        F: FnOnce() -> Result<R, Box<dyn std::error::Error>>,
    {
        // Start all branches
        self.hazelcast_resource.start(xid).await?;
        // self.database_resource.start(xid).await?;

        // Execute business logic
        let result = work()?;

        // End all branches
        self.hazelcast_resource.end(xid).await?;
        // self.database_resource.end(xid).await?;

        // Prepare all resources
        let hz_prepared = self.hazelcast_resource.prepare(xid).await?;
        // let db_prepared = self.database_resource.prepare(xid).await?;

        // Commit or rollback all
        if hz_prepared.is_ok() /* && db_prepared.is_ok() */ {
            self.hazelcast_resource.commit(xid, false).await?;
            // self.database_resource.commit(xid, false).await?;
        } else {
            self.hazelcast_resource.rollback(xid).await?;
            // self.database_resource.rollback(xid).await?;
            return Err("Prepare failed".into());
        }

        Ok(result)
    }
}
```

### Recovery of In-Doubt Transactions

Handle transactions that were interrupted:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HazelcastClient::new(
        ClientConfig::new().addresses(vec!["127.0.0.1:5701"])
    ).await?;

    let xa_resource = client.get_xa_resource();

    // Recover in-doubt transactions
    let pending_xids = xa_resource.recover().await?;

    for xid in pending_xids {
        println!("Found in-doubt transaction: {:?}", xid);

        // Decide whether to commit or rollback based on external TM state
        // This is typically coordinated with your transaction manager
        let should_commit = check_external_tm_decision(&xid).await;

        if should_commit {
            xa_resource.commit(&xid, false).await?;
            println!("Recovered transaction committed");
        } else {
            xa_resource.rollback(&xid).await?;
            println!("Recovered transaction rolled back");
        }
    }

    client.shutdown().await?;
    Ok(())
}

async fn check_external_tm_decision(
    _xid: &hazelcast_client::transaction::Xid
) -> bool {
    // Query your transaction manager for the decision
    true
}
```

## Transactional Data Structures

### Transactional Map Operations

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::transaction::TransactionOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HazelcastClient::new(
        ClientConfig::new().addresses(vec!["127.0.0.1:5701"])
    ).await?;

    let tx = client.new_transaction(TransactionOptions::default()).await?;
    let map = tx.get_map::<String, i32>("tx-map").await?;

    // Available operations in transaction context
    map.put("key1".to_string(), 100).await?;
    map.put_if_absent("key2".to_string(), 200).await?;

    let value = map.get(&"key1".to_string()).await?;
    let removed = map.remove(&"key2".to_string()).await?;

    let contains = map.contains_key(&"key1".to_string()).await?;
    let size = map.size().await?;

    // get_for_update locks the key until commit/rollback
    let locked_value = map.get_for_update(&"key1".to_string()).await?;

    tx.commit().await?;
    client.shutdown().await?;
    Ok(())
}
```

### Transactional Queue

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::transaction::TransactionOptions;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HazelcastClient::new(
        ClientConfig::new().addresses(vec!["127.0.0.1:5701"])
    ).await?;

    let tx = client.new_transaction(TransactionOptions::default()).await?;
    let queue = tx.get_queue::<String>("tx-queue").await?;

    // Offer with timeout
    queue.offer("task-1".to_string(), Duration::from_secs(5)).await?;

    // Poll with timeout
    if let Some(task) = queue.poll(Duration::from_secs(5)).await? {
        println!("Processing: {}", task);
        // Process task...
    }

    let size = queue.size().await?;
    println!("Queue size: {}", size);

    tx.commit().await?;
    client.shutdown().await?;
    Ok(())
}
```

## Error Handling Patterns

### Automatic Rollback on Error

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::transaction::TransactionOptions;

async fn transfer_funds(
    client: &HazelcastClient,
    from: &str,
    to: &str,
    amount: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let tx = client.new_transaction(TransactionOptions::default()).await?;

    let result = async {
        let accounts = tx.get_map::<String, i64>("accounts").await?;

        let from_balance = accounts.get(&from.to_string()).await?
            .ok_or("Source account not found")?;

        if from_balance < amount {
            return Err("Insufficient funds".into());
        }

        let to_balance = accounts.get(&to.to_string()).await?
            .ok_or("Destination account not found")?;

        accounts.put(from.to_string(), from_balance - amount).await?;
        accounts.put(to.to_string(), to_balance + amount).await?;

        Ok::<(), Box<dyn std::error::Error>>(())
    }.await;

    match result {
        Ok(()) => {
            tx.commit().await?;
            Ok(())
        }
        Err(e) => {
            tx.rollback().await?;
            Err(e)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HazelcastClient::new(
        ClientConfig::new().addresses(vec!["127.0.0.1:5701"])
    ).await?;

    match transfer_funds(&client, "alice", "bob", 100).await {
        Ok(()) => println!("Transfer successful"),
        Err(e) => println!("Transfer failed: {}", e),
    }

    client.shutdown().await?;
    Ok(())
}
```

### Retry Pattern

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::transaction::TransactionOptions;
use std::time::Duration;

async fn with_retry<F, T, E>(
    max_attempts: u32,
    delay: Duration,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send>>,
    E: std::fmt::Display,
{
    let mut attempts = 0;

    loop {
        attempts += 1;
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) if attempts < max_attempts => {
                eprintln!("Attempt {} failed: {}, retrying...", attempts, e);
                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(e),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HazelcastClient::new(
        ClientConfig::new().addresses(vec!["127.0.0.1:5701"])
    ).await?;

    let client_ref = &client;

    with_retry(3, Duration::from_millis(100), || {
        Box::pin(async move {
            let tx = client_ref.new_transaction(TransactionOptions::default()).await?;
            let map = tx.get_map::<String, i32>("retry-test").await?;

            map.put("counter".to_string(), 1).await?;
            tx.commit().await?;

            Ok::<(), Box<dyn std::error::Error>>(())
        })
    }).await?;

    client.shutdown().await?;
    Ok(())
}
```

## Best Practices

1. **Keep transactions short**: Long-running transactions increase contention
2. **Use appropriate transaction type**: One-phase for speed, two-phase for consistency
3. **Always handle rollback**: Ensure cleanup on any error path
4. **Avoid nested transactions**: Hazelcast doesn't support savepoints
5. **Set reasonable timeouts**: Prevent indefinite blocking
6. **Use `get_for_update`**: When you need pessimistic locking

## Next Steps

- [Cluster Failover](cluster-failover.md) - Ensure transaction durability during failures
- [Getting Started](getting-started.md) - Basic map operations
