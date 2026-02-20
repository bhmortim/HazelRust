# SQL Queries

Hazelcast supports SQL for querying distributed data structures. This guide covers executing queries, streaming results, and parameterized statements.

## Prerequisites

Ensure your maps have proper mappings defined on the cluster:

```sql
-- Execute on the cluster
CREATE MAPPING users (
    id BIGINT,
    username VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP
)
TYPE IMap
OPTIONS (
    'keyFormat' = 'bigint',
    'valueFormat' = 'json-flat'
);
```

## Basic Queries

### Simple SELECT

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .add_address("127.0.0.1:5701".parse()?)
        .build()?;
    let client = HazelcastClient::new(config).await?;

    // Execute a query and collect results
    let result = client.sql()
        .execute("SELECT id, username, email FROM users")
        .await?;

    // Iterate over rows
    for row in result.rows() {
        let id: i64 = row.get("id")?;
        let username: String = row.get("username")?;
        let email: String = row.get("email")?;
        println!("User {}: {} <{}>", id, username, email);
    }

    client.shutdown().await?;
    Ok(())
}
```

### Parameterized Queries

Always use parameters to prevent SQL injection:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::sql::SqlStatement;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .add_address("127.0.0.1:5701".parse()?)
        .build()?;
    let client = HazelcastClient::new(config).await?;

    // Using positional parameters
    let statement = SqlStatement::new("SELECT * FROM users WHERE id = ?")
        .add_parameter(42_i64);

    let result = client.sql().execute_statement(statement).await?;

    for row in result.rows() {
        let username: String = row.get("username")?;
        println!("Found user: {}", username);
    }

    // Multiple parameters
    let search_query = SqlStatement::new(
        "SELECT * FROM users WHERE username LIKE ? AND created_at > ?"
    )
        .add_parameter("%alice%".to_string())
        .add_parameter("2024-01-01T00:00:00Z".to_string());

    let results = client.sql().execute_statement(search_query).await?;

    client.shutdown().await?;
    Ok(())
}
```

## Filtering and Sorting

### WHERE Clauses

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .add_address("127.0.0.1:5701".parse()?)
        .build()?;
    let client = HazelcastClient::new(config).await?;

    // Comparison operators
    let active_users = client.sql()
        .execute("SELECT * FROM users WHERE active = true")
        .await?;

    // Range queries
    let recent = client.sql()
        .execute("SELECT * FROM orders WHERE amount BETWEEN 100 AND 500")
        .await?;

    // Pattern matching
    let admins = client.sql()
        .execute("SELECT * FROM users WHERE email LIKE '%@admin.example.com'")
        .await?;

    // NULL checks
    let unverified = client.sql()
        .execute("SELECT * FROM users WHERE verified_at IS NULL")
        .await?;

    // IN clause
    let specific = client.sql()
        .execute("SELECT * FROM users WHERE status IN ('active', 'pending')")
        .await?;

    client.shutdown().await?;
    Ok(())
}
```

### ORDER BY and LIMIT

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .add_address("127.0.0.1:5701".parse()?)
        .build()?;
    let client = HazelcastClient::new(config).await?;

    // Sorting
    let result = client.sql()
        .execute("SELECT * FROM orders ORDER BY created_at DESC")
        .await?;

    // Pagination with LIMIT and OFFSET
    let page = client.sql()
        .execute("SELECT * FROM products ORDER BY name LIMIT 20 OFFSET 40")
        .await?;

    // Top N query
    let top_customers = client.sql()
        .execute(
            "SELECT customer_id, SUM(amount) as total \
             FROM orders \
             GROUP BY customer_id \
             ORDER BY total DESC \
             LIMIT 10"
        )
        .await?;

    for row in top_customers.rows() {
        let customer_id: i64 = row.get("customer_id")?;
        let total: f64 = row.get("total")?;
        println!("Customer {}: ${:.2}", customer_id, total);
    }

    client.shutdown().await?;
    Ok(())
}
```

## Aggregations

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .add_address("127.0.0.1:5701".parse()?)
        .build()?;
    let client = HazelcastClient::new(config).await?;

    // Count
    let result = client.sql()
        .execute("SELECT COUNT(*) as total FROM users WHERE active = true")
        .await?;

    if let Some(row) = result.rows().next() {
        let count: i64 = row.get("total")?;
        println!("Active users: {}", count);
    }

    // Aggregate functions
    let stats = client.sql()
        .execute(
            "SELECT \
                COUNT(*) as count, \
                SUM(amount) as total, \
                AVG(amount) as average, \
                MIN(amount) as minimum, \
                MAX(amount) as maximum \
             FROM orders"
        )
        .await?;

    if let Some(row) = stats.rows().next() {
        println!("Order Statistics:");
        println!("  Count: {}", row.get::<i64>("count")?);
        println!("  Total: ${:.2}", row.get::<f64>("total")?);
        println!("  Average: ${:.2}", row.get::<f64>("average")?);
    }

    // GROUP BY with HAVING
    let high_volume = client.sql()
        .execute(
            "SELECT category, COUNT(*) as count, SUM(amount) as total \
             FROM products \
             GROUP BY category \
             HAVING COUNT(*) > 100 \
             ORDER BY total DESC"
        )
        .await?;

    client.shutdown().await?;
    Ok(())
}
```

## Streaming Results

For large result sets, use streaming to avoid loading all rows into memory:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::sql::SqlStatement;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .add_address("127.0.0.1:5701".parse()?)
        .build()?;
    let client = HazelcastClient::new(config).await?;

    // Configure cursor buffer size for memory control
    let statement = SqlStatement::new("SELECT * FROM large_table")
        .cursor_buffer_size(1000);

    let result = client.sql().execute_statement(statement).await?;

    // Stream rows one at a time
    let mut stream = result.into_stream();
    let mut count = 0;

    while let Some(row_result) = stream.next().await {
        let row = row_result?;
        let id: i64 = row.get("id")?;

        count += 1;
        if count % 10_000 == 0 {
            println!("Processed {} rows, current id: {}", count, id);
        }
    }

    println!("Total rows processed: {}", count);

    client.shutdown().await?;
    Ok(())
}
```

## Joins

Query across multiple maps:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .add_address("127.0.0.1:5701".parse()?)
        .build()?;
    let client = HazelcastClient::new(config).await?;

    // Inner join
    let result = client.sql()
        .execute(
            "SELECT o.id, o.amount, u.username \
             FROM orders o \
             JOIN users u ON o.user_id = u.id \
             WHERE o.status = 'completed'"
        )
        .await?;

    for row in result.rows() {
        let order_id: i64 = row.get("id")?;
        let amount: f64 = row.get("amount")?;
        let username: String = row.get("username")?;
        println!("Order {} by {}: ${:.2}", order_id, username, amount);
    }

    // Left join for including unmatched rows
    let all_users = client.sql()
        .execute(
            "SELECT u.username, COUNT(o.id) as order_count \
             FROM users u \
             LEFT JOIN orders o ON u.id = o.user_id \
             GROUP BY u.username"
        )
        .await?;

    client.shutdown().await?;
    Ok(())
}
```

## DML Operations

### INSERT, UPDATE, DELETE

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::sql::SqlStatement;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .add_address("127.0.0.1:5701".parse()?)
        .build()?;
    let client = HazelcastClient::new(config).await?;

    // INSERT (SINK INTO for IMap)
    let insert = SqlStatement::new(
        "SINK INTO users (__key, id, username, email) VALUES (?, ?, ?, ?)"
    )
        .add_parameter(1001_i64)
        .add_parameter(1001_i64)
        .add_parameter("newuser".to_string())
        .add_parameter("newuser@example.com".to_string());

    let result = client.sql().execute_statement(insert).await?;
    println!("Rows affected: {}", result.update_count());

    // UPDATE
    let update = SqlStatement::new(
        "UPDATE users SET email = ? WHERE id = ?"
    )
        .add_parameter("updated@example.com".to_string())
        .add_parameter(1001_i64);

    let result = client.sql().execute_statement(update).await?;
    println!("Rows updated: {}", result.update_count());

    // DELETE
    let delete = SqlStatement::new("DELETE FROM users WHERE id = ?")
        .add_parameter(1001_i64);

    let result = client.sql().execute_statement(delete).await?;
    println!("Rows deleted: {}", result.update_count());

    client.shutdown().await?;
    Ok(())
}
```

## Query Timeouts

Configure timeouts for long-running queries:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::sql::SqlStatement;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .add_address("127.0.0.1:5701".parse()?)
        .build()?;
    let client = HazelcastClient::new(config).await?;

    let statement = SqlStatement::new(
        "SELECT * FROM large_table WHERE complex_condition(data)"
    )
        .timeout(Duration::from_secs(30));

    match client.sql().execute_statement(statement).await {
        Ok(result) => {
            println!("Query completed with {} rows", result.rows().count());
        }
        Err(e) if e.is_timeout() => {
            println!("Query timed out");
        }
        Err(e) => return Err(e.into()),
    }

    client.shutdown().await?;
    Ok(())
}
```

## Type Mapping

| SQL Type | Rust Type |
|----------|-----------|
| `BOOLEAN` | `bool` |
| `TINYINT` | `i8` |
| `SMALLINT` | `i16` |
| `INTEGER` | `i32` |
| `BIGINT` | `i64` |
| `REAL` | `f32` |
| `DOUBLE` | `f64` |
| `DECIMAL` | `rust_decimal::Decimal` |
| `VARCHAR` | `String` |
| `DATE` | `chrono::NaiveDate` |
| `TIME` | `chrono::NaiveTime` |
| `TIMESTAMP` | `chrono::NaiveDateTime` |
| `TIMESTAMP WITH TIME ZONE` | `chrono::DateTime<Utc>` |

## Best Practices

1. **Always use parameterized queries** to prevent SQL injection
2. **Stream large result sets** to manage memory
3. **Create indexes** on frequently filtered columns
4. **Use appropriate cursor buffer sizes** based on row size
5. **Set query timeouts** for production workloads

## Next Steps

- [Transactions](transactions.md) - Combine SQL with transactions
- [Getting Started](getting-started.md) - Map operations without SQL
