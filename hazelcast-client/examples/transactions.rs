//! Example: ACID transactions across multiple data structures
//!
//! Demonstrates transactional operations spanning maps and queues.
//!
//! Run with: `cargo run --example transactions`

use hazelcast_client::{Client, ClientConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new()
        .cluster_name("dev")
        .address("127.0.0.1:5701");

    let client = Client::new(config).await?;

    // Initialize accounts
    let accounts = client.get_map::<String, i64>("accounts").await?;
    accounts.put("alice".into(), 1000).await?;
    accounts.put("bob".into(), 500).await?;

    println!("Initial balances:");
    println!("  Alice: ${}", accounts.get(&"alice".into()).await?.unwrap_or(0));
    println!("  Bob:   ${}", accounts.get(&"bob".into()).await?.unwrap_or(0));

    // Successful transaction: transfer $200 from Alice to Bob
    println!("\n--- Transaction 1: Transfer $200 from Alice to Bob ---");
    {
        let ctx = client
            .new_transaction_context()
            .timeout(Duration::from_secs(30))
            .build()
            .await?;

        ctx.begin().await?;

        let tx_accounts = ctx.get_map::<String, i64>("accounts").await?;
        let tx_log = ctx.get_queue::<String>("transaction-log").await?;

        let alice_balance = tx_accounts.get(&"alice".into()).await?.unwrap_or(0);
        let bob_balance = tx_accounts.get(&"bob".into()).await?.unwrap_or(0);

        let transfer_amount = 200;

        if alice_balance >= transfer_amount {
            tx_accounts
                .put("alice".into(), alice_balance - transfer_amount)
                .await?;
            tx_accounts
                .put("bob".into(), bob_balance + transfer_amount)
                .await?;
            tx_log
                .offer(format!(
                    "TRANSFER: Alice -> Bob: ${}",
                    transfer_amount
                ))
                .await?;

            ctx.commit().await?;
            println!("  Transaction committed successfully");
        } else {
            ctx.rollback().await?;
            println!("  Transaction rolled back: insufficient funds");
        }
    }

    println!("\nBalances after transfer:");
    println!("  Alice: ${}", accounts.get(&"alice".into()).await?.unwrap_or(0));
    println!("  Bob:   ${}", accounts.get(&"bob".into()).await?.unwrap_or(0));

    // Failed transaction: attempt to transfer $1000 from Bob (insufficient funds)
    println!("\n--- Transaction 2: Attempt $1000 transfer from Bob (will fail) ---");
    {
        let ctx = client
            .new_transaction_context()
            .timeout(Duration::from_secs(30))
            .build()
            .await?;

        ctx.begin().await?;

        let tx_accounts = ctx.get_map::<String, i64>("accounts").await?;

        let bob_balance = tx_accounts.get(&"bob".into()).await?.unwrap_or(0);
        let transfer_amount = 1000;

        if bob_balance >= transfer_amount {
            tx_accounts
                .put("bob".into(), bob_balance - transfer_amount)
                .await?;
            tx_accounts
                .put("alice".into(), accounts.get(&"alice".into()).await?.unwrap_or(0) + transfer_amount)
                .await?;

            ctx.commit().await?;
            println!("  Transaction committed");
        } else {
            ctx.rollback().await?;
            println!("  Transaction rolled back: Bob has ${}, needs ${}", bob_balance, transfer_amount);
        }
    }

    println!("\nFinal balances (unchanged from failed transaction):");
    println!("  Alice: ${}", accounts.get(&"alice".into()).await?.unwrap_or(0));
    println!("  Bob:   ${}", accounts.get(&"bob".into()).await?.unwrap_or(0));

    // Show transaction log
    let log = client.get_queue::<String>("transaction-log").await?;
    println!("\n--- Transaction Log ---");
    while let Some(entry) = log.poll().await? {
        println!("  {}", entry);
    }

    // Clean up
    accounts.clear().await?;
    client.shutdown().await?;

    println!("\nDone!");
    Ok(())
}
