//! Example: IMap entry listeners.
//!
//! Demonstrates how to subscribe to map entry events (add, update, remove, evict).
//!
//! Run with: `cargo run --example map_listeners`
//!
//! Requires a Hazelcast cluster running on localhost:5701.

use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::listener::EntryListenerConfig;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .cluster_name("dev")
        .add_address("127.0.0.1:5701".parse()?)
        .build()?;

    let client = HazelcastClient::new(config).await?;
    let map = client.get_map::<String, String>("listener-demo");

    // Track event counts
    let add_count = Arc::new(AtomicUsize::new(0));
    let update_count = Arc::new(AtomicUsize::new(0));
    let remove_count = Arc::new(AtomicUsize::new(0));

    let add_counter = add_count.clone();
    let update_counter = update_count.clone();
    let remove_counter = remove_count.clone();

    // Configure listener for all events with values included
    let listener_config = EntryListenerConfig::all();

    // Add entry listener
    let listener_reg = map
        .add_entry_listener(
            listener_config,
            move |event| {
                match event.event_type {
                    hazelcast_client::listener::EntryEventType::Added => {
                        add_counter.fetch_add(1, Ordering::SeqCst);
                        println!(
                            "[ADDED] {} -> {:?}",
                            event.key,
                            event.new_value,
                        );
                    }
                    hazelcast_client::listener::EntryEventType::Updated => {
                        update_counter.fetch_add(1, Ordering::SeqCst);
                        println!(
                            "[UPDATED] {} -> {:?} (was: {:?})",
                            event.key,
                            event.new_value,
                            event.old_value,
                        );
                    }
                    hazelcast_client::listener::EntryEventType::Removed => {
                        remove_counter.fetch_add(1, Ordering::SeqCst);
                        println!("[REMOVED] {}", event.key);
                    }
                    hazelcast_client::listener::EntryEventType::Evicted => {
                        println!("[EVICTED] {}", event.key);
                    }
                    hazelcast_client::listener::EntryEventType::Expired => {
                        println!("[EXPIRED] {}", event.key);
                    }
                }
            },
        )
        .await?;

    println!("Listener registered: {:?}", listener_reg);

    // Perform operations that trigger events
    println!("\n--- Performing map operations ---\n");

    map.put("user:1".into(), "Alice".into()).await?;
    map.put("user:2".into(), "Bob".into()).await?;
    map.put("user:1".into(), "Alice Smith".into()).await?; // update
    map.remove(&"user:2".into()).await?;

    // Allow time for events to arrive
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("\n--- Event Statistics ---");
    println!("Added:   {}", add_count.load(Ordering::SeqCst));
    println!("Updated: {}", update_count.load(Ordering::SeqCst));
    println!("Removed: {}", remove_count.load(Ordering::SeqCst));

    // Clean up
    map.remove_entry_listener(&listener_reg).await?;
    map.clear().await?;
    client.shutdown().await?;

    Ok(())
}
