//! Example demonstrating Prometheus metrics export.
//!
//! This example shows how to expose Hazelcast client metrics
//! via an HTTP endpoint for Prometheus scraping.
//!
//! Run with: `cargo run --example prometheus_metrics --features metrics`

use std::sync::Arc;
use std::time::Duration;

use hazelcast_client::{
    ClientConfigBuilder, HazelcastClient, PrometheusExporter, PrometheusExporterBuilder,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create the Prometheus exporter (installs global metrics recorder)
    let exporter = PrometheusExporterBuilder::new()
        .with_prefix("hazelcast")
        .idle_timeout(Duration::from_secs(300))
        .latency_buckets(vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ])
        .build()?;

    let exporter = Arc::new(exporter);

    // Create client configuration
    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address("127.0.0.1:5701".parse()?)
        .build()?;

    // Connect to the cluster
    let client = HazelcastClient::new(config).await?;

    // Start the background metrics recorder
    let recorder_handle = PrometheusExporter::start_recorder(
        client.statistics_collector().clone(),
        Duration::from_secs(5),
    );

    // Spawn a simple HTTP server to expose metrics
    let exporter_clone = Arc::clone(&exporter);
    let metrics_server = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:9090").await.unwrap();
        println!("Metrics available at http://127.0.0.1:9090/metrics");

        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let exporter = Arc::clone(&exporter_clone);

            tokio::spawn(async move {
                let mut buf = vec![0u8; 1024];
                let _ = socket.read(&mut buf).await;

                let metrics = exporter.render();
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
                     Content-Length: {}\r\n\
                     \r\n\
                     {}",
                    metrics.len(),
                    metrics
                );

                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });

    // Perform some operations to generate metrics
    let map = client.get_map::<String, String>("metrics-demo");

    println!("Performing operations...");
    for i in 0..100 {
        let key = format!("key-{}", i);
        let value = format!("value-{}", i);

        map.put(key.clone(), value).await?;
        let _ = map.get(&key).await?;

        if i % 10 == 0 {
            println!("Completed {} operations", i + 1);
        }
    }

    println!("\nMetrics snapshot:");
    println!("{}", exporter.render());

    // Keep running for demonstration
    println!("\nPress Ctrl+C to exit...");
    tokio::signal::ctrl_c().await?;

    // Cleanup
    recorder_handle.stop();
    client.shutdown().await?;
    metrics_server.abort();

    Ok(())
}
