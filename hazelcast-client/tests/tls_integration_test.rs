//! TLS integration tests for Hazelcast client connections.
//!
//! These tests require a TLS-enabled Hazelcast cluster and are ignored by default.
//! Run with: `cargo test --features tls -- --ignored`
//!
//! Environment variables for configuration:
//! - `HZ_TLS_ADDRESS`: Hazelcast server address (default: 127.0.0.1:5701)
//! - `HZ_TLS_CA_CERT`: Path to CA certificate file
//! - `HZ_TLS_CLIENT_CERT`: Path to client certificate file (for mTLS)
//! - `HZ_TLS_CLIENT_KEY`: Path to client key file (for mTLS)

#![cfg(feature = "tls")]

use std::env;
use std::net::SocketAddr;
use std::time::Duration;

use hazelcast_client::config::{ClientConfigBuilder, NetworkConfigBuilder, TlsConfigBuilder};
use hazelcast_client::connection::ConnectionManager;

fn get_tls_address() -> SocketAddr {
    env::var("HZ_TLS_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:5701".to_string())
        .parse()
        .expect("Invalid HZ_TLS_ADDRESS")
}

fn get_ca_cert_path() -> Option<String> {
    env::var("HZ_TLS_CA_CERT").ok()
}

fn get_client_cert_path() -> Option<String> {
    env::var("HZ_TLS_CLIENT_CERT").ok()
}

fn get_client_key_path() -> Option<String> {
    env::var("HZ_TLS_CLIENT_KEY").ok()
}

#[tokio::test]
#[ignore = "requires TLS-enabled Hazelcast cluster"]
async fn test_tls_connection_with_default_ca() {
    let address = get_tls_address();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(address)
        .connection_timeout(Duration::from_secs(10))
        .network(|n| n.tls(|t| t.enabled(true)))
        .build()
        .expect("failed to build config");

    let manager = ConnectionManager::from_config(config);
    let result = manager.connect_to(address).await;

    assert!(result.is_ok(), "TLS connection failed: {:?}", result.err());
    assert!(manager.is_connected(&address).await);
    assert_eq!(manager.connection_count().await, 1);

    manager.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires TLS-enabled Hazelcast cluster with custom CA"]
async fn test_tls_connection_with_custom_ca() {
    let address = get_tls_address();
    let ca_cert_path = get_ca_cert_path().expect("HZ_TLS_CA_CERT not set");

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(address)
        .connection_timeout(Duration::from_secs(10))
        .network(|n| {
            n.tls(|t| {
                t.enabled(true)
                    .ca_cert_path(&ca_cert_path)
            })
        })
        .build()
        .expect("failed to build config");

    let manager = ConnectionManager::from_config(config);
    let result = manager.connect_to(address).await;

    assert!(result.is_ok(), "TLS connection with custom CA failed: {:?}", result.err());
    assert!(manager.is_connected(&address).await);

    manager.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires TLS-enabled Hazelcast cluster with mTLS"]
async fn test_mutual_tls_connection() {
    let address = get_tls_address();
    let ca_cert_path = get_ca_cert_path().expect("HZ_TLS_CA_CERT not set");
    let client_cert_path = get_client_cert_path().expect("HZ_TLS_CLIENT_CERT not set");
    let client_key_path = get_client_key_path().expect("HZ_TLS_CLIENT_KEY not set");

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(address)
        .connection_timeout(Duration::from_secs(10))
        .network(|n| {
            n.tls(|t| {
                t.enabled(true)
                    .ca_cert_path(&ca_cert_path)
                    .client_auth(&client_cert_path, &client_key_path)
            })
        })
        .build()
        .expect("failed to build config");

    let manager = ConnectionManager::from_config(config);
    let result = manager.connect_to(address).await;

    assert!(
        result.is_ok(),
        "Mutual TLS connection failed: {:?}",
        result.err()
    );
    assert!(manager.is_connected(&address).await);

    manager.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires TLS-enabled Hazelcast cluster"]
async fn test_tls_connection_start_lifecycle() {
    let address = get_tls_address();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(address)
        .connection_timeout(Duration::from_secs(10))
        .network(|n| n.tls(|t| t.enabled(true)))
        .build()
        .expect("failed to build config");

    let manager = ConnectionManager::from_config(config);

    let mut lifecycle_rx = manager.subscribe_lifecycle();

    let start_result = manager.start().await;
    assert!(start_result.is_ok(), "TLS start failed: {:?}", start_result.err());

    let mut received_events = Vec::new();
    while let Ok(event) = tokio::time::timeout(Duration::from_millis(100), lifecycle_rx.recv()).await
    {
        if let Ok(e) = event {
            received_events.push(e);
        }
    }

    assert!(
        received_events
            .iter()
            .any(|e| matches!(e, hazelcast_client::listener::LifecycleEvent::Started)),
        "Expected Started lifecycle event"
    );

    manager.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires TLS-enabled Hazelcast cluster"]
async fn test_tls_connection_events() {
    let address = get_tls_address();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(address)
        .connection_timeout(Duration::from_secs(10))
        .network(|n| n.tls(|t| t.enabled(true)))
        .build()
        .expect("failed to build config");

    let manager = ConnectionManager::from_config(config);
    let mut events_rx = manager.subscribe();

    manager.connect_to(address).await.expect("TLS connection failed");

    let event = tokio::time::timeout(Duration::from_secs(1), events_rx.recv())
        .await
        .expect("timeout waiting for event")
        .expect("failed to receive event");

    match event {
        hazelcast_client::connection::ConnectionEvent::Connected {
            address: conn_addr, ..
        } => {
            assert_eq!(conn_addr, address);
        }
        _ => panic!("expected Connected event, got {:?}", event),
    }

    manager.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires TLS-enabled Hazelcast cluster that rejects plain connections"]
async fn test_plain_connection_rejected_by_tls_server() {
    let address = get_tls_address();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(address)
        .connection_timeout(Duration::from_secs(5))
        .build()
        .expect("failed to build config");

    let manager = ConnectionManager::from_config(config);
    let result = manager.connect_to(address).await;

    assert!(
        result.is_err() || {
            tokio::time::sleep(Duration::from_millis(500)).await;
            !manager.is_connected(&address).await
        },
        "Plain connection should be rejected by TLS-only server"
    );
}

#[tokio::test]
#[ignore = "requires TLS-enabled Hazelcast cluster"]
async fn test_tls_connection_with_hostname_verification_disabled() {
    let address = get_tls_address();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(address)
        .connection_timeout(Duration::from_secs(10))
        .network(|n| {
            n.tls(|t| {
                t.enabled(true)
                    .verify_hostname(false)
            })
        })
        .build()
        .expect("failed to build config");

    let manager = ConnectionManager::from_config(config);
    let result = manager.connect_to(address).await;

    assert!(
        result.is_ok(),
        "TLS connection with hostname verification disabled failed: {:?}",
        result.err()
    );

    manager.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires TLS-enabled Hazelcast cluster"]
async fn test_tls_reconnection() {
    let address = get_tls_address();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(address)
        .connection_timeout(Duration::from_secs(10))
        .network(|n| n.tls(|t| t.enabled(true)))
        .retry(|r| {
            r.initial_backoff(Duration::from_millis(100))
                .max_backoff(Duration::from_secs(1))
                .max_retries(3)
        })
        .build()
        .expect("failed to build config");

    let manager = ConnectionManager::from_config(config);

    manager.connect_to(address).await.expect("initial TLS connection failed");
    assert!(manager.is_connected(&address).await);

    manager.disconnect(address).await.expect("disconnect failed");
    assert!(!manager.is_connected(&address).await);

    let reconnect_result = manager.connect_to(address).await;
    assert!(
        reconnect_result.is_ok(),
        "TLS reconnection failed: {:?}",
        reconnect_result.err()
    );
    assert!(manager.is_connected(&address).await);

    manager.shutdown().await.expect("shutdown failed");
}

#[test]
fn test_tls_config_builder_validation() {
    let result = TlsConfigBuilder::new()
        .enabled(true)
        .client_cert_path("/path/to/cert.pem")
        .build();

    assert!(
        result.is_err(),
        "Should fail when only client cert is provided without key"
    );

    let result = TlsConfigBuilder::new()
        .enabled(true)
        .client_key_path("/path/to/key.pem")
        .build();

    assert!(
        result.is_err(),
        "Should fail when only client key is provided without cert"
    );

    let result = TlsConfigBuilder::new()
        .enabled(true)
        .client_auth("/path/to/cert.pem", "/path/to/key.pem")
        .build();

    assert!(result.is_ok(), "Should succeed with both cert and key");
}

#[test]
fn test_tls_config_has_client_auth() {
    let config = TlsConfigBuilder::new()
        .enabled(true)
        .build()
        .unwrap();

    assert!(!config.has_client_auth());

    let config = TlsConfigBuilder::new()
        .enabled(true)
        .client_auth("/cert.pem", "/key.pem")
        .build()
        .unwrap();

    assert!(config.has_client_auth());
}

#[test]
fn test_network_config_tls_integration() {
    let config = NetworkConfigBuilder::new()
        .add_address("127.0.0.1:5701".parse().unwrap())
        .tls(|t| {
            t.enabled(true)
                .ca_cert_path("/ca.pem")
                .client_auth("/cert.pem", "/key.pem")
                .verify_hostname(true)
        })
        .build()
        .unwrap();

    assert!(config.tls().enabled());
    assert_eq!(
        config.tls().ca_cert_path(),
        Some(&std::path::PathBuf::from("/ca.pem"))
    );
    assert!(config.tls().has_client_auth());
    assert!(config.tls().verify_hostname());
}

#[test]
fn test_client_config_with_full_tls() {
    let config = ClientConfigBuilder::new()
        .cluster_name("secure-cluster")
        .add_address("10.0.0.1:5701".parse().unwrap())
        .network(|n| {
            n.tls(|t| {
                t.enabled(true)
                    .ca_cert_path("/certs/ca.pem")
                    .client_auth("/certs/client.pem", "/certs/client.key")
            })
        })
        .build()
        .unwrap();

    assert_eq!(config.cluster_name(), "secure-cluster");
    assert!(config.network().tls().enabled());
    assert!(config.network().tls().has_client_auth());
}
