//! Integration tests for XA transaction support.
//!
//! These tests require a running Hazelcast cluster with the following configuration:
//! - Default cluster name: "dev"
//! - Default address: 127.0.0.1:5701
//!
//! Run with: `cargo test --test xa_transaction_test -- --ignored`

use std::time::Duration;

use hazelcast_client::{
    ClientConfigBuilder, HazelcastClient, XAResource, XaTransactionState, Xid,
    XA_OK, XA_RDONLY, XA_TMFAIL, XA_TMNOFLAGS, XA_TMSUCCESS, XA_TMSUSPEND,
};

async fn create_test_client() -> HazelcastClient {
    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address("127.0.0.1:5701".parse().unwrap())
        .connection_timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    HazelcastClient::new(config).await.unwrap()
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_xa_transaction_full_lifecycle() {
    let client = create_test_client().await;

    let xid = Xid::new(0, b"test-global-txn-001", b"branch-001");
    let mut xa_txn = client.new_xa_transaction(xid.clone());

    assert_eq!(xa_txn.state(), XaTransactionState::NotStarted);

    xa_txn.start(XA_TMNOFLAGS).await.unwrap();
    assert_eq!(xa_txn.state(), XaTransactionState::Active);
    assert!(xa_txn.is_active());

    xa_txn.end(XA_TMSUCCESS).await.unwrap();
    assert_eq!(xa_txn.state(), XaTransactionState::Idle);

    let vote = xa_txn.prepare().await.unwrap();
    assert!(vote == XA_OK || vote == XA_RDONLY);
    assert_eq!(xa_txn.state(), XaTransactionState::Prepared);
    assert!(xa_txn.is_prepared());

    xa_txn.commit(false).await.unwrap();
    assert_eq!(xa_txn.state(), XaTransactionState::Committed);

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_xa_transaction_one_phase_commit() {
    let client = create_test_client().await;

    let xid = Xid::new(0, b"test-global-txn-002", b"branch-001");
    let mut xa_txn = client.new_xa_transaction(xid);

    xa_txn.start(XA_TMNOFLAGS).await.unwrap();
    xa_txn.end(XA_TMSUCCESS).await.unwrap();

    xa_txn.commit(true).await.unwrap();
    assert_eq!(xa_txn.state(), XaTransactionState::Committed);

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_xa_transaction_rollback_from_active() {
    let client = create_test_client().await;

    let xid = Xid::new(0, b"test-global-txn-003", b"branch-001");
    let mut xa_txn = client.new_xa_transaction(xid);

    xa_txn.start(XA_TMNOFLAGS).await.unwrap();
    xa_txn.end(XA_TMFAIL).await.unwrap();

    xa_txn.rollback().await.unwrap();
    assert_eq!(xa_txn.state(), XaTransactionState::RolledBack);

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_xa_transaction_rollback_after_prepare() {
    let client = create_test_client().await;

    let xid = Xid::new(0, b"test-global-txn-004", b"branch-001");
    let mut xa_txn = client.new_xa_transaction(xid);

    xa_txn.start(XA_TMNOFLAGS).await.unwrap();
    xa_txn.end(XA_TMSUCCESS).await.unwrap();
    xa_txn.prepare().await.unwrap();

    xa_txn.rollback().await.unwrap();
    assert_eq!(xa_txn.state(), XaTransactionState::RolledBack);

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_xa_transaction_suspend_resume() {
    let client = create_test_client().await;

    let xid = Xid::new(0, b"test-global-txn-005", b"branch-001");
    let mut xa_txn = client.new_xa_transaction(xid);

    xa_txn.start(XA_TMNOFLAGS).await.unwrap();
    assert_eq!(xa_txn.state(), XaTransactionState::Active);

    xa_txn.end(XA_TMSUSPEND).await.unwrap();
    assert_eq!(xa_txn.state(), XaTransactionState::Suspended);

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_xa_transaction_auto_generated_xid() {
    let client = create_test_client().await;

    let mut xa_txn = client.new_xa_transaction_auto();

    let xid = xa_txn.xid().clone();
    assert_eq!(xid.format_id(), 0);
    assert!(!xid.global_transaction_id().is_empty());

    xa_txn.start(XA_TMNOFLAGS).await.unwrap();
    xa_txn.end(XA_TMSUCCESS).await.unwrap();
    xa_txn.commit(true).await.unwrap();

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_xa_transaction_recover() {
    let client = create_test_client().await;

    let xid = Xid::new(0, b"test-global-txn-006", b"branch-001");
    let xa_txn = client.new_xa_transaction(xid);

    let recovered = xa_txn.recover(XA_TMNOFLAGS).await.unwrap();
    assert!(recovered.is_empty() || recovered.iter().all(|x| x.format_id() >= 0));

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_xa_transaction_timeout() {
    let client = create_test_client().await;

    let xid = Xid::new(0, b"test-global-txn-007", b"branch-001");
    let mut xa_txn = client.new_xa_transaction(xid);

    assert_eq!(xa_txn.get_transaction_timeout(), Duration::from_secs(120));

    let set_result = xa_txn.set_transaction_timeout(Duration::from_secs(60));
    assert!(set_result);
    assert_eq!(xa_txn.get_transaction_timeout(), Duration::from_secs(60));

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_xa_transaction_via_transaction_context() {
    let client = create_test_client().await;

    let options = hazelcast_client::TransactionOptions::new()
        .with_timeout(Duration::from_secs(30));

    let txn_ctx = client.new_transaction_context(options);

    let xid = Xid::new(0, b"test-global-txn-008", b"branch-001");
    let mut xa_txn = txn_ctx.create_xa_transaction(xid);

    xa_txn.start(XA_TMNOFLAGS).await.unwrap();
    xa_txn.end(XA_TMSUCCESS).await.unwrap();
    xa_txn.commit(true).await.unwrap();

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_multiple_xa_transactions() {
    let client = create_test_client().await;

    let xid1 = Xid::new(0, b"test-multi-txn-001", b"branch-001");
    let xid2 = Xid::new(0, b"test-multi-txn-002", b"branch-001");

    let mut xa_txn1 = client.new_xa_transaction(xid1);
    let mut xa_txn2 = client.new_xa_transaction(xid2);

    xa_txn1.start(XA_TMNOFLAGS).await.unwrap();
    xa_txn2.start(XA_TMNOFLAGS).await.unwrap();

    xa_txn1.end(XA_TMSUCCESS).await.unwrap();
    xa_txn2.end(XA_TMSUCCESS).await.unwrap();

    xa_txn1.commit(true).await.unwrap();
    xa_txn2.rollback().await.unwrap();

    assert_eq!(xa_txn1.state(), XaTransactionState::Committed);
    assert_eq!(xa_txn2.state(), XaTransactionState::RolledBack);

    client.shutdown().await.unwrap();
}

// ============================================================================
// Unit Tests (no cluster required)
// ============================================================================

mod unit_tests {
    use super::*;

    #[test]
    fn test_xid_creation_and_accessors() {
        let xid = Xid::new(42, b"global-txn-id", b"branch-qualifier");

        assert_eq!(xid.format_id(), 42);
        assert_eq!(xid.global_transaction_id(), b"global-txn-id");
        assert_eq!(xid.branch_qualifier(), b"branch-qualifier");
    }

    #[test]
    fn test_xid_serialization_roundtrip() {
        let original = Xid::new(123, b"test-gtrid", b"test-bqual");
        let bytes = original.to_bytes();
        let restored = Xid::from_bytes(&bytes).unwrap();

        assert_eq!(original, restored);
    }

    #[test]
    fn test_xid_generate_uniqueness() {
        let xid1 = Xid::generate();
        let xid2 = Xid::generate();

        assert_ne!(xid1.global_transaction_id(), xid2.global_transaction_id());
    }

    #[test]
    fn test_xa_state_machine_transitions() {
        assert!(XaTransactionState::NotStarted.can_start());
        assert!(!XaTransactionState::Active.can_start());

        assert!(XaTransactionState::Active.can_end());
        assert!(!XaTransactionState::Idle.can_end());

        assert!(XaTransactionState::Idle.can_prepare());
        assert!(!XaTransactionState::Active.can_prepare());

        assert!(XaTransactionState::Prepared.can_commit());
        assert!(XaTransactionState::Idle.can_commit());
        assert!(!XaTransactionState::Active.can_commit());

        assert!(XaTransactionState::Active.can_rollback());
        assert!(XaTransactionState::Idle.can_rollback());
        assert!(XaTransactionState::Prepared.can_rollback());
        assert!(!XaTransactionState::Committed.can_rollback());
    }

    #[test]
    fn test_xa_state_terminal_states() {
        assert!(XaTransactionState::Committed.is_terminal());
        assert!(XaTransactionState::RolledBack.is_terminal());
        assert!(XaTransactionState::HeuristicallyCompleted.is_terminal());

        assert!(!XaTransactionState::NotStarted.is_terminal());
        assert!(!XaTransactionState::Active.is_terminal());
        assert!(!XaTransactionState::Idle.is_terminal());
        assert!(!XaTransactionState::Prepared.is_terminal());
    }

    #[test]
    fn test_xa_flags_are_distinct() {
        let flags = [
            XA_TMNOFLAGS,
            hazelcast_client::XA_TMJOIN,
            hazelcast_client::XA_TMRESUME,
            XA_TMSUCCESS,
            XA_TMFAIL,
            XA_TMSUSPEND,
            hazelcast_client::XA_TMSTARTRSCAN,
            hazelcast_client::XA_TMENDRSCAN,
            hazelcast_client::XA_TMONEPHASE,
        ];

        for (i, &flag_a) in flags.iter().enumerate() {
            for (j, &flag_b) in flags.iter().enumerate() {
                if i != j && flag_a != 0 && flag_b != 0 {
                    assert_ne!(flag_a, flag_b, "Flags at {} and {} should be distinct", i, j);
                }
            }
        }
    }

    #[test]
    fn test_xa_return_codes() {
        assert_eq!(XA_OK, 0);
        assert_eq!(XA_RDONLY, 3);
        assert!(hazelcast_client::XA_RBBASE <= hazelcast_client::XA_RBEND);
    }

    #[test]
    fn test_xid_max_sizes() {
        let max_gtrid = vec![0xAAu8; Xid::MAXGTRIDSIZE];
        let max_bqual = vec![0xBBu8; Xid::MAXBQUALSIZE];

        let xid = Xid::new(0, &max_gtrid, &max_bqual);

        assert_eq!(xid.global_transaction_id().len(), 64);
        assert_eq!(xid.branch_qualifier().len(), 64);
    }

    #[test]
    #[should_panic(expected = "Global transaction ID exceeds maximum size")]
    fn test_xid_gtrid_overflow() {
        let oversized = vec![0u8; Xid::MAXGTRIDSIZE + 1];
        Xid::new(0, &oversized, b"");
    }

    #[test]
    #[should_panic(expected = "Branch qualifier exceeds maximum size")]
    fn test_xid_bqual_overflow() {
        let oversized = vec![0u8; Xid::MAXBQUALSIZE + 1];
        Xid::new(0, b"", &oversized);
    }

    #[test]
    fn test_xid_equality_and_hash() {
        use std::collections::HashSet;

        let xid1 = Xid::new(1, b"gtrid", b"bqual");
        let xid2 = Xid::new(1, b"gtrid", b"bqual");
        let xid3 = Xid::new(2, b"gtrid", b"bqual");

        assert_eq!(xid1, xid2);
        assert_ne!(xid1, xid3);

        let mut set = HashSet::new();
        set.insert(xid1.clone());
        assert!(set.contains(&xid2));
        assert!(!set.contains(&xid3));
    }

    #[test]
    fn test_xid_default() {
        let xid = Xid::default();
        assert_eq!(xid.format_id(), 0);
        assert!(!xid.global_transaction_id().is_empty());
    }

    #[test]
    fn test_xa_transaction_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<hazelcast_client::XATransaction>();
        assert_send_sync::<Xid>();
    }
}
