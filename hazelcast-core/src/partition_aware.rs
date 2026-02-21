//! Partition-aware key routing.
//!
//! The [`PartitionAware`] trait allows keys to declare a custom partition key
//! that differs from the key itself. This enables co-location of related entries
//! on the same partition for efficient local processing.
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_core::PartitionAware;
//!
//! struct OrderKey {
//!     order_id: String,
//!     customer_id: String,
//! }
//!
//! impl PartitionAware for OrderKey {
//!     fn partition_key_bytes(&self) -> Vec<u8> {
//!         // Route all orders for the same customer to the same partition
//!         self.customer_id.as_bytes().to_vec()
//!     }
//! }
//! ```

/// Trait for keys that should be routed to a specific partition based on a
/// partition key different from the key itself.
///
/// When a type implementing this trait is used as a map key, the partition hash
/// is computed from [`partition_key_bytes`](PartitionAware::partition_key_bytes)
/// instead of from the serialized key. This allows related entries to be
/// co-located on the same cluster member.
pub trait PartitionAware: Send + Sync {
    /// Returns the serialized bytes of the partition key.
    ///
    /// The partition hash will be computed from these bytes instead of from the
    /// key itself. All keys returning the same partition key bytes will be
    /// stored on the same partition.
    fn partition_key_bytes(&self) -> Vec<u8>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CustomerOrderKey {
        order_id: u64,
        customer_id: String,
    }

    impl PartitionAware for CustomerOrderKey {
        fn partition_key_bytes(&self) -> Vec<u8> {
            self.customer_id.as_bytes().to_vec()
        }
    }

    #[test]
    fn test_partition_aware_same_customer_same_key() {
        let key1 = CustomerOrderKey {
            order_id: 1,
            customer_id: "cust-42".into(),
        };
        let key2 = CustomerOrderKey {
            order_id: 2,
            customer_id: "cust-42".into(),
        };
        assert_eq!(key1.partition_key_bytes(), key2.partition_key_bytes());
    }

    #[test]
    fn test_partition_aware_different_customer_different_key() {
        let key1 = CustomerOrderKey {
            order_id: 1,
            customer_id: "cust-42".into(),
        };
        let key2 = CustomerOrderKey {
            order_id: 1,
            customer_id: "cust-99".into(),
        };
        assert_ne!(key1.partition_key_bytes(), key2.partition_key_bytes());
    }
}
