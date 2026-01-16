//! Distributed cardinality estimator proxy implementation.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, HazelcastError, Result, Serializable};

use crate::connection::ConnectionManager;

/// A distributed cardinality estimator proxy backed by HyperLogLog.
///
/// `CardinalityEstimator` provides probabilistic cardinality estimation with
/// configurable precision. It uses the HyperLogLog algorithm to estimate the
/// number of distinct elements added to the estimator.
///
/// This data structure is ideal for:
/// - Counting unique visitors, users, or events
/// - Approximating set cardinality without storing all elements
/// - Memory-efficient distinct counting across distributed systems
///
/// The estimator provides a typical error rate of ~1.04/sqrt(m) where m is
/// the number of registers (determined by cluster configuration).
#[derive(Debug)]
pub struct CardinalityEstimator<T> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> CardinalityEstimator<T>
where
    T: Serializable + Send + Sync,
{
    /// Creates a new cardinality estimator proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns the name of this cardinality estimator.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Adds a value to the cardinality estimator.
    ///
    /// The value is serialized and its hash is used to update the HyperLogLog
    /// registers. Adding the same value multiple times does not increase the
    /// estimated cardinality.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to add to the estimator
    ///
    /// # Example
    ///
    /// ```ignore
    /// let estimator = client.get_cardinality_estimator::<String>("visitors");
    /// estimator.add(&"user-123".to_string()).await?;
    /// estimator.add(&"user-456".to_string()).await?;
    /// estimator.add(&"user-123".to_string()).await?; // duplicate, won't increase count
    ///
    /// let count = estimator.estimate().await?;
    /// // count will be approximately 2
    /// ```
    pub async fn add(&self, value: &T) -> Result<()> {
        let serialized = value.serialize()?;

        let mut message = ClientMessage::create_for_encode_any_partition(CARDINALITY_ESTIMATOR_ADD);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&serialized));

        self.invoke(message).await?;
        Ok(())
    }

    /// Estimates the cardinality (number of distinct elements) in the estimator.
    ///
    /// The returned value is an approximation based on the HyperLogLog algorithm.
    /// The accuracy depends on the number of registers configured for the estimator.
    ///
    /// # Returns
    ///
    /// The estimated number of distinct elements added to the estimator.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let estimator = client.get_cardinality_estimator::<String>("visitors");
    ///
    /// // Add some values
    /// for i in 0..1000 {
    ///     estimator.add(&format!("user-{}", i)).await?;
    /// }
    ///
    /// let estimate = estimator.estimate().await?;
    /// // estimate will be approximately 1000 (with some error margin)
    /// ```
    pub async fn estimate(&self) -> Result<i64> {
        let mut message = ClientMessage::create_for_encode_any_partition(CARDINALITY_ESTIMATOR_ESTIMATE);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_long_response(&response)
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }

    fn data_frame(data: &[u8]) -> Frame {
        Frame::with_content(BytesMut::from(data))
    }

    async fn invoke(&self, message: ClientMessage) -> Result<ClientMessage> {
        let address = self.get_connection_address().await?;

        self.connection_manager.send_to(address, message).await?;
        self.connection_manager
            .receive_from(address)
            .await?
            .ok_or_else(|| HazelcastError::Connection("connection closed unexpectedly".to_string()))
    }

    async fn get_connection_address(&self) -> Result<SocketAddr> {
        let addresses = self.connection_manager.connected_addresses().await;
        addresses.into_iter().next().ok_or_else(|| {
            HazelcastError::Connection("no connections available".to_string())
        })
    }

    fn decode_long_response(response: &ClientMessage) -> Result<i64> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization(
                "empty response".to_string(),
            ));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 8 {
            let offset = RESPONSE_HEADER_SIZE;
            Ok(i64::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
                initial_frame.content[offset + 4],
                initial_frame.content[offset + 5],
                initial_frame.content[offset + 6],
                initial_frame.content[offset + 7],
            ]))
        } else {
            Ok(0)
        }
    }
}

impl<T> Clone for CardinalityEstimator<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cardinality_estimator_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CardinalityEstimator<String>>();
    }

    #[test]
    fn test_cardinality_estimator_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<CardinalityEstimator<String>>();
    }

    #[test]
    fn test_string_frame() {
        let frame = CardinalityEstimator::<String>::string_frame("test-estimator");
        assert_eq!(&frame.content[..], b"test-estimator");
    }

    #[test]
    fn test_data_frame() {
        let data = b"test-data";
        let frame = CardinalityEstimator::<String>::data_frame(data);
        assert_eq!(&frame.content[..], b"test-data");
    }

    #[test]
    fn test_data_frame_empty() {
        let data: &[u8] = &[];
        let frame = CardinalityEstimator::<String>::data_frame(data);
        assert!(frame.content.is_empty());
    }
}
