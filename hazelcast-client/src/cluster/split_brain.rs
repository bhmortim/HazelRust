//! Split-brain protection service for ensuring quorum-based data consistency.

use std::sync::Arc;

use crate::connection::ConnectionManager;

/// Service for checking split-brain protection (quorum) status.
///
/// Split-brain protection ensures that operations on distributed data structures
/// only proceed when a minimum number of cluster members are available. This
/// prevents data inconsistency during network partitions.
///
/// The service checks quorum status based on the configured `QuorumConfig` for
/// each data structure name pattern.
///
/// # Example
///
/// ```ignore
/// let sbp = client.split_brain_protection();
///
/// // Check if quorum is present for a specific data structure
/// if sbp.is_quorum_present("my-map").await {
///     println!("Quorum is present, operations are safe");
/// } else {
///     println!("Quorum is NOT present, operations may fail");
/// }
///
/// // Get all configured quorum names
/// let names = sbp.get_quorum_names();
/// for name in &names {
///     println!("Quorum config: {}", name);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct SplitBrainProtectionService {
    connection_manager: Arc<ConnectionManager>,
}

impl SplitBrainProtectionService {
    /// Creates a new split-brain protection service.
    pub(crate) fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }

    /// Returns `true` if the quorum (split-brain protection) is present for
    /// the data structure with the given name.
    ///
    /// This checks whether the number of connected cluster members meets the
    /// minimum cluster size requirement defined in the quorum configuration
    /// for the given name pattern.
    ///
    /// Returns `true` if no quorum configuration is found for the name
    /// (i.e., the data structure has no quorum requirement).
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the data structure to check quorum for
    pub async fn is_quorum_present(&self, name: &str) -> bool {
        if let Some(quorum_config) = self.connection_manager.find_quorum_config(name) {
            let members = self.connection_manager.members().await;
            quorum_config.check_quorum(&members)
        } else {
            // No quorum config means no quorum requirement
            true
        }
    }

    /// Returns the names/patterns of all configured quorum (split-brain protection) rules.
    ///
    /// Each name corresponds to a `QuorumConfig` that defines the minimum
    /// cluster size and which operations are protected.
    pub fn get_quorum_names(&self) -> Vec<String> {
        self.connection_manager
            .quorum_configs()
            .iter()
            .map(|qc| qc.name().to_string())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_brain_protection_service_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SplitBrainProtectionService>();
    }

    #[test]
    fn test_split_brain_protection_service_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<SplitBrainProtectionService>();
    }
}
