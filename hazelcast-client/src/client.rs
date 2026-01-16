//! Hazelcast client entry point.

use std::sync::Arc;

use hazelcast_core::{Deserializable, Result, Serializable};
use uuid::Uuid;

use crate::cluster::{ClusterService, LifecycleService, PartitionService};
use crate::config::ClientConfig;
use crate::connection::ConnectionManager;
use crate::diagnostics::{ClientStatistics, StatisticsCollector};
use crate::executor::ExecutorService;
use crate::jet::JetService;
use crate::listener::{LifecycleEvent, Member, MemberEvent};
use crate::proxy::{
    AtomicLong, CountDownLatch, FencedLock, FlakeIdGenerator, IList, IMap, IQueue, ISet, ITopic,
    MultiMap, PNCounter, ReliableTopic, ReplicatedMap, Ringbuffer, Semaphore,
};
use crate::sql::SqlService;
use crate::transaction::{TransactionContext, TransactionOptions, XATransaction, Xid};

/// The main entry point for connecting to a Hazelcast cluster.
///
/// `HazelcastClient` manages connections to cluster members and provides
/// access to distributed data structures.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::{ClientConfig, HazelcastClient};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = ClientConfig::builder()
///         .cluster_name("dev")
///         .build()?;
///
///     let client = HazelcastClient::new(config).await?;
///     let map = client.get_map::<String, String>("my-map");
///
///     map.put("key".to_string(), "value".to_string()).await?;
///     let value = map.get(&"key".to_string()).await?;
///
///     client.shutdown().await?;
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct HazelcastClient {
    config: Arc<ClientConfig>,
    connection_manager: Arc<ConnectionManager>,
    statistics_collector: Arc<StatisticsCollector>,
    client_uuid: Uuid,
}

impl HazelcastClient {
    /// Creates a new client and connects to the Hazelcast cluster.
    ///
    /// This method establishes connections to cluster members based on the
    /// provided configuration. It will fail if no connections can be established.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No cluster addresses are configured
    /// - All connection attempts fail
    /// - Network errors occur during connection
    pub async fn new(config: ClientConfig) -> Result<Self> {
        let client_uuid = Uuid::new_v4();
        let connection_manager = ConnectionManager::from_config(config.clone());
        connection_manager.start().await?;

        tracing::info!(
            cluster = %config.cluster_name(),
            client_uuid = %client_uuid,
            "connected to Hazelcast cluster"
        );

        Ok(Self {
            config: Arc::new(config),
            connection_manager: Arc::new(connection_manager),
            statistics_collector: Arc::new(StatisticsCollector::new()),
            client_uuid,
        })
    }

    /// Returns a distributed map proxy for the given name.
    ///
    /// The map proxy allows performing key-value operations on the cluster.
    /// The actual map is created on the cluster lazily when first accessed.
    ///
    /// # Type Parameters
    ///
    /// - `K`: The key type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    /// - `V`: The value type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    pub fn get_map<K, V>(&self, name: &str) -> IMap<K, V>
    where
        K: Serializable + Deserializable + Send + Sync,
        V: Serializable + Deserializable + Send + Sync,
    {
        IMap::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed queue proxy for the given name.
    ///
    /// The queue proxy allows performing FIFO operations on the cluster.
    /// The actual queue is created on the cluster lazily when first accessed.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The element type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    pub fn get_queue<T>(&self, name: &str) -> IQueue<T>
    where
        T: Serializable + Deserializable + Send + Sync,
    {
        IQueue::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed set proxy for the given name.
    ///
    /// The set proxy allows performing set operations on the cluster.
    /// The actual set is created on the cluster lazily when first accessed.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The element type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    pub fn get_set<T>(&self, name: &str) -> ISet<T>
    where
        T: Serializable + Deserializable + Send + Sync,
    {
        ISet::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed list proxy for the given name.
    ///
    /// The list proxy allows performing indexed list operations on the cluster.
    /// The actual list is created on the cluster lazily when first accessed.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The element type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    pub fn get_list<T>(&self, name: &str) -> IList<T>
    where
        T: Serializable + Deserializable + Send + Sync,
    {
        IList::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed multi-map proxy for the given name.
    ///
    /// The multi-map proxy allows storing multiple values per key on the cluster.
    /// The actual multi-map is created on the cluster lazily when first accessed.
    ///
    /// # Type Parameters
    ///
    /// - `K`: The key type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    /// - `V`: The value type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    pub fn get_multimap<K, V>(&self, name: &str) -> MultiMap<K, V>
    where
        K: Serializable + Deserializable + Send + Sync,
        V: Serializable + Deserializable + Send + Sync,
    {
        MultiMap::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed topic proxy for the given name.
    ///
    /// The topic proxy allows pub/sub messaging on the cluster.
    /// The actual topic is created on the cluster lazily when first accessed.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The message type, must implement `Serializable`, `Deserializable`, `Send`, `Sync`, and `'static`
    pub fn get_topic<T>(&self, name: &str) -> ITopic<T>
    where
        T: Serializable + Deserializable + Send + Sync + 'static,
    {
        ITopic::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed atomic long proxy for the given name.
    ///
    /// The atomic long proxy allows performing atomic counter operations on the CP subsystem.
    /// The actual atomic long is created on the cluster lazily when first accessed.
    ///
    /// Note: AtomicLong requires the CP subsystem to be enabled on the Hazelcast cluster.
    pub fn get_atomic_long(&self, name: &str) -> AtomicLong {
        AtomicLong::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed replicated map proxy for the given name.
    ///
    /// The replicated map proxy provides an eventually-consistent, fully replicated
    /// map where data is stored on all cluster members for faster reads.
    /// The actual replicated map is created on the cluster lazily when first accessed.
    ///
    /// # Type Parameters
    ///
    /// - `K`: The key type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    /// - `V`: The value type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    pub fn get_replicated_map<K, V>(&self, name: &str) -> ReplicatedMap<K, V>
    where
        K: Serializable + Deserializable + Send + Sync,
        V: Serializable + Deserializable + Send + Sync,
    {
        ReplicatedMap::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed ringbuffer proxy for the given name.
    ///
    /// The ringbuffer proxy provides a bounded circular buffer with sequence-based
    /// access. Items can be read by sequence number, allowing reliable event streaming.
    /// The actual ringbuffer is created on the cluster lazily when first accessed.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The element type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    pub fn get_ringbuffer<T>(&self, name: &str) -> Ringbuffer<T>
    where
        T: Serializable + Deserializable + Send + Sync,
    {
        Ringbuffer::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a reliable topic proxy for the given name.
    ///
    /// `ReliableTopic` is backed by a Ringbuffer and provides:
    /// - Reliable delivery with sequence tracking
    /// - Gap detection when messages are lost due to ringbuffer overflow
    /// - Ability to replay messages from history
    ///
    /// # Type Parameters
    ///
    /// - `T`: The message type, must implement serialization traits and `'static`
    ///
    /// # Example
    ///
    /// ```ignore
    /// let topic = client.get_reliable_topic::<String>("my-reliable-topic");
    ///
    /// // Publish messages
    /// topic.publish("Hello".to_string()).await?;
    ///
    /// // Subscribe with custom config
    /// let config = ReliableTopicConfig::default().start_from_oldest(true);
    /// let registration = topic.add_message_listener_with_config(config, |msg| {
    ///     println!("Received: {} at seq {}", msg.message, msg.sequence);
    /// }).await?;
    /// ```
    pub fn get_reliable_topic<T>(&self, name: &str) -> ReliableTopic<T>
    where
        T: Serializable + Deserializable + Send + Sync + 'static,
    {
        ReliableTopic::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed PN Counter proxy for the given name.
    ///
    /// The PN Counter (Positive-Negative Counter) is a CRDT that supports both
    /// increment and decrement operations with eventual consistency guarantees.
    /// Unlike `AtomicLong`, it does not require the CP subsystem.
    ///
    /// PN Counters are ideal for use cases where:
    /// - High availability is more important than strong consistency
    /// - Concurrent increments and decrements from multiple cluster members are common
    /// - Conflict-free merge semantics are acceptable
    pub fn get_pn_counter(&self, name: &str) -> PNCounter {
        PNCounter::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed FlakeIdGenerator proxy for the given name.
    ///
    /// The FlakeIdGenerator produces cluster-wide unique 64-bit IDs that are
    /// roughly ordered by time. IDs are composed of a timestamp, node ID, and
    /// sequence number (similar to Twitter's Snowflake algorithm).
    ///
    /// FlakeIdGenerators are ideal for:
    /// - Generating unique primary keys for distributed databases
    /// - Creating roughly time-ordered identifiers without coordination
    /// - High-throughput ID generation scenarios
    pub fn get_flake_id_generator(&self, name: &str) -> FlakeIdGenerator {
        FlakeIdGenerator::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed FencedLock proxy for the given name.
    ///
    /// The FencedLock provides a distributed mutual exclusion primitive with fencing
    /// tokens. Each successful lock acquisition returns a monotonically increasing
    /// fence token that can be used to detect stale lock holders.
    ///
    /// Note: FencedLock requires the CP subsystem to be enabled on the Hazelcast cluster.
    ///
    /// FencedLocks are ideal for:
    /// - Distributed coordination requiring strong consistency
    /// - Protecting critical sections across multiple processes
    /// - Scenarios where detecting stale lock holders is important
    pub fn get_fenced_lock(&self, name: &str) -> FencedLock {
        FencedLock::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed Semaphore proxy for the given name.
    ///
    /// The Semaphore provides a distributed counting semaphore backed by the CP
    /// subsystem. It controls access to a shared resource through permits.
    ///
    /// Note: Semaphore requires the CP subsystem to be enabled on the Hazelcast cluster.
    ///
    /// Semaphores are ideal for:
    /// - Limiting concurrent access to a resource across the cluster
    /// - Implementing distributed rate limiting
    /// - Coordinating access to bounded resources
    pub fn get_semaphore(&self, name: &str) -> Semaphore {
        Semaphore::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed CountDownLatch proxy for the given name.
    ///
    /// The CountDownLatch provides a distributed synchronization primitive that
    /// allows one or more tasks to wait until a set of operations completes.
    ///
    /// Note: CountDownLatch requires the CP subsystem to be enabled on the Hazelcast cluster.
    ///
    /// CountDownLatches are ideal for:
    /// - Waiting for multiple distributed tasks to complete
    /// - Implementing barrier synchronization across processes
    /// - Coordinating startup sequences in distributed systems
    pub fn get_countdown_latch(&self, name: &str) -> CountDownLatch {
        CountDownLatch::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed executor service proxy for the given name.
    ///
    /// The executor service allows submitting callable tasks for execution
    /// on remote cluster members.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let executor = client.get_executor_service("my-executor");
    /// let future = executor.submit(&my_task).await?;
    /// let result = future.get().await?;
    /// ```
    pub fn get_executor_service(&self, name: &str) -> ExecutorService {
        ExecutorService::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns the SQL service for executing SQL queries.
    ///
    /// The SQL service allows executing SQL queries against data stored in
    /// Hazelcast maps and other data structures.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let sql = client.sql();
    /// let statement = SqlStatement::new("SELECT * FROM users WHERE age > ?")
    ///     .add_parameter(SqlValue::Integer(21));
    ///
    /// let mut result = sql.execute(statement).await?;
    /// while let Some(row) = result.next_row().await? {
    ///     println!("Row: {:?}", row);
    /// }
    /// result.close().await?;
    /// ```
    pub fn sql(&self) -> SqlService {
        SqlService::new(Arc::clone(&self.connection_manager))
    }

    /// Returns the Jet service for submitting and managing streaming jobs.
    ///
    /// The Jet service allows submitting streaming pipelines for execution
    /// on the Hazelcast cluster, monitoring job status, and managing job lifecycle.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let jet = client.jet();
    /// let pipeline = Pipeline::builder()
    ///     .read_from("source-map")
    ///     .map("transform")
    ///     .write_to("sink-map")
    ///     .build();
    ///
    /// let job = jet.submit_job(&pipeline, None).await?;
    /// println!("Job {} submitted", job.id());
    ///
    /// let status = job.get_status().await?;
    /// println!("Job status: {}", status);
    ///
    /// job.cancel().await?;
    /// ```
    pub fn jet(&self) -> JetService {
        JetService::new(Arc::clone(&self.connection_manager))
    }

    /// Creates a new transaction context with the specified options.
    ///
    /// The transaction context allows performing atomic operations across
    /// multiple data structures.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let options = TransactionOptions::new()
    ///     .with_timeout(Duration::from_secs(30))
    ///     .with_type(TransactionType::TwoPhase);
    ///
    /// let mut txn = client.new_transaction_context(options);
    /// txn.begin().await?;
    ///
    /// let map = txn.get_map::<String, String>("my-map");
    /// map.put("key".to_string(), "value".to_string()).await?;
    ///
    /// txn.commit().await?;
    /// ```
    pub fn new_transaction_context(&self, options: TransactionOptions) -> TransactionContext {
        TransactionContext::new(Arc::clone(&self.connection_manager), options)
    }

    /// Creates a new XA transaction with the specified transaction identifier.
    ///
    /// XA transactions support distributed two-phase commit across multiple
    /// resource managers, enabling ACID guarantees across heterogeneous systems.
    ///
    /// # Arguments
    ///
    /// * `xid` - The XA transaction identifier
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::{HazelcastClient, Xid, XAResource, XA_TMNOFLAGS, XA_TMSUCCESS, XA_OK};
    ///
    /// let xid = Xid::new(0, b"global-txn-123", b"branch-001");
    /// let mut xa_txn = client.new_xa_transaction(xid);
    ///
    /// xa_txn.start(XA_TMNOFLAGS).await?;
    /// // ... perform operations ...
    /// xa_txn.end(XA_TMSUCCESS).await?;
    ///
    /// let vote = xa_txn.prepare().await?;
    /// if vote == XA_OK {
    ///     xa_txn.commit(false).await?;
    /// } else {
    ///     xa_txn.rollback().await?;
    /// }
    /// ```
    pub fn new_xa_transaction(&self, xid: Xid) -> XATransaction {
        XATransaction::new(Arc::clone(&self.connection_manager), xid)
    }

    /// Creates a new XA transaction with an auto-generated transaction identifier.
    ///
    /// This is a convenience method that generates a unique `Xid` using a random UUID.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut xa_txn = client.new_xa_transaction_auto();
    /// xa_txn.start(XA_TMNOFLAGS).await?;
    /// // ... operations ...
    /// ```
    pub fn new_xa_transaction_auto(&self) -> XATransaction {
        XATransaction::with_generated_xid(Arc::clone(&self.connection_manager))
    }

    /// Returns the cluster name this client is connected to.
    pub fn cluster_name(&self) -> &str {
        self.config.cluster_name()
    }

    /// Returns the client configuration.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Returns the number of active connections to cluster members.
    pub async fn connection_count(&self) -> usize {
        self.connection_manager.connection_count().await
    }

    /// Returns the current list of known cluster members.
    ///
    /// This list is updated as members join or leave the cluster.
    pub async fn members(&self) -> Vec<Member> {
        self.connection_manager.members().await
    }

    /// Returns the number of known cluster members.
    pub async fn member_count(&self) -> usize {
        self.connection_manager.member_count().await
    }

    /// Returns the cluster service for querying and monitoring cluster membership.
    ///
    /// The cluster service provides access to:
    /// - Current cluster members and their attributes
    /// - Local client information
    /// - Cluster time
    /// - Membership event notifications
    ///
    /// # Example
    ///
    /// ```ignore
    /// let cluster = client.cluster();
    ///
    /// // Get all cluster members
    /// let members = cluster.get_members().await;
    /// for member in members {
    ///     println!("Member: {} at {}", member.uuid(), member.address());
    /// }
    ///
    /// // Get local client info
    /// let client_info = cluster.get_local_client();
    /// println!("Client UUID: {}", client_info.uuid());
    ///
    /// // Subscribe to membership changes
    /// let mut listener = cluster.add_membership_listener();
    /// tokio::spawn(async move {
    ///     while let Ok(event) = listener.recv().await {
    ///         println!("Membership event: {}", event);
    ///     }
    /// });
    /// ```
    pub fn cluster(&self) -> ClusterService {
        ClusterService::new(
            Arc::clone(&self.connection_manager),
            self.client_uuid,
            self.config.cluster_name().to_string(),
            Vec::new(),
        )
    }

    /// Returns the lifecycle service for managing client lifecycle.
    ///
    /// The lifecycle service provides methods to:
    /// - Check if the client is running
    /// - Gracefully shut down or forcefully terminate the client
    /// - Register and unregister lifecycle event listeners
    ///
    /// # Example
    ///
    /// ```ignore
    /// let lifecycle = client.lifecycle();
    ///
    /// // Check if client is running
    /// if lifecycle.is_running().await {
    ///     println!("Client is active");
    /// }
    ///
    /// // Add a lifecycle listener
    /// let mut registration = lifecycle.add_lifecycle_listener();
    /// tokio::spawn(async move {
    ///     while let Ok(event) = registration.recv().await {
    ///         println!("Lifecycle event: {}", event);
    ///         if event == LifecycleEvent::Shutdown {
    ///             break;
    ///         }
    ///     }
    /// });
    ///
    /// // Graceful shutdown
    /// lifecycle.shutdown().await?;
    /// ```
    pub fn lifecycle(&self) -> LifecycleService {
        LifecycleService::new(Arc::clone(&self.connection_manager))
    }

    /// Returns the partition service for querying partition information.
    ///
    /// The partition service provides access to:
    /// - Partition count and partition list
    /// - Partition owners (which member owns each partition)
    /// - Key-to-partition mapping
    /// - Migration event notifications
    ///
    /// # Example
    ///
    /// ```ignore
    /// let partition_service = client.partition_service();
    ///
    /// // Get partition count
    /// let count = partition_service.get_partition_count();
    /// println!("Cluster has {} partitions", count);
    ///
    /// // Find which partition owns a key
    /// let partition = partition_service.get_partition(&"my-key".to_string()).await;
    /// println!("Key belongs to partition {}", partition.id());
    ///
    /// // Get the owner of a partition
    /// if let Some(owner) = partition_service.get_partition_owner(0).await {
    ///     println!("Partition 0 owned by {}", owner);
    /// }
    /// ```
    pub fn partition_service(&self) -> PartitionService {
        PartitionService::new(Arc::clone(&self.connection_manager))
    }

    /// Subscribes to cluster membership events.
    ///
    /// The returned receiver will emit events when members join or leave the cluster.
    pub fn subscribe_membership(&self) -> tokio::sync::broadcast::Receiver<MemberEvent> {
        self.connection_manager.subscribe_membership()
    }

    /// Subscribes to client lifecycle events.
    ///
    /// The returned receiver will emit events during client state transitions
    /// such as starting, connected, disconnected, and shutdown.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut lifecycle = client.subscribe_lifecycle();
    ///
    /// tokio::spawn(async move {
    ///     while let Ok(event) = lifecycle.recv().await {
    ///         println!("Lifecycle event: {}", event);
    ///     }
    /// });
    /// ```
    pub fn subscribe_lifecycle(&self) -> tokio::sync::broadcast::Receiver<LifecycleEvent> {
        self.connection_manager.subscribe_lifecycle()
    }

    /// Returns a snapshot of current client statistics.
    ///
    /// Statistics include connection counts, operation metrics, near cache
    /// hit/miss ratios, and memory usage estimates.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stats = client.statistics().await;
    /// println!("Active connections: {}", stats.connection_stats().active_connections());
    /// println!("Total operations: {}", stats.total_operations());
    /// println!("Near cache hit ratio: {:.2}%", stats.aggregate_near_cache_hit_ratio() * 100.0);
    /// ```
    pub async fn statistics(&self) -> ClientStatistics {
        let connection_count = self.connection_count().await as u64;
        self.statistics_collector.collect(connection_count).await
    }

    /// Returns the statistics collector for recording metrics.
    ///
    /// This is primarily used internally by proxies to record operation counts.
    pub fn statistics_collector(&self) -> &Arc<StatisticsCollector> {
        &self.statistics_collector
    }

    /// Shuts down the client and closes all connections.
    ///
    /// After shutdown, the client cannot be used for any operations.
    pub async fn shutdown(&self) -> Result<()> {
        tracing::info!(
            cluster = %self.config.cluster_name(),
            "shutting down Hazelcast client"
        );
        self.connection_manager.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HazelcastClient>();
    }
}
