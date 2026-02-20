//! Hazelcast client entry point.

use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::{Deserializable, Result, Serializable};
use uuid::Uuid;

use crate::cluster::{
    CPSessionManagementService, CPSubsystemManagementService, ClusterService, LifecycleService,
    PartitionService,
};
use crate::config::ClientConfig;
use crate::connection::ConnectionManager;
use crate::diagnostics::{ClientStatistics, StatisticsCollector};
use crate::executor::ExecutorService;
use crate::jet::JetService;
use crate::listener::{
    BoxedDistributedObjectListener, ClientStateListener, DistributedObjectEvent,
    DistributedObjectListener, LifecycleEvent, ListenerId, ListenerRegistration, Member,
    MemberEvent, MigrationEvent, MigrationListener, MigrationState, PartitionLostEvent,
    PartitionLostListener,
};
use crate::proxy::{
    AtomicLong, AtomicReference, CPMap, CardinalityEstimator, CountDownLatch, FencedLock,
    FlakeIdGenerator, ICache, IList, IMap, IQueue, ISet, ITopic, MultiMap, PNCounter,
    ReliableTopic, ReplicatedMap, Ringbuffer, Semaphore,
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
use std::sync::RwLock;

/// Information about a distributed object in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DistributedObjectInfo {
    /// The service name that manages this distributed object (e.g., "hz:impl:mapService").
    pub service_name: String,
    /// The name of the distributed object.
    pub name: String,
}

impl DistributedObjectInfo {
    /// Creates a new DistributedObjectInfo.
    pub fn new(service_name: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            name: name.into(),
        }
    }

    /// Returns the service name.
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    /// Returns the object name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Internal state for managing distributed object listeners.
struct DistributedObjectListenerState {
    listeners: std::collections::HashMap<ListenerId, (Arc<dyn DistributedObjectListener>, ListenerRegistration)>,
}

impl std::fmt::Debug for DistributedObjectListenerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedObjectListenerState")
            .field("listener_count", &self.listeners.len())
            .finish()
    }
}

/// Internal state for managing client state listeners.
struct ClientStateListenerState {
    listeners: std::collections::HashMap<ListenerId, (Arc<dyn ClientStateListener>, ListenerRegistration)>,
}

impl std::fmt::Debug for ClientStateListenerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientStateListenerState")
            .field("listener_count", &self.listeners.len())
            .finish()
    }
}

/// Internal state for managing partition lost listeners.
struct PartitionLostListenerState {
    listeners: std::collections::HashMap<ListenerId, (Arc<dyn PartitionLostListener>, ListenerRegistration)>,
}

impl std::fmt::Debug for PartitionLostListenerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionLostListenerState")
            .field("listener_count", &self.listeners.len())
            .finish()
    }
}

/// Internal state for managing migration listeners.
struct MigrationListenerState {
    listeners: std::collections::HashMap<ListenerId, (Arc<dyn MigrationListener>, ListenerRegistration)>,
}

impl std::fmt::Debug for MigrationListenerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationListenerState")
            .field("listener_count", &self.listeners.len())
            .finish()
    }
}

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
    distributed_object_listeners: RwLock<DistributedObjectListenerState>,
    client_state_listeners: RwLock<ClientStateListenerState>,
    partition_lost_listeners: RwLock<PartitionLostListenerState>,
    migration_listeners: RwLock<MigrationListenerState>,
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
            distributed_object_listeners: RwLock::new(DistributedObjectListenerState {
                listeners: std::collections::HashMap::new(),
            }),
            client_state_listeners: RwLock::new(ClientStateListenerState {
                listeners: std::collections::HashMap::new(),
            }),
            partition_lost_listeners: RwLock::new(PartitionLostListenerState {
                listeners: std::collections::HashMap::new(),
            }),
            migration_listeners: RwLock::new(MigrationListenerState {
                listeners: std::collections::HashMap::new(),
            }),
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

    /// Returns a distributed atomic reference proxy for the given name.
    ///
    /// The atomic reference proxy allows performing atomic operations on object references
    /// through the CP (Consistent Partition) subsystem.
    ///
    /// Note: AtomicReference requires the CP subsystem to be enabled on the Hazelcast cluster.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The value type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    pub fn get_atomic_reference<T>(&self, name: &str) -> AtomicReference<T>
    where
        T: Serializable + Deserializable + Send + Sync,
    {
        AtomicReference::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed CP Map proxy for the given name.
    ///
    /// The CP Map provides a key-value store with strong consistency guarantees
    /// through the CP (Consistent Partition) subsystem using the Raft consensus algorithm.
    ///
    /// Unlike the regular `IMap`, `CPMap` guarantees linearizable operations,
    /// making it suitable for scenarios requiring strong consistency such as
    /// configuration management or leader election metadata.
    ///
    /// Note: CPMap requires the CP subsystem to be enabled on the Hazelcast cluster.
    ///
    /// # Type Parameters
    ///
    /// - `K`: The key type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    /// - `V`: The value type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    ///
    /// # Example
    ///
    /// ```ignore
    /// let cp_map = client.get_cp_map::<String, String>("my-cp-map");
    ///
    /// // Strong consistency guarantees for all operations
    /// cp_map.put("config-key".to_string(), "config-value".to_string()).await?;
    /// let value = cp_map.get(&"config-key".to_string()).await?;
    ///
    /// // Atomic compare-and-set operation
    /// let success = cp_map.compare_and_set(
    ///     &"config-key".to_string(),
    ///     &"config-value".to_string(),
    ///     "new-value".to_string()
    /// ).await?;
    /// ```
    pub fn get_cp_map<K, V>(&self, name: &str) -> CPMap<K, V>
    where
        K: Serializable + Deserializable + Send + Sync,
        V: Serializable + Deserializable + Send + Sync,
    {
        CPMap::new(name.to_string(), Arc::clone(&self.connection_manager))
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

    /// Returns a distributed cache proxy for the given name.
    ///
    /// The cache proxy implements a subset of the JCache (JSR-107) API, providing
    /// standard caching operations with strong consistency guarantees.
    ///
    /// # Type Parameters
    ///
    /// - `K`: The key type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    /// - `V`: The value type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    ///
    /// # Example
    ///
    /// ```ignore
    /// let cache = client.get_cache::<String, User>("user-cache");
    ///
    /// // Basic operations
    /// cache.put("user:1".to_string(), user).await?;
    /// let user = cache.get(&"user:1".to_string()).await?;
    ///
    /// // Atomic operations
    /// let old = cache.get_and_put("user:1".to_string(), new_user).await?;
    /// let removed = cache.get_and_remove(&"user:1".to_string()).await?;
    ///
    /// // Conditional operations
    /// cache.put_if_absent("user:2".to_string(), user2).await?;
    /// cache.replace_if_equals(&"user:1".to_string(), &old_user, new_user).await?;
    /// ```
    pub fn get_cache<K, V>(&self, name: &str) -> ICache<K, V>
    where
        K: Serializable + Deserializable + Send + Sync,
        V: Serializable + Deserializable + Send + Sync,
    {
        ICache::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed CardinalityEstimator proxy for the given name.
    ///
    /// The CardinalityEstimator uses the HyperLogLog algorithm to provide
    /// probabilistic cardinality estimation. It can estimate the number of
    /// distinct elements in a set without storing all elements.
    ///
    /// CardinalityEstimators are ideal for:
    /// - Counting unique visitors, users, or events
    /// - Approximating distinct counts in streaming data
    /// - Memory-efficient cardinality estimation across clusters
    ///
    /// # Type Parameters
    ///
    /// - `T`: The element type, must implement `Serializable`, `Send`, and `Sync`
    ///
    /// # Example
    ///
    /// ```ignore
    /// let estimator = client.get_cardinality_estimator::<String>("unique-visitors");
    ///
    /// // Add values
    /// estimator.add(&"user-123".to_string()).await?;
    /// estimator.add(&"user-456".to_string()).await?;
    ///
    /// // Get estimated cardinality
    /// let count = estimator.estimate().await?;
    /// println!("Estimated unique visitors: {}", count);
    /// ```
    pub fn get_cardinality_estimator<T>(&self, name: &str) -> CardinalityEstimator<T>
    where
        T: Serializable + Send + Sync,
    {
        CardinalityEstimator::new(name.to_string(), Arc::clone(&self.connection_manager))
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

    /// Returns the CP Subsystem management service.
    ///
    /// The CP Subsystem management service provides operations to:
    /// - Query CP groups and their identifiers
    /// - Get detailed information about specific CP groups
    /// - Force destroy CP groups (use with caution)
    /// - Query and manage CP members
    ///
    /// Note: These operations require the CP Subsystem to be enabled on the
    /// Hazelcast cluster.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let cp = client.cp_subsystem();
    ///
    /// // List all CP groups
    /// let group_ids = cp.get_cp_group_ids().await?;
    /// for id in &group_ids {
    ///     println!("CP Group: {}", id.name());
    /// }
    ///
    /// // Get CP members
    /// let members = cp.get_cp_members().await?;
    /// println!("CP members: {}", members.len());
    ///
    /// // Force destroy a group (dangerous!)
    /// cp.force_destroy_cp_group("stuck-group").await?;
    /// ```
    pub fn cp_subsystem(&self) -> CPSubsystemManagementService {
        CPSubsystemManagementService::new(Arc::clone(&self.connection_manager))
    }

    /// Returns the CP Session management service.
    ///
    /// The CP Session management service provides operations to:
    /// - Query active CP sessions for a group
    /// - Force close stuck CP sessions (use with caution)
    ///
    /// CP sessions are used internally by the CP Subsystem to track client
    /// and server endpoints that hold resources like locks and semaphores.
    ///
    /// Note: These operations require the CP Subsystem to be enabled on the
    /// Hazelcast cluster.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let session_mgmt = client.cp_session_management();
    ///
    /// // List all sessions for a CP group
    /// let sessions = session_mgmt.get_sessions("default").await?;
    /// for session in &sessions {
    ///     println!("Session {} owned by {}", session.id().session_id(), session.endpoint_name());
    ///     println!("  Created: {}, Expires: {}", session.creation_time(), session.expiration_time());
    /// }
    ///
    /// // Force close a stuck session (dangerous!)
    /// if let Some(session) = sessions.first() {
    ///     session_mgmt.force_close_session("default", session.id().session_id()).await?;
    /// }
    /// ```
    pub fn cp_session_management(&self) -> CPSessionManagementService {
        CPSessionManagementService::new(Arc::clone(&self.connection_manager))
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

    /// Subscribes to distributed object events.
    ///
    /// The returned receiver will emit events when distributed objects are
    /// created or destroyed on the cluster.
    pub fn subscribe_distributed_object(
        &self,
    ) -> tokio::sync::broadcast::Receiver<DistributedObjectEvent> {
        self.connection_manager.subscribe_distributed_object()
    }

    /// Returns a collection of all distributed objects in the cluster.
    ///
    /// This includes all maps, queues, topics, and other distributed data structures
    /// that have been created on the cluster.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let objects = client.get_distributed_objects().await?;
    /// for obj in objects {
    ///     println!("Object: {} (service: {})", obj.name(), obj.service_name());
    /// }
    /// ```
    pub async fn get_distributed_objects(&self) -> Result<Vec<DistributedObjectInfo>> {
        use hazelcast_core::protocol::constants::{
            CLIENT_GET_DISTRIBUTED_OBJECTS, PARTITION_ID_ANY,
        };
        use hazelcast_core::protocol::{ClientMessage, Frame};

        let mut request = ClientMessage::new_request(CLIENT_GET_DISTRIBUTED_OBJECTS);
        request.set_partition_id(PARTITION_ID_ANY);

        let response = self.connection_manager.send(request).await?;

        let mut result = Vec::new();
        let frames = response.frames();

        let mut frame_idx = 1;

        while frame_idx + 1 < frames.len() {
            let service_frame = &frames[frame_idx];
            let name_frame = &frames[frame_idx + 1];

            if service_frame.is_end_frame() {
                break;
            }

            let service_name = String::from_utf8_lossy(service_frame.content()).to_string();
            let name = String::from_utf8_lossy(name_frame.content()).to_string();

            result.push(DistributedObjectInfo::new(service_name, name));
            frame_idx += 2;
        }

        tracing::debug!(count = result.len(), "retrieved distributed objects");
        Ok(result)
    }

    /// Destroys a distributed object on the cluster.
    ///
    /// After destruction, the object is removed from the cluster and any local
    /// proxies will become invalid.
    ///
    /// # Arguments
    ///
    /// * `service_name` - The service name (e.g., "hz:impl:mapService" for maps)
    /// * `name` - The name of the distributed object to destroy
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Destroy a map
    /// client.destroy_distributed_object("hz:impl:mapService", "my-map").await?;
    ///
    /// // Destroy a queue
    /// client.destroy_distributed_object("hz:impl:queueService", "my-queue").await?;
    /// ```
    pub async fn destroy_distributed_object(
        &self,
        service_name: &str,
        name: &str,
    ) -> Result<()> {
        use hazelcast_core::protocol::constants::{CLIENT_DESTROY_PROXY, PARTITION_ID_ANY};
        use hazelcast_core::protocol::{ClientMessage, Frame};

        let mut request = ClientMessage::new_request(CLIENT_DESTROY_PROXY);
        request.set_partition_id(PARTITION_ID_ANY);

        let service_bytes = BytesMut::from(service_name.as_bytes());
        request.add_frame(Frame::with_content(service_bytes));

        let name_bytes = BytesMut::from(name.as_bytes());
        request.add_frame(Frame::with_content(name_bytes));

        let _response = self.connection_manager.send(request).await?;

        tracing::info!(
            service_name = %service_name,
            name = %name,
            "destroyed distributed object"
        );

        Ok(())
    }

    /// Adds a distributed object listener.
    ///
    /// The listener will be notified when distributed objects are created or
    /// destroyed on the cluster.
    ///
    /// # Arguments
    ///
    /// * `listener` - The listener to add
    ///
    /// # Returns
    ///
    /// A `ListenerId` that can be used to remove the listener later.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::listener::{DistributedObjectListener, DistributedObjectEvent};
    ///
    /// struct MyListener;
    ///
    /// impl DistributedObjectListener for MyListener {
    ///     fn distributed_object_created(&self, event: &DistributedObjectEvent) {
    ///         println!("Created: {} ({})", event.name, event.service_name);
    ///     }
    ///
    ///     fn distributed_object_destroyed(&self, event: &DistributedObjectEvent) {
    ///         println!("Destroyed: {} ({})", event.name, event.service_name);
    ///     }
    /// }
    ///
    /// let listener_id = client.add_distributed_object_listener(MyListener).await?;
    /// ```
    pub async fn add_distributed_object_listener<L>(&self, listener: L) -> Result<ListenerId>
    where
        L: DistributedObjectListener + 'static,
    {
        let listener_id = ListenerId::new();
        let registration = ListenerRegistration::new(listener_id);
        let listener_arc: Arc<dyn DistributedObjectListener> = Arc::new(listener);

        let mut rx = self.connection_manager.subscribe_distributed_object();
        let listener_clone = Arc::clone(&listener_arc);
        let active_flag = registration.active_flag();

        tokio::spawn(async move {
            while active_flag.load(std::sync::atomic::Ordering::Acquire) {
                match rx.recv().await {
                    Ok(event) => {
                        if !active_flag.load(std::sync::atomic::Ordering::Acquire) {
                            break;
                        }
                        match event.event_type {
                            crate::listener::DistributedObjectEventType::Created => {
                                listener_clone.distributed_object_created(&event);
                            }
                            crate::listener::DistributedObjectEventType::Destroyed => {
                                listener_clone.distributed_object_destroyed(&event);
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        {
            let mut state = self
                .distributed_object_listeners
                .write()
                .map_err(|_| hazelcast_core::HazelcastError::IllegalState("lock poisoned".into()))?;
            state.listeners.insert(listener_id, (listener_arc, registration));
        }

        tracing::debug!(listener_id = %listener_id, "added distributed object listener");
        Ok(listener_id)
    }

    /// Removes a distributed object listener.
    ///
    /// # Arguments
    ///
    /// * `listener_id` - The ID of the listener to remove
    ///
    /// # Returns
    ///
    /// `true` if the listener was found and removed, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let listener_id = client.add_distributed_object_listener(MyListener).await?;
    /// // ... later ...
    /// let removed = client.remove_distributed_object_listener(listener_id).await?;
    /// ```
    pub async fn remove_distributed_object_listener(&self, listener_id: ListenerId) -> Result<bool> {
        let removed = {
            let mut state = self
                .distributed_object_listeners
                .write()
                .map_err(|_| hazelcast_core::HazelcastError::IllegalState("lock poisoned".into()))?;

            if let Some((_, registration)) = state.listeners.remove(&listener_id) {
                registration.deactivate();
                true
            } else {
                false
            }
        };

        if removed {
            tracing::debug!(listener_id = %listener_id, "removed distributed object listener");
        }

        Ok(removed)
    }

    /// Adds a client state listener.
    ///
    /// The listener will be notified when the client's connection state changes,
    /// including connect, disconnect, start, and shutdown events.
    ///
    /// # Arguments
    ///
    /// * `listener` - The listener to add
    ///
    /// # Returns
    ///
    /// A `ListenerId` that can be used to remove the listener later.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::listener::ClientStateListener;
    ///
    /// struct MyStateListener;
    ///
    /// impl ClientStateListener for MyStateListener {
    ///     fn client_connected(&self) {
    ///         println!("Connected to cluster!");
    ///     }
    ///
    ///     fn client_disconnected(&self) {
    ///         println!("Disconnected from cluster!");
    ///     }
    ///
    ///     fn client_started(&self) {
    ///         println!("Client started!");
    ///     }
    ///
    ///     fn client_shutdown(&self) {
    ///         println!("Client shutdown!");
    ///     }
    /// }
    ///
    /// let listener_id = client.add_client_state_listener(MyStateListener).await?;
    /// ```
    pub async fn add_client_state_listener<L>(&self, listener: L) -> Result<ListenerId>
    where
        L: ClientStateListener + 'static,
    {
        let listener_id = ListenerId::new();
        let registration = ListenerRegistration::new(listener_id);
        let listener_arc: Arc<dyn ClientStateListener> = Arc::new(listener);

        let mut rx = self.connection_manager.subscribe_lifecycle();
        let listener_clone = Arc::clone(&listener_arc);
        let active_flag = registration.active_flag();

        tokio::spawn(async move {
            while active_flag.load(std::sync::atomic::Ordering::Acquire) {
                match rx.recv().await {
                    Ok(event) => {
                        if !active_flag.load(std::sync::atomic::Ordering::Acquire) {
                            break;
                        }
                        match event {
                            LifecycleEvent::ClientConnected => {
                                listener_clone.client_connected();
                            }
                            LifecycleEvent::ClientDisconnected => {
                                listener_clone.client_disconnected();
                            }
                            LifecycleEvent::Started => {
                                listener_clone.client_started();
                            }
                            LifecycleEvent::Shutdown => {
                                listener_clone.client_shutdown();
                            }
                            _ => {}
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        {
            let mut state = self
                .client_state_listeners
                .write()
                .map_err(|_| hazelcast_core::HazelcastError::IllegalState("lock poisoned".into()))?;
            state.listeners.insert(listener_id, (listener_arc, registration));
        }

        tracing::debug!(listener_id = %listener_id, "added client state listener");
        Ok(listener_id)
    }

    /// Removes a client state listener.
    ///
    /// # Arguments
    ///
    /// * `listener_id` - The ID of the listener to remove
    ///
    /// # Returns
    ///
    /// `true` if the listener was found and removed, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let listener_id = client.add_client_state_listener(MyStateListener).await?;
    /// // ... later ...
    /// let removed = client.remove_client_state_listener(listener_id).await?;
    /// ```
    pub async fn remove_client_state_listener(&self, listener_id: ListenerId) -> Result<bool> {
        let removed = {
            let mut state = self
                .client_state_listeners
                .write()
                .map_err(|_| hazelcast_core::HazelcastError::IllegalState("lock poisoned".into()))?;

            if let Some((_, registration)) = state.listeners.remove(&listener_id) {
                registration.deactivate();
                true
            } else {
                false
            }
        };

        if removed {
            tracing::debug!(listener_id = %listener_id, "removed client state listener");
        }

        Ok(removed)
    }

    /// Adds a partition lost listener.
    ///
    /// The listener will be notified when partitions are lost (all replicas
    /// become unavailable) in the cluster.
    ///
    /// # Arguments
    ///
    /// * `listener` - The listener to add
    ///
    /// # Returns
    ///
    /// A `ListenerId` that can be used to remove the listener later.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::listener::{PartitionLostListener, PartitionLostEvent};
    ///
    /// struct MyPartitionLostListener;
    ///
    /// impl PartitionLostListener for MyPartitionLostListener {
    ///     fn partition_lost(&self, event: &PartitionLostEvent) {
    ///         println!("Partition {} lost!", event.partition_id);
    ///     }
    /// }
    ///
    /// let listener_id = client.add_partition_lost_listener(MyPartitionLostListener).await?;
    /// ```
    pub async fn add_partition_lost_listener<L>(&self, listener: L) -> Result<ListenerId>
    where
        L: PartitionLostListener + 'static,
    {
        let listener_id = ListenerId::new();
        let registration = ListenerRegistration::new(listener_id);
        let listener_arc: Arc<dyn PartitionLostListener> = Arc::new(listener);

        let mut rx = self.connection_manager.subscribe_partition_lost();
        let listener_clone = Arc::clone(&listener_arc);
        let active_flag = registration.active_flag();

        tokio::spawn(async move {
            while active_flag.load(std::sync::atomic::Ordering::Acquire) {
                match rx.recv().await {
                    Ok(event) => {
                        if !active_flag.load(std::sync::atomic::Ordering::Acquire) {
                            break;
                        }
                        listener_clone.partition_lost(&event);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        {
            let mut state = self
                .partition_lost_listeners
                .write()
                .map_err(|_| hazelcast_core::HazelcastError::IllegalState("lock poisoned".into()))?;
            state.listeners.insert(listener_id, (listener_arc, registration));
        }

        tracing::debug!(listener_id = %listener_id, "added partition lost listener");
        Ok(listener_id)
    }

    /// Removes a partition lost listener.
    ///
    /// # Arguments
    ///
    /// * `listener_id` - The ID of the listener to remove
    ///
    /// # Returns
    ///
    /// `true` if the listener was found and removed, `false` otherwise.
    pub async fn remove_partition_lost_listener(&self, listener_id: ListenerId) -> Result<bool> {
        let removed = {
            let mut state = self
                .partition_lost_listeners
                .write()
                .map_err(|_| hazelcast_core::HazelcastError::IllegalState("lock poisoned".into()))?;

            if let Some((_, registration)) = state.listeners.remove(&listener_id) {
                registration.deactivate();
                true
            } else {
                false
            }
        };

        if removed {
            tracing::debug!(listener_id = %listener_id, "removed partition lost listener");
        }

        Ok(removed)
    }

    /// Adds a migration listener.
    ///
    /// The listener will be notified when partition migrations occur between
    /// cluster members.
    ///
    /// # Arguments
    ///
    /// * `listener` - The listener to add
    ///
    /// # Returns
    ///
    /// A `ListenerId` that can be used to remove the listener later.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::listener::{MigrationListener, MigrationEvent};
    ///
    /// struct MyMigrationListener;
    ///
    /// impl MigrationListener for MyMigrationListener {
    ///     fn migration_started(&self, event: &MigrationEvent) {
    ///         println!("Migration started for partition {}", event.partition_id);
    ///     }
    ///
    ///     fn migration_completed(&self, event: &MigrationEvent) {
    ///         println!("Migration completed for partition {}", event.partition_id);
    ///     }
    ///
    ///     fn migration_failed(&self, event: &MigrationEvent) {
    ///         println!("Migration failed for partition {}", event.partition_id);
    ///     }
    /// }
    ///
    /// let listener_id = client.add_migration_listener(MyMigrationListener).await?;
    /// ```
    pub async fn add_migration_listener<L>(&self, listener: L) -> Result<ListenerId>
    where
        L: MigrationListener + 'static,
    {
        let listener_id = ListenerId::new();
        let registration = ListenerRegistration::new(listener_id);
        let listener_arc: Arc<dyn MigrationListener> = Arc::new(listener);

        let mut rx = self.connection_manager.subscribe_migration();
        let listener_clone = Arc::clone(&listener_arc);
        let active_flag = registration.active_flag();

        tokio::spawn(async move {
            while active_flag.load(std::sync::atomic::Ordering::Acquire) {
                match rx.recv().await {
                    Ok(event) => {
                        if !active_flag.load(std::sync::atomic::Ordering::Acquire) {
                            break;
                        }
                        match event.state {
                            MigrationState::Started => {
                                listener_clone.migration_started(&event);
                            }
                            MigrationState::Completed => {
                                listener_clone.migration_completed(&event);
                            }
                            MigrationState::Failed => {
                                listener_clone.migration_failed(&event);
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        {
            let mut state = self
                .migration_listeners
                .write()
                .map_err(|_| hazelcast_core::HazelcastError::IllegalState("lock poisoned".into()))?;
            state.listeners.insert(listener_id, (listener_arc, registration));
        }

        tracing::debug!(listener_id = %listener_id, "added migration listener");
        Ok(listener_id)
    }

    /// Removes a migration listener.
    ///
    /// # Arguments
    ///
    /// * `listener_id` - The ID of the listener to remove
    ///
    /// # Returns
    ///
    /// `true` if the listener was found and removed, `false` otherwise.
    pub async fn remove_migration_listener(&self, listener_id: ListenerId) -> Result<bool> {
        let removed = {
            let mut state = self
                .migration_listeners
                .write()
                .map_err(|_| hazelcast_core::HazelcastError::IllegalState("lock poisoned".into()))?;

            if let Some((_, registration)) = state.listeners.remove(&listener_id) {
                registration.deactivate();
                true
            } else {
                false
            }
        };

        if removed {
            tracing::debug!(listener_id = %listener_id, "removed migration listener");
        }

        Ok(removed)
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

    #[test]
    fn test_distributed_object_info() {
        let info = DistributedObjectInfo::new("hz:impl:mapService", "test-map");
        assert_eq!(info.service_name(), "hz:impl:mapService");
        assert_eq!(info.name(), "test-map");
    }

    #[test]
    fn test_distributed_object_info_equality() {
        let info1 = DistributedObjectInfo::new("service", "name");
        let info2 = DistributedObjectInfo::new("service", "name");
        let info3 = DistributedObjectInfo::new("service", "other");

        assert_eq!(info1, info2);
        assert_ne!(info1, info3);
    }

    #[test]
    fn test_distributed_object_info_clone() {
        let info = DistributedObjectInfo::new("service", "name");
        let cloned = info.clone();
        assert_eq!(info, cloned);
    }

    #[test]
    fn test_client_state_listener_state_debug() {
        let state = ClientStateListenerState {
            listeners: std::collections::HashMap::new(),
        };
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("ClientStateListenerState"));
        assert!(debug_str.contains("listener_count"));
    }

    #[test]
    fn test_partition_lost_listener_state_debug() {
        let state = PartitionLostListenerState {
            listeners: std::collections::HashMap::new(),
        };
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("PartitionLostListenerState"));
        assert!(debug_str.contains("listener_count"));
    }

    #[test]
    fn test_migration_listener_state_debug() {
        let state = MigrationListenerState {
            listeners: std::collections::HashMap::new(),
        };
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("MigrationListenerState"));
        assert!(debug_str.contains("listener_count"));
    }
}
