//! Transaction support for Hazelcast distributed operations.
//!
//! This module provides transactional access to Hazelcast data structures,
//! ensuring atomicity across multiple operations.
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client::{HazelcastClient, TransactionOptions, TransactionType};
//!
//! let client = HazelcastClient::new(config).await?;
//! let options = TransactionOptions::new()
//!     .with_timeout(Duration::from_secs(30))
//!     .with_type(TransactionType::TwoPhase);
//!
//! let mut txn = client.new_transaction_context(options);
//! txn.begin().await?;
//!
//! let map = txn.get_map::<String, String>("my-map");
//! map.put("key".to_string(), "value".to_string()).await?;
//!
//! txn.commit().await?;
//! ```

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use uuid::Uuid;

use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{ClientMessage, Deserializable, HazelcastError, Result, Serializable};

use crate::connection::ConnectionManager;

pub mod xa;
pub use xa::*;

// ============================================================================
// Transaction Types and Options
// ============================================================================

/// The type of transaction to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionType {
    /// Single-phase transaction (faster, less durable).
    #[default]
    OnePhase = 1,
    /// Two-phase transaction (slower, more durable).
    TwoPhase = 2,
}

impl TransactionType {
    /// Returns the wire protocol value for this transaction type.
    pub fn value(&self) -> i32 {
        *self as i32
    }

    /// Creates a TransactionType from its wire protocol value.
    pub fn from_value(value: i32) -> Option<Self> {
        match value {
            1 => Some(TransactionType::OnePhase),
            2 => Some(TransactionType::TwoPhase),
            _ => None,
        }
    }
}

/// Options for configuring a transaction.
#[derive(Debug, Clone)]
pub struct TransactionOptions {
    timeout: Duration,
    durability: i32,
    transaction_type: TransactionType,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(120),
            durability: 1,
            transaction_type: TransactionType::OnePhase,
        }
    }
}

impl TransactionOptions {
    /// Creates new transaction options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the transaction timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the durability level (number of backups).
    pub fn with_durability(mut self, durability: i32) -> Self {
        self.durability = durability;
        self
    }

    /// Sets the transaction type.
    pub fn with_type(mut self, transaction_type: TransactionType) -> Self {
        self.transaction_type = transaction_type;
        self
    }

    /// Returns the timeout duration.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns the durability level.
    pub fn durability(&self) -> i32 {
        self.durability
    }

    /// Returns the transaction type.
    pub fn transaction_type(&self) -> TransactionType {
        self.transaction_type
    }
}

// ============================================================================
// Transaction State
// ============================================================================

/// The current state of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction has not been started.
    NotStarted,
    /// Transaction is active and operations can be performed.
    Active,
    /// Transaction has been committed.
    Committed,
    /// Transaction has been rolled back.
    RolledBack,
}

// ============================================================================
// Transaction Context
// ============================================================================

/// A context for performing transactional operations.
///
/// The transaction context manages the lifecycle of a transaction and provides
/// access to transactional data structure proxies.
#[derive(Debug)]
pub struct TransactionContext {
    connection_manager: Arc<ConnectionManager>,
    options: TransactionOptions,
    txn_id: Uuid,
    thread_id: i64,
    state: TransactionState,
    start_time: Option<std::time::Instant>,
}

impl TransactionContext {
    /// Creates a new transaction context.
    pub(crate) fn new(
        connection_manager: Arc<ConnectionManager>,
        options: TransactionOptions,
    ) -> Self {
        static THREAD_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

        Self {
            connection_manager,
            options,
            txn_id: Uuid::new_v4(),
            thread_id: THREAD_ID_COUNTER.fetch_add(1, Ordering::Relaxed) as i64,
            state: TransactionState::NotStarted,
            start_time: None,
        }
    }

    /// Returns the transaction ID.
    pub fn txn_id(&self) -> Uuid {
        self.txn_id
    }

    /// Returns the current transaction state.
    pub fn state(&self) -> TransactionState {
        self.state
    }

    /// Returns true if the transaction is active.
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    /// Begins the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction has already been started or completed.
    pub async fn begin(&mut self) -> Result<()> {
        if self.state != TransactionState::NotStarted {
            return Err(HazelcastError::Protocol(
                "Transaction has already been started".to_string(),
            ));
        }

        let timeout_ms = self.options.timeout.as_millis() as i64;

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_CREATE);
        message.add_frame(txn_long_frame(timeout_ms));
        message.add_frame(txn_int_frame(self.options.durability));
        message.add_frame(txn_int_frame(self.options.transaction_type.value()));
        message.add_frame(txn_long_frame(self.thread_id));

        let _response = txn_invoke(&self.connection_manager, message).await?;

        self.state = TransactionState::Active;
        self.start_time = Some(std::time::Instant::now());

        Ok(())
    }

    /// Commits the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is not active.
    pub async fn commit(&mut self) -> Result<()> {
        if self.state != TransactionState::Active {
            return Err(HazelcastError::Protocol(
                "Transaction is not active".to_string(),
            ));
        }

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_COMMIT);
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));

        let _response = txn_invoke(&self.connection_manager, message).await?;

        self.state = TransactionState::Committed;
        Ok(())
    }

    /// Rolls back the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is not active.
    pub async fn rollback(&mut self) -> Result<()> {
        if self.state != TransactionState::Active {
            return Err(HazelcastError::Protocol(
                "Transaction is not active".to_string(),
            ));
        }

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_ROLLBACK);
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));

        let _response = txn_invoke(&self.connection_manager, message).await?;

        self.state = TransactionState::RolledBack;
        Ok(())
    }

    /// Returns a transactional map proxy.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is not active.
    pub fn get_map<K, V>(&self, name: &str) -> Result<TransactionalMap<K, V>>
    where
        K: Serializable + Deserializable + Send + Sync,
        V: Serializable + Deserializable + Send + Sync,
    {
        self.ensure_active("get_map")?;
        Ok(TransactionalMap::new(
            name.to_string(),
            Arc::clone(&self.connection_manager),
            self.txn_id,
            self.thread_id,
        ))
    }

    /// Returns a transactional queue proxy.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is not active.
    pub fn get_queue<T>(&self, name: &str) -> Result<TransactionalQueue<T>>
    where
        T: Serializable + Deserializable + Send + Sync,
    {
        self.ensure_active("get_queue")?;
        Ok(TransactionalQueue::new(
            name.to_string(),
            Arc::clone(&self.connection_manager),
            self.txn_id,
            self.thread_id,
        ))
    }

    /// Returns a transactional set proxy.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is not active.
    pub fn get_set<T>(&self, name: &str) -> Result<TransactionalSet<T>>
    where
        T: Serializable + Deserializable + Send + Sync,
    {
        self.ensure_active("get_set")?;
        Ok(TransactionalSet::new(
            name.to_string(),
            Arc::clone(&self.connection_manager),
            self.txn_id,
            self.thread_id,
        ))
    }

    /// Returns a transactional list proxy.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is not active.
    pub fn get_list<T>(&self, name: &str) -> Result<TransactionalList<T>>
    where
        T: Serializable + Deserializable + Send + Sync,
    {
        self.ensure_active("get_list")?;
        Ok(TransactionalList::new(
            name.to_string(),
            Arc::clone(&self.connection_manager),
            self.txn_id,
            self.thread_id,
        ))
    }

    /// Returns a transactional multimap proxy for the given name.
    ///
    /// All operations on the returned multimap will participate in this transaction.
    ///
    /// Returns an error if the transaction is not active.
    pub fn get_multimap<K, V>(&self, name: &str) -> Result<TransactionalMultiMap<K, V>>
    where
        K: Serializable + Deserializable + Send + Sync,
        V: Serializable + Deserializable + Send + Sync,
    {
        self.ensure_active("get_multimap")?;
        Ok(TransactionalMultiMap::new(
            name.to_string(),
            Arc::clone(&self.connection_manager),
            self.txn_id,
            self.thread_id,
        ))
    }

    /// Ensures the transaction is in the `Active` state, returning an error if not.
    fn ensure_active(&self, operation: &str) -> Result<()> {
        if self.state != TransactionState::Active {
            return Err(HazelcastError::IllegalState(format!(
                "transaction must be active to call {}, current state: {:?}",
                operation, self.state
            )));
        }
        Ok(())
    }

    /// Creates a new XA transaction associated with this context's connection.
    ///
    /// # Arguments
    ///
    /// * `xid` - The XA transaction identifier
    ///
    /// # Example
    ///
    /// ```ignore
    /// let xid = Xid::new(0, b"global-txn", b"branch-1");
    /// let xa_txn = ctx.create_xa_transaction(xid);
    /// ```
    pub fn create_xa_transaction(&self, xid: xa::Xid) -> xa::XATransaction {
        xa::XATransaction::new(Arc::clone(&self.connection_manager), xid)
    }

    /// Creates a new XA transaction with an auto-generated Xid.
    pub fn create_xa_transaction_auto(&self) -> xa::XATransaction {
        xa::XATransaction::with_generated_xid(Arc::clone(&self.connection_manager))
    }

}

// ============================================================================
// Shared Transactional Helpers
// ============================================================================

/// Serializes a value into a byte vector using the Hazelcast data format.
fn txn_serialize_value<T: Serializable>(value: &T) -> Result<Vec<u8>> {
    let mut output = ObjectDataOutput::new();
    value.serialize(&mut output)?;
    Ok(output.into_bytes())
}

/// Creates a frame containing a UTF-8 string.
fn txn_string_frame(s: &str) -> Frame {
    Frame::with_content(BytesMut::from(s.as_bytes()))
}

/// Creates a frame containing raw byte data.
fn txn_data_frame(data: &[u8]) -> Frame {
    Frame::with_content(BytesMut::from(data))
}

/// Creates a frame containing a little-endian i64.
fn txn_long_frame(value: i64) -> Frame {
    let mut buf = BytesMut::with_capacity(8);
    buf.extend_from_slice(&value.to_le_bytes());
    Frame::with_content(buf)
}

/// Creates a frame containing a little-endian i32.
fn txn_int_frame(value: i32) -> Frame {
    let mut buf = BytesMut::with_capacity(4);
    buf.extend_from_slice(&value.to_le_bytes());
    Frame::with_content(buf)
}

/// Creates a frame containing a UUID.
fn txn_uuid_frame(uuid: Uuid) -> Frame {
    let mut buf = BytesMut::with_capacity(16);
    buf.extend_from_slice(uuid.as_bytes());
    Frame::with_content(buf)
}

/// Sends a message and waits for the response on an available connection.
async fn txn_invoke(
    connection_manager: &ConnectionManager,
    message: ClientMessage,
) -> Result<ClientMessage> {
    let address = txn_get_connection_address(connection_manager).await?;
    connection_manager.send_to(address, message).await?;
    connection_manager
        .receive_from(address)
        .await?
        .ok_or_else(|| HazelcastError::Connection("connection closed unexpectedly".to_string()))
}

/// Returns the address of an available connection.
async fn txn_get_connection_address(
    connection_manager: &ConnectionManager,
) -> Result<SocketAddr> {
    let addresses = connection_manager.connected_addresses().await;
    addresses
        .into_iter()
        .next()
        .ok_or_else(|| HazelcastError::Connection("no connections available".to_string()))
}

/// Decodes a nullable value from the second frame of a response message.
fn txn_decode_nullable_response<T: Deserializable>(
    response: &ClientMessage,
) -> Result<Option<T>> {
    let frames = response.frames();
    if frames.len() < 2 {
        return Ok(None);
    }

    let data_frame = &frames[1];
    if data_frame.flags & IS_NULL_FLAG != 0 || data_frame.content.is_empty() {
        return Ok(None);
    }

    let mut input = ObjectDataInput::new(&data_frame.content);
    T::deserialize(&mut input).map(Some)
}

/// Decodes a boolean value from the initial frame of a response message.
fn txn_decode_bool_response(response: &ClientMessage) -> Result<bool> {
    let frames = response.frames();
    if frames.is_empty() {
        return Err(HazelcastError::Serialization("empty response".to_string()));
    }

    let initial_frame = &frames[0];
    if initial_frame.content.len() > RESPONSE_HEADER_SIZE {
        Ok(initial_frame.content[RESPONSE_HEADER_SIZE] != 0)
    } else {
        Err(HazelcastError::Serialization(
            "response frame too short for bool field".to_string(),
        ))
    }
}

/// Decodes a collection (Vec) of deserialized values from response frames.
fn txn_decode_collection_response<T: Deserializable>(
    response: &ClientMessage,
) -> Result<Vec<T>> {
    let frames = response.frames();
    let mut result = Vec::new();
    for frame in frames.iter().skip(1) {
        if frame.flags & IS_NULL_FLAG != 0 || frame.content.is_empty() {
            continue;
        }
        if frame.flags & END_FLAG != 0 {
            break;
        }
        let mut input = ObjectDataInput::new(&frame.content);
        result.push(T::deserialize(&mut input)?);
    }
    Ok(result)
}

/// Decodes an i32 value from the initial frame of a response message.
fn txn_decode_int_response(response: &ClientMessage) -> Result<i32> {
    let frames = response.frames();
    if frames.is_empty() {
        return Err(HazelcastError::Serialization("empty response".to_string()));
    }

    let initial_frame = &frames[0];
    if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 4 {
        let offset = RESPONSE_HEADER_SIZE;
        Ok(i32::from_le_bytes([
            initial_frame.content[offset],
            initial_frame.content[offset + 1],
            initial_frame.content[offset + 2],
            initial_frame.content[offset + 3],
        ]))
    } else {
        Err(HazelcastError::Serialization(
            "response frame too short for int field".to_string(),
        ))
    }
}

// ============================================================================
// Transactional Map
// ============================================================================

/// A transactional map proxy for performing key-value operations within a transaction.
#[derive(Debug)]
pub struct TransactionalMap<K, V> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    txn_id: Uuid,
    thread_id: i64,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> TransactionalMap<K, V> {
    fn new(
        name: String,
        connection_manager: Arc<ConnectionManager>,
        txn_id: Uuid,
        thread_id: i64,
    ) -> Self {
        Self {
            name,
            connection_manager,
            txn_id,
            thread_id,
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this map.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<K, V> TransactionalMap<K, V>
where
    K: Serializable + Deserializable + Send + Sync,
    V: Serializable + Deserializable + Send + Sync,
{
    /// Retrieves the value associated with the given key.
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        let key_data = txn_serialize_value(key)?;

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_MAP_GET);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&key_data));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_nullable_response(&response)
    }

    /// Associates the specified value with the specified key.
    pub async fn put(&self, key: K, value: V) -> Result<Option<V>> {
        let key_data = txn_serialize_value(&key)?;
        let value_data = txn_serialize_value(&value)?;

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_MAP_PUT);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&key_data));
        message.add_frame(txn_data_frame(&value_data));
        message.add_frame(txn_long_frame(-1)); // TTL

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_nullable_response(&response)
    }

    /// Sets the value for the specified key (without returning old value).
    pub async fn set(&self, key: K, value: V) -> Result<()> {
        let key_data = txn_serialize_value(&key)?;
        let value_data = txn_serialize_value(&value)?;

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_MAP_SET);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&key_data));
        message.add_frame(txn_data_frame(&value_data));

        txn_invoke(&self.connection_manager, message).await?;
        Ok(())
    }

    /// Puts a value if the key is not already present.
    pub async fn put_if_absent(&self, key: K, value: V) -> Result<Option<V>> {
        let key_data = txn_serialize_value(&key)?;
        let value_data = txn_serialize_value(&value)?;

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_MAP_PUT_IF_ABSENT);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&key_data));
        message.add_frame(txn_data_frame(&value_data));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_nullable_response(&response)
    }

    /// Replaces the value for a key if it exists.
    pub async fn replace(&self, key: &K, value: V) -> Result<Option<V>> {
        let key_data = txn_serialize_value(key)?;
        let value_data = txn_serialize_value(&value)?;

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_MAP_REPLACE);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&key_data));
        message.add_frame(txn_data_frame(&value_data));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_nullable_response(&response)
    }

    /// Removes the mapping for a key.
    pub async fn remove(&self, key: &K) -> Result<Option<V>> {
        let key_data = txn_serialize_value(key)?;

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_MAP_REMOVE);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&key_data));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_nullable_response(&response)
    }

    /// Deletes the mapping for a key (without returning old value).
    pub async fn delete(&self, key: &K) -> Result<()> {
        let key_data = txn_serialize_value(key)?;

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_MAP_DELETE);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&key_data));

        txn_invoke(&self.connection_manager, message).await?;
        Ok(())
    }

    /// Returns true if the map contains the specified key.
    pub async fn contains_key(&self, key: &K) -> Result<bool> {
        let key_data = txn_serialize_value(key)?;

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_MAP_CONTAINS_KEY);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&key_data));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_bool_response(&response)
    }

    /// Returns the number of entries in this map.
    pub async fn size(&self) -> Result<usize> {
        let mut message = ClientMessage::create_for_encode_any_partition(TXN_MAP_SIZE);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_int_response(&response).map(|v| v as usize)
    }
}

// ============================================================================
// Transactional Queue
// ============================================================================

/// A transactional queue proxy for performing FIFO operations within a transaction.
#[derive(Debug)]
pub struct TransactionalQueue<T> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    txn_id: Uuid,
    thread_id: i64,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> TransactionalQueue<T> {
    fn new(
        name: String,
        connection_manager: Arc<ConnectionManager>,
        txn_id: Uuid,
        thread_id: i64,
    ) -> Self {
        Self {
            name,
            connection_manager,
            txn_id,
            thread_id,
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this queue.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<T> TransactionalQueue<T>
where
    T: Serializable + Deserializable + Send + Sync,
{
    /// Inserts the specified element into this queue.
    pub async fn offer(&self, item: T) -> Result<bool> {
        let item_data = txn_serialize_value(&item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_QUEUE_OFFER);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&item_data));
        message.add_frame(txn_long_frame(0)); // timeout

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_bool_response(&response)
    }

    /// Retrieves and removes the head of this queue.
    pub async fn poll(&self) -> Result<Option<T>> {
        let mut message = ClientMessage::create_for_encode_any_partition(TXN_QUEUE_POLL);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_long_frame(0)); // timeout

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_nullable_response(&response)
    }

    /// Returns the number of elements in this queue.
    pub async fn size(&self) -> Result<usize> {
        let mut message = ClientMessage::create_for_encode_any_partition(TXN_QUEUE_SIZE);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_int_response(&response).map(|v| v as usize)
    }
}

// ============================================================================
// Transactional Set
// ============================================================================

/// A transactional set proxy for performing set operations within a transaction.
#[derive(Debug)]
pub struct TransactionalSet<T> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    txn_id: Uuid,
    thread_id: i64,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> TransactionalSet<T> {
    fn new(
        name: String,
        connection_manager: Arc<ConnectionManager>,
        txn_id: Uuid,
        thread_id: i64,
    ) -> Self {
        Self {
            name,
            connection_manager,
            txn_id,
            thread_id,
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this set.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<T> TransactionalSet<T>
where
    T: Serializable + Deserializable + Send + Sync,
{
    /// Adds the specified element to this set.
    pub async fn add(&self, item: T) -> Result<bool> {
        let item_data = txn_serialize_value(&item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_SET_ADD);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&item_data));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_bool_response(&response)
    }

    /// Removes the specified element from this set.
    pub async fn remove(&self, item: &T) -> Result<bool> {
        let item_data = txn_serialize_value(item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_SET_REMOVE);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&item_data));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_bool_response(&response)
    }

    /// Returns the number of elements in this set.
    pub async fn size(&self) -> Result<usize> {
        let mut message = ClientMessage::create_for_encode_any_partition(TXN_SET_SIZE);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_int_response(&response).map(|v| v as usize)
    }
}

// ============================================================================
// Transactional List
// ============================================================================

/// A transactional list proxy for performing list operations within a transaction.
#[derive(Debug)]
pub struct TransactionalList<T> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    txn_id: Uuid,
    thread_id: i64,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> TransactionalList<T> {
    fn new(
        name: String,
        connection_manager: Arc<ConnectionManager>,
        txn_id: Uuid,
        thread_id: i64,
    ) -> Self {
        Self {
            name,
            connection_manager,
            txn_id,
            thread_id,
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this list.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<T> TransactionalList<T>
where
    T: Serializable + Deserializable + Send + Sync,
{
    /// Appends the specified element to the end of this list.
    pub async fn add(&self, item: T) -> Result<bool> {
        let item_data = txn_serialize_value(&item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_LIST_ADD);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&item_data));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_bool_response(&response)
    }

    /// Removes the first occurrence of the specified element from this list.
    pub async fn remove(&self, item: &T) -> Result<bool> {
        let item_data = txn_serialize_value(item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(TXN_LIST_REMOVE);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&item_data));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_bool_response(&response)
    }

    /// Returns the number of elements in this list.
    pub async fn size(&self) -> Result<usize> {
        let mut message = ClientMessage::create_for_encode_any_partition(TXN_LIST_SIZE);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_int_response(&response).map(|v| v as usize)
    }
}

// ============================================================================
// TransactionalMultiMap
// ============================================================================

/// A transactional proxy for a Hazelcast MultiMap.
///
/// All operations on this proxy participate in the enclosing transaction.
/// Changes are only visible to other clients after the transaction is committed.
pub struct TransactionalMultiMap<K, V> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    txn_id: Uuid,
    thread_id: i64,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> TransactionalMultiMap<K, V>
where
    K: Serializable + Deserializable + Send + Sync,
    V: Serializable + Deserializable + Send + Sync,
{
    fn new(
        name: String,
        connection_manager: Arc<ConnectionManager>,
        txn_id: Uuid,
        thread_id: i64,
    ) -> Self {
        Self {
            name,
            connection_manager,
            txn_id,
            thread_id,
            _phantom: PhantomData,
        }
    }

    /// Stores a key-value pair in the multimap.
    ///
    /// Returns `true` if the pair was added (i.e., the value was not already
    /// associated with the key).
    pub async fn put(&self, key: K, value: V) -> Result<bool> {
        let key_data = txn_serialize_value(&key)?;
        let value_data = txn_serialize_value(&value)?;

        let mut message =
            ClientMessage::create_for_encode_any_partition(TXN_MULTIMAP_PUT);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&key_data));
        message.add_frame(txn_data_frame(&value_data));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_bool_response(&response)
    }

    /// Returns all values associated with the given key.
    pub async fn get(&self, key: &K) -> Result<Vec<V>> {
        let key_data = txn_serialize_value(key)?;

        let mut message =
            ClientMessage::create_for_encode_any_partition(TXN_MULTIMAP_GET);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&key_data));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_collection_response(&response)
    }

    /// Removes all values associated with the given key.
    ///
    /// Returns the collection of removed values.
    pub async fn remove(&self, key: &K) -> Result<Vec<V>> {
        let key_data = txn_serialize_value(key)?;

        let mut message =
            ClientMessage::create_for_encode_any_partition(TXN_MULTIMAP_REMOVE);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&key_data));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_collection_response(&response)
    }

    /// Removes a single key-value pair from the multimap.
    ///
    /// Returns `true` if the pair was removed.
    pub async fn remove_entry(&self, key: &K, value: &V) -> Result<bool> {
        let key_data = txn_serialize_value(key)?;
        let value_data = txn_serialize_value(value)?;

        let mut message =
            ClientMessage::create_for_encode_any_partition(TXN_MULTIMAP_REMOVE_ENTRY);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&key_data));
        message.add_frame(txn_data_frame(&value_data));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_bool_response(&response)
    }

    /// Returns the number of values associated with the given key.
    pub async fn value_count(&self, key: &K) -> Result<i32> {
        let key_data = txn_serialize_value(key)?;

        let mut message =
            ClientMessage::create_for_encode_any_partition(TXN_MULTIMAP_VALUE_COUNT);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));
        message.add_frame(txn_data_frame(&key_data));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_int_response(&response)
    }

    /// Returns the total number of key-value pairs in the multimap.
    pub async fn size(&self) -> Result<i32> {
        let mut message =
            ClientMessage::create_for_encode_any_partition(TXN_MULTIMAP_SIZE);
        message.add_frame(txn_string_frame(&self.name));
        message.add_frame(txn_uuid_frame(self.txn_id));
        message.add_frame(txn_long_frame(self.thread_id));

        let response = txn_invoke(&self.connection_manager, message).await?;
        txn_decode_int_response(&response)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_type_values() {
        assert_eq!(TransactionType::OnePhase.value(), 1);
        assert_eq!(TransactionType::TwoPhase.value(), 2);
    }

    #[test]
    fn test_transaction_type_from_value() {
        assert_eq!(
            TransactionType::from_value(1),
            Some(TransactionType::OnePhase)
        );
        assert_eq!(
            TransactionType::from_value(2),
            Some(TransactionType::TwoPhase)
        );
        assert_eq!(TransactionType::from_value(3), None);
    }

    #[test]
    fn test_transaction_options_default() {
        let options = TransactionOptions::default();
        assert_eq!(options.timeout(), Duration::from_secs(120));
        assert_eq!(options.durability(), 1);
        assert_eq!(options.transaction_type(), TransactionType::OnePhase);
    }

    #[test]
    fn test_transaction_options_builder() {
        let options = TransactionOptions::new()
            .with_timeout(Duration::from_secs(60))
            .with_durability(2)
            .with_type(TransactionType::TwoPhase);

        assert_eq!(options.timeout(), Duration::from_secs(60));
        assert_eq!(options.durability(), 2);
        assert_eq!(options.transaction_type(), TransactionType::TwoPhase);
    }

    #[test]
    fn test_transaction_state() {
        assert_eq!(TransactionState::NotStarted, TransactionState::NotStarted);
        assert_ne!(TransactionState::NotStarted, TransactionState::Active);
    }

    #[test]
    fn test_transactional_map_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TransactionalMap<String, String>>();
    }

    #[test]
    fn test_transactional_queue_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TransactionalQueue<String>>();
    }

    #[test]
    fn test_transactional_set_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TransactionalSet<String>>();
    }

    #[test]
    fn test_transactional_list_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TransactionalList<String>>();
    }

    #[test]
    fn test_transaction_options_clone() {
        let options = TransactionOptions::new()
            .with_timeout(Duration::from_secs(30))
            .with_type(TransactionType::TwoPhase);

        let cloned = options.clone();
        assert_eq!(cloned.timeout(), Duration::from_secs(30));
        assert_eq!(cloned.transaction_type(), TransactionType::TwoPhase);
    }
}
