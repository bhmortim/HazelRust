//! Distributed map proxy implementation.

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use tokio::spawn;
use uuid::Uuid;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{
    compute_partition_hash, ClientMessage, Deserializable, HazelcastError, Result, Serializable,
};

use crate::connection::ConnectionManager;
use crate::listener::{
    EntryEvent, EntryEventType, EntryListenerConfig, ListenerId, ListenerRegistration,
    ListenerStats,
};

/// A distributed map proxy for performing key-value operations on a Hazelcast cluster.
///
/// `IMap` provides async CRUD operations with automatic serialization and partition routing.
#[derive(Debug)]
pub struct IMap<K, V> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    listener_stats: Arc<ListenerStats>,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> IMap<K, V> {
    /// Creates a new map proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            listener_stats: Arc::new(ListenerStats::new()),
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this map.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<K, V> IMap<K, V>
where
    K: Serializable + Deserializable + Send + Sync,
    V: Serializable + Deserializable + Send + Sync,
{
    /// Retrieves the value associated with the given key.
    ///
    /// Returns `None` if the key does not exist in the map.
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_GET, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Associates the specified value with the specified key.
    ///
    /// Returns the previous value associated with the key, or `None` if there was no mapping.
    pub async fn put(&self, key: K, value: V) -> Result<Option<V>> {
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_PUT, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));
        message.add_frame(Self::long_frame(-1)); // TTL: no expiry
        message.add_frame(Self::long_frame(-1)); // Max idle: no expiry

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Removes the mapping for a key from this map if it is present.
    ///
    /// Returns the previous value associated with the key, or `None` if there was no mapping.
    pub async fn remove(&self, key: &K) -> Result<Option<V>> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_REMOVE, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Returns `true` if this map contains a mapping for the specified key.
    pub async fn contains_key(&self, key: &K) -> Result<bool> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_CONTAINS_KEY, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns the number of key-value mappings in this map.
    pub async fn size(&self) -> Result<usize> {
        let message = ClientMessage::create_for_encode(MAP_SIZE, PARTITION_ID_ANY);
        let mut msg = message;
        msg.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(msg).await?;
        Self::decode_int_response(&response).map(|v| v as usize)
    }

    /// Removes all entries from this map.
    pub async fn clear(&self) -> Result<()> {
        let message = ClientMessage::create_for_encode(MAP_CLEAR, PARTITION_ID_ANY);
        let mut msg = message;
        msg.add_frame(Self::string_frame(&self.name));

        self.invoke(msg).await?;
        Ok(())
    }

    fn serialize_value<T: Serializable>(value: &T) -> Result<Vec<u8>> {
        let mut output = ObjectDataOutput::new();
        value.serialize(&mut output)?;
        Ok(output.into_bytes())
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }

    fn data_frame(data: &[u8]) -> Frame {
        Frame::with_content(BytesMut::from(data))
    }

    fn long_frame(value: i64) -> Frame {
        let mut buf = BytesMut::with_capacity(8);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
    }

    async fn invoke(&self, mut message: ClientMessage) -> Result<ClientMessage> {
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

    fn decode_nullable_response<T: Deserializable>(response: &ClientMessage) -> Result<Option<T>> {
        let frames = response.frames();
        if frames.len() < 2 {
            return Ok(None);
        }

        let data_frame = &frames[1];

        if data_frame.flags & IS_NULL_FLAG != 0 {
            return Ok(None);
        }

        if data_frame.content.is_empty() {
            return Ok(None);
        }

        let mut input = ObjectDataInput::new(&data_frame.content);
        T::deserialize(&mut input).map(Some)
    }

    fn decode_bool_response(response: &ClientMessage) -> Result<bool> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization(
                "empty response".to_string(),
            ));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() > RESPONSE_HEADER_SIZE {
            Ok(initial_frame.content[RESPONSE_HEADER_SIZE] != 0)
        } else {
            Ok(false)
        }
    }

    fn decode_int_response(response: &ClientMessage) -> Result<i32> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization(
                "empty response".to_string(),
            ));
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
            Ok(0)
        }
    }

    fn bool_frame(value: bool) -> Frame {
        let mut buf = BytesMut::with_capacity(1);
        buf.extend_from_slice(&[if value { 1 } else { 0 }]);
        Frame::with_content(buf)
    }

    fn int_frame(value: i32) -> Frame {
        let mut buf = BytesMut::with_capacity(4);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
    }

    fn uuid_frame(uuid: Uuid) -> Frame {
        let mut buf = BytesMut::with_capacity(16);
        buf.extend_from_slice(uuid.as_bytes());
        Frame::with_content(buf)
    }

    fn decode_uuid_response(response: &ClientMessage) -> Result<Uuid> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("empty response".to_string()));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 16 {
            let offset = RESPONSE_HEADER_SIZE;
            let uuid_bytes: [u8; 16] = initial_frame.content[offset..offset + 16]
                .try_into()
                .map_err(|_| HazelcastError::Serialization("invalid UUID bytes".to_string()))?;
            Ok(Uuid::from_bytes(uuid_bytes))
        } else {
            Ok(Uuid::new_v4())
        }
    }

    fn is_entry_event(message: &ClientMessage, map_name: &str) -> bool {
        let frames = message.frames();
        if frames.is_empty() {
            return false;
        }

        let initial_frame = &frames[0];
        if initial_frame.flags & IS_EVENT_FLAG == 0 {
            return false;
        }

        if frames.len() > 1 {
            let name_frame = &frames[1];
            if let Ok(name) = std::str::from_utf8(&name_frame.content) {
                return name == map_name;
            }
        }

        true
    }

    fn decode_entry_event(message: &ClientMessage, include_value: bool) -> Result<EntryEvent<K, V>> {
        let frames = message.frames();
        if frames.len() < 3 {
            return Err(HazelcastError::Serialization(
                "insufficient frames for entry event".to_string(),
            ));
        }

        let initial_frame = &frames[0];
        let mut offset = RESPONSE_HEADER_SIZE;

        let event_type_value = if initial_frame.content.len() >= offset + 4 {
            i32::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
            ])
        } else {
            1
        };
        offset += 4;

        let event_type =
            EntryEventType::from_value(event_type_value).unwrap_or(EntryEventType::Added);

        let member_uuid = if initial_frame.content.len() >= offset + 16 {
            let uuid_bytes: [u8; 16] = initial_frame.content[offset..offset + 16]
                .try_into()
                .unwrap_or([0u8; 16]);
            Uuid::from_bytes(uuid_bytes)
        } else {
            Uuid::nil()
        };
        offset += 16;

        let timestamp = if initial_frame.content.len() >= offset + 8 {
            i64::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
                initial_frame.content[offset + 4],
                initial_frame.content[offset + 5],
                initial_frame.content[offset + 6],
                initial_frame.content[offset + 7],
            ])
        } else {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        };

        let key_frame = &frames[2];
        let key = if !key_frame.content.is_empty() && key_frame.flags & IS_NULL_FLAG == 0 {
            let mut input = ObjectDataInput::new(&key_frame.content);
            K::deserialize(&mut input)?
        } else {
            return Err(HazelcastError::Serialization(
                "missing key in entry event".to_string(),
            ));
        };

        let (old_value, new_value) = if include_value && frames.len() >= 5 {
            let old_value_frame = &frames[3];
            let old_value = if !old_value_frame.content.is_empty()
                && old_value_frame.flags & IS_NULL_FLAG == 0
            {
                let mut input = ObjectDataInput::new(&old_value_frame.content);
                V::deserialize(&mut input).ok()
            } else {
                None
            };

            let new_value_frame = &frames[4];
            let new_value = if !new_value_frame.content.is_empty()
                && new_value_frame.flags & IS_NULL_FLAG == 0
            {
                let mut input = ObjectDataInput::new(&new_value_frame.content);
                V::deserialize(&mut input).ok()
            } else {
                None
            };

            (old_value, new_value)
        } else {
            (None, None)
        };

        Ok(EntryEvent::new(
            key,
            old_value,
            new_value,
            event_type,
            member_uuid,
            timestamp,
        ))
    }

    /// Adds an entry listener to this map.
    ///
    /// The handler will be called for each entry event that matches the listener configuration.
    /// Returns a registration that can be used to remove the listener.
    pub async fn add_entry_listener<F>(
        &self,
        config: EntryListenerConfig,
        handler: F,
    ) -> Result<ListenerRegistration>
    where
        F: Fn(EntryEvent<K, V>) + Send + Sync + 'static,
        K: 'static,
        V: 'static,
    {
        let mut message =
            ClientMessage::create_for_encode(MAP_ADD_ENTRY_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(config.include_value));
        message.add_frame(Self::int_frame(config.event_flags()));
        message.add_frame(Self::bool_frame(false)); // local only = false

        let response = self.invoke(message).await?;
        let listener_uuid = Self::decode_uuid_response(&response)?;

        let registration = ListenerRegistration::new(ListenerId::from_uuid(listener_uuid));
        let active_flag = registration.active_flag();
        let shutdown_rx = registration.shutdown_receiver();

        let connection_manager = Arc::clone(&self.connection_manager);
        let listener_stats = Arc::clone(&self.listener_stats);
        let handler = Arc::new(handler);
        let map_name = self.name.clone();
        let include_value = config.include_value;

        spawn(async move {
            let mut shutdown_rx = match shutdown_rx {
                Some(rx) => rx,
                None => return,
            };

            loop {
                if !active_flag.load(Ordering::Acquire) {
                    break;
                }

                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        let addresses = connection_manager.connected_addresses().await;
                        for address in addresses {
                            if !active_flag.load(Ordering::Acquire) {
                                break;
                            }

                            match connection_manager.receive_from(address).await {
                                Ok(Some(msg)) => {
                                    if Self::is_entry_event(&msg, &map_name) {
                                        listener_stats.record_message();
                                        if let Ok(event) =
                                            Self::decode_entry_event(&msg, include_value)
                                        {
                                            handler(event);
                                        } else {
                                            listener_stats.record_error();
                                        }
                                    }
                                }
                                Ok(None) => {}
                                Err(_) => {
                                    listener_stats.record_error();
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(registration)
    }

    /// Removes an entry listener from this map.
    ///
    /// Returns `true` if the listener was successfully removed, `false` if it was not found.
    pub async fn remove_entry_listener(&self, registration: &ListenerRegistration) -> Result<bool> {
        registration.deactivate();

        let mut message =
            ClientMessage::create_for_encode(MAP_REMOVE_ENTRY_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::uuid_frame(registration.id().as_uuid()));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns the listener statistics for this map.
    pub fn listener_stats(&self) -> &ListenerStats {
        &self.listener_stats
    }
}

impl<K, V> Clone for IMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            listener_stats: Arc::clone(&self.listener_stats),
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_imap_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<IMap<String, String>>();
    }

    #[test]
    fn test_imap_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<IMap<String, String>>();
    }

    #[test]
    fn test_string_frame() {
        let frame = IMap::<String, String>::string_frame("test-map");
        assert_eq!(&frame.content[..], b"test-map");
    }

    #[test]
    fn test_long_frame() {
        let frame = IMap::<String, String>::long_frame(-1);
        assert_eq!(frame.content.len(), 8);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            -1
        );
    }

    #[test]
    fn test_serialize_string() {
        let data = IMap::<String, String>::serialize_value(&"hello".to_string()).unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_bool_frame() {
        let frame_true = IMap::<String, String>::bool_frame(true);
        assert_eq!(frame_true.content[0], 1);

        let frame_false = IMap::<String, String>::bool_frame(false);
        assert_eq!(frame_false.content[0], 0);
    }

    #[test]
    fn test_int_frame() {
        let frame = IMap::<String, String>::int_frame(42);
        assert_eq!(frame.content.len(), 4);
        assert_eq!(
            i32::from_le_bytes(frame.content[..4].try_into().unwrap()),
            42
        );
    }

    #[test]
    fn test_uuid_frame() {
        let uuid = Uuid::new_v4();
        let frame = IMap::<String, String>::uuid_frame(uuid);
        assert_eq!(frame.content.len(), 16);
        assert_eq!(
            Uuid::from_bytes(frame.content[..16].try_into().unwrap()),
            uuid
        );
    }
}
