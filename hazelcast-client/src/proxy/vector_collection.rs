//! Distributed vector collection proxy implementation.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{ClientMessage, Deserializable, HazelcastError, Result, Serializable};

use crate::config::PermissionAction;
use crate::connection::ConnectionManager;

/// A vector value representing dense floating-point vectors.
#[derive(Debug, Clone, PartialEq)]
pub enum VectorValue {
    /// A dense vector of f32 values.
    Dense(Vec<f32>),
}

/// A document stored in a vector collection, containing a value and associated vectors.
#[derive(Debug, Clone)]
pub struct VectorDocument<V> {
    /// The document value.
    pub value: V,
    /// Named vectors associated with this document.
    pub vectors: HashMap<String, VectorValue>,
}

impl<V> VectorDocument<V> {
    /// Creates a new vector document with the given value and vectors.
    pub fn new(value: V, vectors: HashMap<String, VectorValue>) -> Self {
        Self { value, vectors }
    }

    /// Creates a new vector document with a single unnamed dense vector.
    pub fn with_dense_vector(value: V, name: impl Into<String>, vector: Vec<f32>) -> Self {
        let mut vectors = HashMap::new();
        vectors.insert(name.into(), VectorValue::Dense(vector));
        Self { value, vectors }
    }
}

/// A search result from a vector similarity search.
#[derive(Debug, Clone)]
pub struct VectorSearchResult<K, V> {
    /// The key of the matching document.
    pub key: K,
    /// The value of the matching document.
    pub value: V,
    /// The similarity score.
    pub score: f32,
    /// The vectors associated with the matching document.
    pub vectors: HashMap<String, VectorValue>,
}

/// Distance metric for vector similarity search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorMetric {
    /// Cosine similarity metric.
    Cosine,
    /// Euclidean distance metric.
    Euclidean,
    /// Dot product metric.
    Dot,
}

impl VectorMetric {
    /// Returns the integer representation of this metric.
    pub fn as_i32(&self) -> i32 {
        match self {
            VectorMetric::Cosine => 0,
            VectorMetric::Euclidean => 1,
            VectorMetric::Dot => 2,
        }
    }
}

/// Configuration for a vector index within a vector collection.
#[derive(Debug, Clone)]
pub struct VectorIndexConfig {
    /// The name of this index.
    pub name: String,
    /// The distance metric to use for this index.
    pub metric: VectorMetric,
    /// The dimension of vectors in this index.
    pub dimension: usize,
    /// Maximum degree of the HNSW graph (number of bi-directional links per node).
    pub max_degree: Option<usize>,
    /// Size of the dynamic candidate list during index construction.
    pub ef_construction: Option<usize>,
}

impl VectorIndexConfig {
    /// Creates a new vector index configuration.
    pub fn new(name: impl Into<String>, metric: VectorMetric, dimension: usize) -> Self {
        Self {
            name: name.into(),
            metric,
            dimension,
            max_degree: None,
            ef_construction: None,
        }
    }

    /// Sets the maximum degree for the HNSW graph.
    pub fn with_max_degree(mut self, max_degree: usize) -> Self {
        self.max_degree = Some(max_degree);
        self
    }

    /// Sets the ef_construction parameter for the HNSW index.
    pub fn with_ef_construction(mut self, ef_construction: usize) -> Self {
        self.ef_construction = Some(ef_construction);
        self
    }
}

/// Configuration for a vector collection.
#[derive(Debug, Clone)]
pub struct VectorCollectionConfig {
    /// The name of this vector collection.
    pub name: String,
    /// The vector indexes configured for this collection.
    pub indexes: Vec<VectorIndexConfig>,
}

impl VectorCollectionConfig {
    /// Creates a new vector collection configuration.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            indexes: Vec::new(),
        }
    }

    /// Adds a vector index to this collection configuration.
    pub fn add_index(mut self, index: VectorIndexConfig) -> Self {
        self.indexes.push(index);
        self
    }
}

/// A distributed vector collection proxy for performing vector similarity operations
/// on a Hazelcast cluster.
///
/// `VectorCollection` supports storing documents with associated vectors and performing
/// similarity searches using configurable distance metrics.
#[derive(Debug)]
pub struct VectorCollection<K, V> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> VectorCollection<K, V> {
    /// Creates a new vector collection proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this vector collection.
    pub fn name(&self) -> &str {
        &self.name
    }

    fn check_permission(&self, action: PermissionAction) -> Result<()> {
        let permissions = self.connection_manager.effective_permissions();
        if !permissions.is_permitted(action) {
            return Err(HazelcastError::Authorization(format!(
                "vector collection '{}' operation denied: requires {:?} permission",
                self.name, action
            )));
        }
        Ok(())
    }
}

impl<K, V> VectorCollection<K, V>
where
    K: Serializable + Deserializable + Send + Sync,
    V: Serializable + Deserializable + Send + Sync,
{
    async fn check_quorum(&self, is_read: bool) -> Result<()> {
        self.connection_manager
            .check_quorum(&self.name, is_read)
            .await
    }

    /// Puts a document with associated vectors into the collection.
    ///
    /// If a document with the same key already exists, it will be replaced
    /// and the previous document's value is returned.
    pub async fn put(&self, key: K, document: VectorDocument<V>) -> Result<Option<V>> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&document.value)?;

        let mut message = ClientMessage::create_for_encode_any_partition(VECTOR_COLLECTION_PUT);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));
        Self::add_vectors_frame(&mut message, &document.vectors);

        let response = self.invoke(message).await?;
        Self::decode_optional_data_response(&response)
    }

    /// Gets the document associated with the given key.
    ///
    /// Returns `None` if no document exists for the key.
    pub async fn get(&self, key: &K) -> Result<Option<VectorDocument<V>>> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let key_data = Self::serialize_value(key)?;

        let mut message = ClientMessage::create_for_encode_any_partition(VECTOR_COLLECTION_GET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_optional_document_response(&response)
    }

    /// Removes the document associated with the given key.
    ///
    /// Returns the previous document value if one was present.
    pub async fn remove(&self, key: &K) -> Result<Option<V>> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(key)?;

        let mut message = ClientMessage::create_for_encode_any_partition(VECTOR_COLLECTION_REMOVE);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_optional_data_response(&response)
    }

    /// Searches for documents similar to the given vector.
    ///
    /// Returns up to `limit` results sorted by similarity score.
    pub async fn search_near_vector(
        &self,
        index_name: &str,
        vector: Vec<f32>,
        limit: usize,
    ) -> Result<Vec<VectorSearchResult<K, V>>> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;

        let mut message =
            ClientMessage::create_for_encode_any_partition(VECTOR_COLLECTION_SEARCH_NEAR_VECTOR);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::string_frame(index_name));
        Self::add_float_vector_frame(&mut message, &vector);
        message.add_frame(Self::int_frame(limit as i32));

        let response = self.invoke(message).await?;
        Self::decode_search_results(&response)
    }

    /// Puts multiple documents into the collection.
    pub async fn put_all(&self, entries: Vec<(K, VectorDocument<V>)>) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;

        let mut message =
            ClientMessage::create_for_encode_any_partition(VECTOR_COLLECTION_PUT_ALL);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(entries.len() as i32));

        for (key, document) in &entries {
            let key_data = Self::serialize_value(key)?;
            let value_data = Self::serialize_value(&document.value)?;
            message.add_frame(Self::data_frame(&key_data));
            message.add_frame(Self::data_frame(&value_data));
            Self::add_vectors_frame(&mut message, &document.vectors);
        }

        self.invoke(message).await?;
        Ok(())
    }

    /// Removes all documents from the collection.
    pub async fn clear(&self) -> Result<()> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;
        let mut message = ClientMessage::create_for_encode_any_partition(VECTOR_COLLECTION_CLEAR);
        message.add_frame(Self::string_frame(&self.name));

        self.invoke(message).await?;
        Ok(())
    }

    /// Returns the number of documents in the collection.
    pub async fn size(&self) -> Result<usize> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let mut message = ClientMessage::create_for_encode_any_partition(VECTOR_COLLECTION_SIZE);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_int_response(&response).map(|v| v as usize)
    }

    /// Deletes the document associated with the given key without returning the old value.
    pub async fn delete(&self, key: &K) -> Result<()> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(key)?;

        let mut message = ClientMessage::create_for_encode_any_partition(VECTOR_COLLECTION_DELETE);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        self.invoke(message).await?;
        Ok(())
    }

    /// Puts a document if no document exists for the given key.
    ///
    /// Returns the existing document value if one was already present.
    pub async fn put_if_absent(
        &self,
        key: K,
        document: VectorDocument<V>,
    ) -> Result<Option<V>> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&document.value)?;

        let mut message =
            ClientMessage::create_for_encode_any_partition(VECTOR_COLLECTION_PUT_IF_ABSENT);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));
        Self::add_vectors_frame(&mut message, &document.vectors);

        let response = self.invoke(message).await?;
        Self::decode_optional_data_response(&response)
    }

    /// Sets the document for the given key without returning the previous value.
    pub async fn set(&self, key: K, document: VectorDocument<V>) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&document.value)?;

        let mut message = ClientMessage::create_for_encode_any_partition(VECTOR_COLLECTION_SET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));
        Self::add_vectors_frame(&mut message, &document.vectors);

        self.invoke(message).await?;
        Ok(())
    }

    /// Triggers optimization of the vector indexes.
    ///
    /// This may improve search performance by reorganizing the internal index structures.
    pub async fn optimize(&self) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let mut message =
            ClientMessage::create_for_encode_any_partition(VECTOR_COLLECTION_OPTIMIZE);
        message.add_frame(Self::string_frame(&self.name));

        self.invoke(message).await?;
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

    fn int_frame(value: i32) -> Frame {
        let mut buf = BytesMut::with_capacity(4);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
    }

    fn add_vectors_frame(message: &mut ClientMessage, vectors: &HashMap<String, VectorValue>) {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&(vectors.len() as i32).to_le_bytes());

        for (name, vector_value) in vectors {
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as i32).to_le_bytes());
            buf.extend_from_slice(name_bytes);

            match vector_value {
                VectorValue::Dense(values) => {
                    // Type discriminator: 0 = Dense
                    buf.extend_from_slice(&0i32.to_le_bytes());
                    buf.extend_from_slice(&(values.len() as i32).to_le_bytes());
                    for v in values {
                        buf.extend_from_slice(&v.to_le_bytes());
                    }
                }
            }
        }

        message.add_frame(Frame::with_content(buf));
    }

    fn add_float_vector_frame(message: &mut ClientMessage, vector: &[f32]) {
        let mut buf = BytesMut::with_capacity(4 + vector.len() * 4);
        buf.extend_from_slice(&(vector.len() as i32).to_le_bytes());
        for v in vector {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        message.add_frame(Frame::with_content(buf));
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

    fn decode_optional_data_response(response: &ClientMessage) -> Result<Option<V>> {
        let frames = response.frames();
        if frames.len() < 2 {
            return Ok(None);
        }

        let data_frame = &frames[1];
        if data_frame.is_null_frame() || data_frame.content.is_empty() {
            return Ok(None);
        }

        let content_bytes = data_frame.content.to_vec();
        let mut input = ObjectDataInput::new(&content_bytes);
        let value = V::deserialize(&mut input)?;
        Ok(Some(value))
    }

    fn decode_optional_document_response(
        response: &ClientMessage,
    ) -> Result<Option<VectorDocument<V>>> {
        let frames = response.frames();
        if frames.len() < 2 {
            return Ok(None);
        }

        let data_frame = &frames[1];
        if data_frame.is_null_frame() || data_frame.content.is_empty() {
            return Ok(None);
        }

        let content_bytes = data_frame.content.to_vec();
        let mut input = ObjectDataInput::new(&content_bytes);
        let value = V::deserialize(&mut input)?;

        let vectors = if frames.len() > 2 {
            Self::decode_vectors_frame(&frames[2])?
        } else {
            HashMap::new()
        };

        Ok(Some(VectorDocument { value, vectors }))
    }

    fn decode_vectors_frame(frame: &Frame) -> Result<HashMap<String, VectorValue>> {
        let mut vectors = HashMap::new();
        let content = &frame.content;

        if content.len() < 4 {
            return Ok(vectors);
        }

        let mut offset = 0;
        let count = i32::from_le_bytes([
            content[offset],
            content[offset + 1],
            content[offset + 2],
            content[offset + 3],
        ]) as usize;
        offset += 4;

        for _ in 0..count {
            if offset + 4 > content.len() {
                break;
            }
            let name_len = i32::from_le_bytes([
                content[offset],
                content[offset + 1],
                content[offset + 2],
                content[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + name_len > content.len() {
                break;
            }
            let name = String::from_utf8_lossy(&content[offset..offset + name_len]).to_string();
            offset += name_len;

            if offset + 4 > content.len() {
                break;
            }
            let _type_discriminator = i32::from_le_bytes([
                content[offset],
                content[offset + 1],
                content[offset + 2],
                content[offset + 3],
            ]);
            offset += 4;

            if offset + 4 > content.len() {
                break;
            }
            let vec_len = i32::from_le_bytes([
                content[offset],
                content[offset + 1],
                content[offset + 2],
                content[offset + 3],
            ]) as usize;
            offset += 4;

            let mut values = Vec::with_capacity(vec_len);
            for _ in 0..vec_len {
                if offset + 4 > content.len() {
                    break;
                }
                let v = f32::from_le_bytes([
                    content[offset],
                    content[offset + 1],
                    content[offset + 2],
                    content[offset + 3],
                ]);
                offset += 4;
                values.push(v);
            }

            vectors.insert(name, VectorValue::Dense(values));
        }

        Ok(vectors)
    }

    fn decode_search_results(
        response: &ClientMessage,
    ) -> Result<Vec<VectorSearchResult<K, V>>> {
        let frames = response.frames();
        if frames.len() < 2 {
            return Ok(Vec::new());
        }

        let initial_frame = &frames[0];
        let result_count = if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 4 {
            let offset = RESPONSE_HEADER_SIZE;
            i32::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
            ]) as usize
        } else {
            0
        };

        let mut results = Vec::with_capacity(result_count);
        let mut frame_idx = 1;

        for _ in 0..result_count {
            if frame_idx + 3 >= frames.len() {
                break;
            }

            let key_frame = &frames[frame_idx];
            let value_frame = &frames[frame_idx + 1];
            let score_frame = &frames[frame_idx + 2];
            let vectors_frame = &frames[frame_idx + 3];
            frame_idx += 4;

            if key_frame.is_end_frame() {
                break;
            }

            let key_bytes = key_frame.content.to_vec();
            let mut key_input = ObjectDataInput::new(&key_bytes);
            let key = K::deserialize(&mut key_input)?;

            let value_bytes = value_frame.content.to_vec();
            let mut value_input = ObjectDataInput::new(&value_bytes);
            let value = V::deserialize(&mut value_input)?;

            let score = if score_frame.content.len() >= 4 {
                f32::from_le_bytes([
                    score_frame.content[0],
                    score_frame.content[1],
                    score_frame.content[2],
                    score_frame.content[3],
                ])
            } else {
                0.0
            };

            let vectors = Self::decode_vectors_frame(vectors_frame)?;

            results.push(VectorSearchResult {
                key,
                value,
                score,
                vectors,
            });
        }

        Ok(results)
    }
}

impl<K, V> Clone for VectorCollection<K, V> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_collection_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<VectorCollection<String, String>>();
    }

    #[test]
    fn test_vector_collection_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<VectorCollection<String, String>>();
    }

    #[test]
    fn test_vector_metric_as_i32() {
        assert_eq!(VectorMetric::Cosine.as_i32(), 0);
        assert_eq!(VectorMetric::Euclidean.as_i32(), 1);
        assert_eq!(VectorMetric::Dot.as_i32(), 2);
    }

    #[test]
    fn test_vector_index_config() {
        let config = VectorIndexConfig::new("test-index", VectorMetric::Cosine, 128)
            .with_max_degree(16)
            .with_ef_construction(200);

        assert_eq!(config.name, "test-index");
        assert_eq!(config.metric, VectorMetric::Cosine);
        assert_eq!(config.dimension, 128);
        assert_eq!(config.max_degree, Some(16));
        assert_eq!(config.ef_construction, Some(200));
    }

    #[test]
    fn test_vector_collection_config() {
        let config = VectorCollectionConfig::new("test-collection")
            .add_index(VectorIndexConfig::new("idx", VectorMetric::Dot, 64));

        assert_eq!(config.name, "test-collection");
        assert_eq!(config.indexes.len(), 1);
    }

    #[test]
    fn test_vector_document_new() {
        let mut vectors = HashMap::new();
        vectors.insert(
            "embedding".to_string(),
            VectorValue::Dense(vec![1.0, 2.0, 3.0]),
        );
        let doc = VectorDocument::new("test-value".to_string(), vectors);
        assert_eq!(doc.value, "test-value");
        assert_eq!(doc.vectors.len(), 1);
    }

    #[test]
    fn test_vector_document_with_dense_vector() {
        let doc =
            VectorDocument::with_dense_vector("value".to_string(), "emb", vec![0.1, 0.2, 0.3]);
        assert_eq!(doc.value, "value");
        assert!(doc.vectors.contains_key("emb"));
        match &doc.vectors["emb"] {
            VectorValue::Dense(v) => assert_eq!(v.len(), 3),
        }
    }

    #[test]
    fn test_vector_value_equality() {
        let v1 = VectorValue::Dense(vec![1.0, 2.0]);
        let v2 = VectorValue::Dense(vec![1.0, 2.0]);
        let v3 = VectorValue::Dense(vec![3.0, 4.0]);

        assert_eq!(v1, v2);
        assert_ne!(v1, v3);
    }

    #[test]
    fn test_string_frame() {
        let frame = VectorCollection::<String, String>::string_frame("test-collection");
        assert_eq!(&frame.content[..], b"test-collection");
    }
}
