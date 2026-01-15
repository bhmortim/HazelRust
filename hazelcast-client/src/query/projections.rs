//! Projections for extracting specific attributes from distributed data.
//!
//! This module provides projections for extracting specific attributes from
//! entries in distributed data structures like IMap, reducing data transfer
//! by only returning the requested fields.
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client::query::{Projections, Predicates};
//!
//! // Project a single attribute
//! let names: Vec<String> = map.project(&Projections::single("name")).await?;
//!
//! // Project multiple attributes with a predicate filter
//! let predicate = Predicates::greater_than("age", &18i32)?;
//! let results: Vec<Vec<Option<String>>> = map
//!     .project_with_predicate(&Projections::multi(["name", "email"]), &predicate)
//!     .await?;
//! ```

use std::fmt::Debug;
use std::marker::PhantomData;

use hazelcast_core::serialization::ObjectDataOutput;
use hazelcast_core::{Result, Serializable};

/// Factory ID for built-in Hazelcast projections.
pub const PROJECTION_FACTORY_ID: i32 = -30;

/// Class IDs for projection types in the Hazelcast protocol.
pub mod class_ids {
    /// Single attribute projection class ID.
    pub const SINGLE_ATTRIBUTE: i32 = 0;
    /// Multi attribute projection class ID.
    pub const MULTI_ATTRIBUTE: i32 = 1;
}

/// Trait for projections that extract specific attributes from map entries.
///
/// Projections are serialized using Hazelcast's IdentifiedDataSerializable format
/// with factory ID -30.
pub trait Projection: Debug + Send + Sync {
    /// The output type of this projection.
    type Output;

    /// Returns the class ID for this projection type.
    fn class_id(&self) -> i32;

    /// Writes the projection-specific data to the output.
    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()>;

    /// Serializes this projection to bytes including type information.
    fn to_projection_data(&self) -> Result<Vec<u8>> {
        let mut output = ObjectDataOutput::new();
        write_i32(&mut output, PROJECTION_FACTORY_ID)?;
        write_i32(&mut output, self.class_id())?;
        self.write_data(&mut output)?;
        Ok(output.into_bytes())
    }
}

/// Helper to write an i32 in big-endian format.
fn write_i32(output: &mut ObjectDataOutput, value: i32) -> Result<()> {
    for byte in value.to_be_bytes() {
        byte.serialize(output)?;
    }
    Ok(())
}

/// Helper to write a UTF string with length prefix.
fn write_string(output: &mut ObjectDataOutput, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    write_i32(output, bytes.len() as i32)?;
    for byte in bytes {
        (*byte as i8).serialize(output)?;
    }
    Ok(())
}

// ============================================================================
// Projection Implementations
// ============================================================================

/// A projection that extracts a single attribute from each entry.
///
/// # Example
///
/// ```ignore
/// let projection = SingleAttributeProjection::<String>::new("name");
/// let names: Vec<String> = map.project(&projection).await?;
/// ```
#[derive(Debug, Clone)]
pub struct SingleAttributeProjection<T> {
    attribute: String,
    _phantom: PhantomData<T>,
}

impl<T> SingleAttributeProjection<T> {
    /// Creates a new single attribute projection.
    ///
    /// # Arguments
    ///
    /// * `attribute` - The name of the attribute to extract. Can use dot notation
    ///   for nested attributes (e.g., `"address.city"`).
    pub fn new(attribute: impl Into<String>) -> Self {
        Self {
            attribute: attribute.into(),
            _phantom: PhantomData,
        }
    }

    /// Returns the attribute name this projection extracts.
    pub fn attribute(&self) -> &str {
        &self.attribute
    }
}

impl<T: Send + Sync + Debug> Projection for SingleAttributeProjection<T> {
    type Output = T;

    fn class_id(&self) -> i32 {
        class_ids::SINGLE_ATTRIBUTE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_string(output, &self.attribute)
    }
}

/// A projection that extracts multiple attributes from each entry.
///
/// Returns results as a vector of optional values, where each element
/// corresponds to an attribute in the order they were specified.
///
/// # Example
///
/// ```ignore
/// let projection = MultiAttributeProjection::<String>::new(["name", "email", "phone"]);
/// let results: Vec<Vec<Option<String>>> = map.project(&projection).await?;
/// for row in results {
///     let name = &row[0];
///     let email = &row[1];
///     let phone = &row[2];
/// }
/// ```
#[derive(Debug, Clone)]
pub struct MultiAttributeProjection<T> {
    attributes: Vec<String>,
    _phantom: PhantomData<T>,
}

impl<T> MultiAttributeProjection<T> {
    /// Creates a new multi-attribute projection.
    ///
    /// # Arguments
    ///
    /// * `attributes` - The names of the attributes to extract. Can use dot notation
    ///   for nested attributes (e.g., `"address.city"`).
    pub fn new<I, S>(attributes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            attributes: attributes.into_iter().map(Into::into).collect(),
            _phantom: PhantomData,
        }
    }

    /// Returns the attribute names this projection extracts.
    pub fn attributes(&self) -> &[String] {
        &self.attributes
    }
}

impl<T: Send + Sync + Debug> Projection for MultiAttributeProjection<T> {
    type Output = Vec<Option<T>>;

    fn class_id(&self) -> i32 {
        class_ids::MULTI_ATTRIBUTE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_i32(output, self.attributes.len() as i32)?;
        for attr in &self.attributes {
            write_string(output, attr)?;
        }
        Ok(())
    }
}

// ============================================================================
// Projections Factory
// ============================================================================

/// Factory for creating projections.
///
/// Provides convenient static methods for creating all projection types.
pub struct Projections;

impl Projections {
    /// Returns a projection that extracts a single attribute.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The expected type of the attribute value.
    ///
    /// # Arguments
    ///
    /// * `attribute` - The name of the attribute to extract.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let projection = Projections::single::<String>("name");
    /// ```
    pub fn single<T>(attribute: impl Into<String>) -> SingleAttributeProjection<T> {
        SingleAttributeProjection::new(attribute)
    }

    /// Returns a projection that extracts multiple attributes.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The expected type of each attribute value.
    ///
    /// # Arguments
    ///
    /// * `attributes` - The names of the attributes to extract.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let projection = Projections::multi::<String>(["name", "email"]);
    /// ```
    pub fn multi<T, I, S>(attributes: I) -> MultiAttributeProjection<T>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        MultiAttributeProjection::new(attributes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_attribute_projection() {
        let proj: SingleAttributeProjection<String> = SingleAttributeProjection::new("name");
        assert_eq!(proj.class_id(), class_ids::SINGLE_ATTRIBUTE);
        assert_eq!(proj.attribute(), "name");

        let data = proj.to_projection_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_single_attribute_projection_nested() {
        let proj: SingleAttributeProjection<String> = SingleAttributeProjection::new("address.city");
        assert_eq!(proj.attribute(), "address.city");

        let data = proj.to_projection_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_multi_attribute_projection() {
        let proj: MultiAttributeProjection<String> =
            MultiAttributeProjection::new(["name", "email", "age"]);
        assert_eq!(proj.class_id(), class_ids::MULTI_ATTRIBUTE);
        assert_eq!(proj.attributes().len(), 3);
        assert_eq!(proj.attributes()[0], "name");
        assert_eq!(proj.attributes()[1], "email");
        assert_eq!(proj.attributes()[2], "age");

        let data = proj.to_projection_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_multi_attribute_projection_empty() {
        let proj: MultiAttributeProjection<String> = MultiAttributeProjection::new(Vec::<String>::new());
        assert_eq!(proj.attributes().len(), 0);

        let data = proj.to_projection_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_projections_factory_single() {
        let proj: SingleAttributeProjection<i64> = Projections::single("value");
        assert_eq!(proj.attribute(), "value");
        assert_eq!(proj.class_id(), class_ids::SINGLE_ATTRIBUTE);
    }

    #[test]
    fn test_projections_factory_multi() {
        let proj: MultiAttributeProjection<String> = Projections::multi(["a", "b", "c"]);
        assert_eq!(proj.attributes().len(), 3);
        assert_eq!(proj.class_id(), class_ids::MULTI_ATTRIBUTE);
    }

    #[test]
    fn test_projection_data_format() {
        let proj: SingleAttributeProjection<String> = SingleAttributeProjection::new("test");
        let data = proj.to_projection_data().unwrap();

        assert!(data.len() >= 8);
        let factory_id = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        assert_eq!(factory_id, PROJECTION_FACTORY_ID);

        let class_id = i32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        assert_eq!(class_id, class_ids::SINGLE_ATTRIBUTE);
    }

    #[test]
    fn test_multi_projection_data_format() {
        let proj: MultiAttributeProjection<String> = MultiAttributeProjection::new(["field1", "field2"]);
        let data = proj.to_projection_data().unwrap();

        assert!(data.len() > 8);
        let factory_id = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        assert_eq!(factory_id, PROJECTION_FACTORY_ID);

        let class_id = i32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        assert_eq!(class_id, class_ids::MULTI_ATTRIBUTE);

        let count = i32::from_be_bytes([data[8], data[9], data[10], data[11]]);
        assert_eq!(count, 2);
    }

    #[test]
    fn test_projection_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SingleAttributeProjection<String>>();
        assert_send_sync::<MultiAttributeProjection<String>>();
        assert_send_sync::<SingleAttributeProjection<i64>>();
        assert_send_sync::<MultiAttributeProjection<i64>>();
    }

    #[test]
    fn test_projection_clone() {
        let single: SingleAttributeProjection<String> = SingleAttributeProjection::new("attr");
        let cloned = single.clone();
        assert_eq!(single.attribute(), cloned.attribute());
        assert_eq!(single.class_id(), cloned.class_id());

        let multi: MultiAttributeProjection<String> = MultiAttributeProjection::new(["a", "b"]);
        let cloned = multi.clone();
        assert_eq!(multi.attributes(), cloned.attributes());
        assert_eq!(multi.class_id(), cloned.class_id());
    }

    #[test]
    fn test_projection_debug() {
        let single: SingleAttributeProjection<String> = SingleAttributeProjection::new("name");
        let debug_str = format!("{:?}", single);
        assert!(debug_str.contains("SingleAttributeProjection"));
        assert!(debug_str.contains("name"));

        let multi: MultiAttributeProjection<String> = MultiAttributeProjection::new(["x", "y"]);
        let debug_str = format!("{:?}", multi);
        assert!(debug_str.contains("MultiAttributeProjection"));
    }

    #[test]
    fn test_projection_with_vec_input() {
        let attrs = vec!["a".to_string(), "b".to_string()];
        let proj: MultiAttributeProjection<String> = MultiAttributeProjection::new(attrs);
        assert_eq!(proj.attributes().len(), 2);
    }

    #[test]
    fn test_projection_with_different_types() {
        let _string_proj: SingleAttributeProjection<String> = Projections::single("name");
        let _i32_proj: SingleAttributeProjection<i32> = Projections::single("count");
        let _i64_proj: SingleAttributeProjection<i64> = Projections::single("id");
        let _f64_proj: SingleAttributeProjection<f64> = Projections::single("price");
        let _bool_proj: SingleAttributeProjection<bool> = Projections::single("active");
    }
}
