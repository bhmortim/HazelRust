//! Index configuration types for IMap indexing.
//!
//! This module provides types for configuring bitmap indexes and other
//! advanced indexing options.

use std::fmt::Debug;

/// Transformation to apply to unique key values in a bitmap index.
///
/// Determines how attribute values are transformed before being used
/// as unique keys in the bitmap index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(i32)]
pub enum UniqueKeyTransformation {
    /// Use the object's identity as the unique key.
    ///
    /// This is the default transformation. The object itself (or its
    /// serialized form) is used as the unique key.
    #[default]
    Object = 0,

    /// Extract a long value from the object to use as the unique key.
    ///
    /// The attribute value must be convertible to a 64-bit integer.
    Long = 1,

    /// Use the raw serialized bytes as the unique key.
    ///
    /// No transformation is applied; the serialized bytes are used directly.
    Raw = 2,
}

impl UniqueKeyTransformation {
    /// Returns the integer value of this transformation.
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    /// Creates a transformation from its integer value.
    ///
    /// Returns `None` if the value doesn't correspond to a valid transformation.
    pub fn from_value(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Object),
            1 => Some(Self::Long),
            2 => Some(Self::Raw),
            _ => None,
        }
    }
}

/// Configuration options for bitmap indexes.
///
/// Bitmap indexes are optimized for attributes with low cardinality
/// (few distinct values) and support efficient equality and IN queries.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::query::BitmapIndexOptions;
///
/// let options = BitmapIndexOptions::builder()
///     .unique_key("__key")
///     .unique_key_transformation(UniqueKeyTransformation::Long)
///     .build();
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmapIndexOptions {
    unique_key: String,
    unique_key_transformation: UniqueKeyTransformation,
}

impl Default for BitmapIndexOptions {
    fn default() -> Self {
        Self {
            unique_key: "__key".to_string(),
            unique_key_transformation: UniqueKeyTransformation::Object,
        }
    }
}

impl BitmapIndexOptions {
    /// Creates a new builder for `BitmapIndexOptions`.
    pub fn builder() -> BitmapIndexOptionsBuilder {
        BitmapIndexOptionsBuilder::new()
    }

    /// Creates default bitmap index options.
    ///
    /// Uses `__key` as the unique key with `Object` transformation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the unique key attribute name.
    ///
    /// The unique key identifies each entry uniquely in the bitmap index.
    /// Defaults to `__key` which represents the map entry's key.
    pub fn unique_key(&self) -> &str {
        &self.unique_key
    }

    /// Returns the transformation applied to the unique key.
    pub fn unique_key_transformation(&self) -> UniqueKeyTransformation {
        self.unique_key_transformation
    }
}

/// Builder for creating [`BitmapIndexOptions`] instances.
#[derive(Debug, Clone)]
pub struct BitmapIndexOptionsBuilder {
    unique_key: String,
    unique_key_transformation: UniqueKeyTransformation,
}

impl BitmapIndexOptionsBuilder {
    /// Creates a new builder with default values.
    fn new() -> Self {
        Self {
            unique_key: "__key".to_string(),
            unique_key_transformation: UniqueKeyTransformation::Object,
        }
    }

    /// Sets the unique key attribute name.
    ///
    /// The unique key identifies each entry uniquely in the bitmap index.
    /// Use `__key` for the map entry's key or specify a custom attribute.
    ///
    /// # Arguments
    ///
    /// * `key` - The attribute name to use as the unique key
    ///
    /// # Example
    ///
    /// ```ignore
    /// let options = BitmapIndexOptions::builder()
    ///     .unique_key("userId")
    ///     .build();
    /// ```
    pub fn unique_key(mut self, key: impl Into<String>) -> Self {
        self.unique_key = key.into();
        self
    }

    /// Sets the transformation to apply to the unique key.
    ///
    /// # Arguments
    ///
    /// * `transformation` - The transformation type
    ///
    /// # Example
    ///
    /// ```ignore
    /// let options = BitmapIndexOptions::builder()
    ///     .unique_key_transformation(UniqueKeyTransformation::Long)
    ///     .build();
    /// ```
    pub fn unique_key_transformation(mut self, transformation: UniqueKeyTransformation) -> Self {
        self.unique_key_transformation = transformation;
        self
    }

    /// Builds the [`BitmapIndexOptions`].
    pub fn build(self) -> BitmapIndexOptions {
        BitmapIndexOptions {
            unique_key: self.unique_key,
            unique_key_transformation: self.unique_key_transformation,
        }
    }
}

impl Default for BitmapIndexOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unique_key_transformation_values() {
        assert_eq!(UniqueKeyTransformation::Object.as_i32(), 0);
        assert_eq!(UniqueKeyTransformation::Long.as_i32(), 1);
        assert_eq!(UniqueKeyTransformation::Raw.as_i32(), 2);
    }

    #[test]
    fn test_unique_key_transformation_from_value() {
        assert_eq!(
            UniqueKeyTransformation::from_value(0),
            Some(UniqueKeyTransformation::Object)
        );
        assert_eq!(
            UniqueKeyTransformation::from_value(1),
            Some(UniqueKeyTransformation::Long)
        );
        assert_eq!(
            UniqueKeyTransformation::from_value(2),
            Some(UniqueKeyTransformation::Raw)
        );
        assert_eq!(UniqueKeyTransformation::from_value(3), None);
        assert_eq!(UniqueKeyTransformation::from_value(-1), None);
    }

    #[test]
    fn test_unique_key_transformation_default() {
        let default = UniqueKeyTransformation::default();
        assert_eq!(default, UniqueKeyTransformation::Object);
    }

    #[test]
    fn test_bitmap_index_options_default() {
        let options = BitmapIndexOptions::default();
        assert_eq!(options.unique_key(), "__key");
        assert_eq!(
            options.unique_key_transformation(),
            UniqueKeyTransformation::Object
        );
    }

    #[test]
    fn test_bitmap_index_options_new() {
        let options = BitmapIndexOptions::new();
        assert_eq!(options.unique_key(), "__key");
        assert_eq!(
            options.unique_key_transformation(),
            UniqueKeyTransformation::Object
        );
    }

    #[test]
    fn test_bitmap_index_options_builder() {
        let options = BitmapIndexOptions::builder()
            .unique_key("customKey")
            .unique_key_transformation(UniqueKeyTransformation::Long)
            .build();

        assert_eq!(options.unique_key(), "customKey");
        assert_eq!(
            options.unique_key_transformation(),
            UniqueKeyTransformation::Long
        );
    }

    #[test]
    fn test_bitmap_index_options_builder_default_values() {
        let options = BitmapIndexOptions::builder().build();
        assert_eq!(options.unique_key(), "__key");
        assert_eq!(
            options.unique_key_transformation(),
            UniqueKeyTransformation::Object
        );
    }

    #[test]
    fn test_bitmap_index_options_builder_chaining() {
        let options = BitmapIndexOptions::builder()
            .unique_key("id")
            .unique_key_transformation(UniqueKeyTransformation::Raw)
            .unique_key("finalKey")
            .build();

        assert_eq!(options.unique_key(), "finalKey");
        assert_eq!(
            options.unique_key_transformation(),
            UniqueKeyTransformation::Raw
        );
    }

    #[test]
    fn test_bitmap_index_options_clone() {
        let options = BitmapIndexOptions::builder()
            .unique_key("test")
            .unique_key_transformation(UniqueKeyTransformation::Long)
            .build();

        let cloned = options.clone();
        assert_eq!(options, cloned);
    }

    #[test]
    fn test_bitmap_index_options_debug() {
        let options = BitmapIndexOptions::new();
        let debug_str = format!("{:?}", options);
        assert!(debug_str.contains("BitmapIndexOptions"));
        assert!(debug_str.contains("unique_key"));
    }

    #[test]
    fn test_unique_key_transformation_copy() {
        let t1 = UniqueKeyTransformation::Long;
        let t2 = t1;
        assert_eq!(t1, t2);
    }

    #[test]
    fn test_unique_key_transformation_equality() {
        assert_eq!(UniqueKeyTransformation::Object, UniqueKeyTransformation::Object);
        assert_ne!(UniqueKeyTransformation::Object, UniqueKeyTransformation::Long);
        assert_ne!(UniqueKeyTransformation::Long, UniqueKeyTransformation::Raw);
    }
}
