//! Identified data serializable support for Hazelcast serialization.

use crate::error::Result;
use std::collections::HashMap;

use super::{DataInput, DataOutput};

/// Type ID for identified data serializable format.
pub const IDENTIFIED_DATA_SERIALIZABLE_TYPE_ID: i32 = -2;

/// Trait for types that can be serialized using Hazelcast's identified data serializable format.
///
/// Types implementing this trait are identified by a factory ID and class ID combination,
/// which allows efficient type lookup during deserialization.
pub trait IdentifiedDataSerializable: Send + Sync {
    /// Returns the factory ID for this type.
    fn factory_id(&self) -> i32;

    /// Returns the class ID for this type within its factory.
    fn class_id(&self) -> i32;

    /// Writes the object's data to the output.
    fn write_data(&self, output: &mut dyn DataOutput) -> Result<()>;

    /// Reads the object's data from the input, populating this instance.
    fn read_data(&mut self, input: &mut dyn DataInput) -> Result<()>;
}

/// Factory for creating instances of `IdentifiedDataSerializable` types.
///
/// Each factory is responsible for creating instances of types that share the same factory ID.
pub trait DataSerializableFactory: Send + Sync {
    /// Creates a default/empty instance of the type with the given class ID.
    ///
    /// Returns `None` if the class ID is not recognized by this factory.
    fn create(&self, class_id: i32) -> Option<Box<dyn IdentifiedDataSerializable>>;
}

/// Registry for `DataSerializableFactory` instances.
///
/// This registry maps factory IDs to their corresponding factories, enabling
/// type lookup during deserialization.
#[derive(Default)]
pub struct FactoryRegistry {
    factories: HashMap<i32, Box<dyn DataSerializableFactory>>,
}

impl FactoryRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    /// Registers a factory with its factory ID.
    ///
    /// If a factory with the same ID was previously registered, it is replaced.
    pub fn register(&mut self, factory_id: i32, factory: Box<dyn DataSerializableFactory>) {
        self.factories.insert(factory_id, factory);
    }

    /// Removes a factory by its factory ID.
    ///
    /// Returns the removed factory if it was present.
    pub fn unregister(&mut self, factory_id: i32) -> Option<Box<dyn DataSerializableFactory>> {
        self.factories.remove(&factory_id)
    }

    /// Returns the factory for the given factory ID, if registered.
    pub fn get(&self, factory_id: i32) -> Option<&dyn DataSerializableFactory> {
        self.factories.get(&factory_id).map(|f| f.as_ref())
    }

    /// Creates an instance using the registered factory for the given factory ID and class ID.
    ///
    /// Returns `None` if no factory is registered for the factory ID, or if the factory
    /// does not recognize the class ID.
    pub fn create(
        &self,
        factory_id: i32,
        class_id: i32,
    ) -> Option<Box<dyn IdentifiedDataSerializable>> {
        self.factories.get(&factory_id)?.create(class_id)
    }

    /// Returns `true` if a factory is registered for the given factory ID.
    pub fn contains(&self, factory_id: i32) -> bool {
        self.factories.contains_key(&factory_id)
    }

    /// Returns the number of registered factories.
    pub fn len(&self) -> usize {
        self.factories.len()
    }

    /// Returns `true` if no factories are registered.
    pub fn is_empty(&self) -> bool {
        self.factories.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialization::{ObjectDataInput, ObjectDataOutput};

    const TEST_FACTORY_ID: i32 = 1000;
    const TEST_CLASS_ID: i32 = 1;

    #[derive(Debug, Default, PartialEq)]
    struct TestData {
        value: i32,
        name: String,
    }

    impl IdentifiedDataSerializable for TestData {
        fn factory_id(&self) -> i32 {
            TEST_FACTORY_ID
        }

        fn class_id(&self) -> i32 {
            TEST_CLASS_ID
        }

        fn write_data(&self, output: &mut dyn DataOutput) -> Result<()> {
            output.write_int(self.value)?;
            output.write_string(&self.name)
        }

        fn read_data(&mut self, input: &mut dyn DataInput) -> Result<()> {
            self.value = input.read_int()?;
            self.name = input.read_string()?;
            Ok(())
        }
    }

    struct TestFactory;

    impl DataSerializableFactory for TestFactory {
        fn create(&self, class_id: i32) -> Option<Box<dyn IdentifiedDataSerializable>> {
            match class_id {
                TEST_CLASS_ID => Some(Box::new(TestData::default())),
                _ => None,
            }
        }
    }

    #[test]
    fn test_type_id_constant() {
        assert_eq!(IDENTIFIED_DATA_SERIALIZABLE_TYPE_ID, -2);
    }

    #[test]
    fn test_identified_data_serializable_ids() {
        let data = TestData {
            value: 42,
            name: String::from("test"),
        };
        assert_eq!(data.factory_id(), TEST_FACTORY_ID);
        assert_eq!(data.class_id(), TEST_CLASS_ID);
    }

    #[test]
    fn test_write_and_read_data() {
        let original = TestData {
            value: 42,
            name: String::from("hello"),
        };

        let mut output = ObjectDataOutput::new();
        original.write_data(&mut output).unwrap();

        let bytes = output.as_bytes();
        let mut input = ObjectDataInput::new(bytes);

        let mut restored = TestData::default();
        restored.read_data(&mut input).unwrap();

        assert_eq!(original, restored);
    }

    #[test]
    fn test_factory_registry_new() {
        let registry = FactoryRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_factory_registry_default() {
        let registry = FactoryRegistry::default();
        assert!(registry.is_empty());
    }

    #[test]
    fn test_factory_registry_register() {
        let mut registry = FactoryRegistry::new();
        registry.register(TEST_FACTORY_ID, Box::new(TestFactory));

        assert!(!registry.is_empty());
        assert_eq!(registry.len(), 1);
        assert!(registry.contains(TEST_FACTORY_ID));
    }

    #[test]
    fn test_factory_registry_get() {
        let mut registry = FactoryRegistry::new();
        registry.register(TEST_FACTORY_ID, Box::new(TestFactory));

        assert!(registry.get(TEST_FACTORY_ID).is_some());
        assert!(registry.get(9999).is_none());
    }

    #[test]
    fn test_factory_registry_create() {
        let mut registry = FactoryRegistry::new();
        registry.register(TEST_FACTORY_ID, Box::new(TestFactory));

        let instance = registry.create(TEST_FACTORY_ID, TEST_CLASS_ID);
        assert!(instance.is_some());

        let instance = instance.unwrap();
        assert_eq!(instance.factory_id(), TEST_FACTORY_ID);
        assert_eq!(instance.class_id(), TEST_CLASS_ID);
    }

    #[test]
    fn test_factory_registry_create_unknown_factory() {
        let registry = FactoryRegistry::new();
        assert!(registry.create(TEST_FACTORY_ID, TEST_CLASS_ID).is_none());
    }

    #[test]
    fn test_factory_registry_create_unknown_class() {
        let mut registry = FactoryRegistry::new();
        registry.register(TEST_FACTORY_ID, Box::new(TestFactory));

        assert!(registry.create(TEST_FACTORY_ID, 9999).is_none());
    }

    #[test]
    fn test_factory_registry_unregister() {
        let mut registry = FactoryRegistry::new();
        registry.register(TEST_FACTORY_ID, Box::new(TestFactory));

        let removed = registry.unregister(TEST_FACTORY_ID);
        assert!(removed.is_some());
        assert!(registry.is_empty());
        assert!(!registry.contains(TEST_FACTORY_ID));
    }

    #[test]
    fn test_factory_registry_unregister_nonexistent() {
        let mut registry = FactoryRegistry::new();
        assert!(registry.unregister(TEST_FACTORY_ID).is_none());
    }

    #[test]
    fn test_factory_registry_replace() {
        let mut registry = FactoryRegistry::new();
        registry.register(TEST_FACTORY_ID, Box::new(TestFactory));
        registry.register(TEST_FACTORY_ID, Box::new(TestFactory));

        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_round_trip_via_registry() {
        let mut registry = FactoryRegistry::new();
        registry.register(TEST_FACTORY_ID, Box::new(TestFactory));

        let original = TestData {
            value: 123,
            name: String::from("round trip"),
        };

        let mut output = ObjectDataOutput::new();
        output.write_int(original.factory_id()).unwrap();
        output.write_int(original.class_id()).unwrap();
        original.write_data(&mut output).unwrap();

        let bytes = output.as_bytes();
        let mut input = ObjectDataInput::new(bytes);

        let factory_id = input.read_int().unwrap();
        let class_id = input.read_int().unwrap();

        let mut instance = registry.create(factory_id, class_id).unwrap();
        instance.read_data(&mut input).unwrap();

        assert_eq!(instance.factory_id(), original.factory_id());
        assert_eq!(instance.class_id(), original.class_id());
    }
}
