//! Portable serialization framework integration.

use super::{
    ClassDefinition, DefaultPortableReader, DefaultPortableWriter, Portable, PortableFactory,
};
use crate::error::{HazelcastError, Result};
use crate::serialization::{DataInput, DataOutput, ObjectDataInput, ObjectDataOutput};
use std::collections::HashMap;
use std::sync::Arc;

/// Type identifier for Portable serialization.
pub const PORTABLE_TYPE_ID: i32 = -1;

/// Serializer for Portable objects.
#[derive(Default)]
pub struct PortableSerializer {
    factories: HashMap<i32, Arc<dyn PortableFactory>>,
    class_definitions: HashMap<(i32, i32, i32), ClassDefinition>,
}

impl std::fmt::Debug for PortableSerializer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortableSerializer")
            .field("class_definitions", &self.class_definitions)
            .finish()
    }
}

impl PortableSerializer {
    /// Creates a new PortableSerializer.
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
            class_definitions: HashMap::new(),
        }
    }

    /// Registers a factory for creating Portable instances.
    pub fn register_factory(&mut self, factory: Arc<dyn PortableFactory>) {
        let factory_id = factory.factory_id();
        self.factories.insert(factory_id, factory);
    }

    /// Registers a class definition.
    pub fn register_class_definition(&mut self, class_def: ClassDefinition) {
        let key = (
            class_def.factory_id(),
            class_def.class_id(),
            class_def.version(),
        );
        self.class_definitions.insert(key, class_def);
    }

    /// Looks up a class definition by its identifiers.
    pub fn get_class_definition(
        &self,
        factory_id: i32,
        class_id: i32,
        version: i32,
    ) -> Option<&ClassDefinition> {
        self.class_definitions
            .get(&(factory_id, class_id, version))
    }

    /// Serializes a Portable object to bytes.
    pub fn serialize<P: Portable>(&self, portable: &P) -> Result<Vec<u8>> {
        let factory_id = portable.factory_id();
        let class_id = portable.class_id();

        let class_def = self
            .class_definitions
            .values()
            .find(|cd| cd.factory_id() == factory_id && cd.class_id() == class_id)
            .cloned()
            .unwrap_or_else(|| ClassDefinition::new(factory_id, class_id, 0));

        let mut writer = DefaultPortableWriter::new(class_def.clone());
        portable.write_portable(&mut writer)?;
        let field_data = writer.to_bytes();

        let mut output = ObjectDataOutput::new();
        output.write_int(factory_id)?;
        output.write_int(class_id)?;
        output.write_int(class_def.version())?;
        output.write_int(field_data.len() as i32)?;
        output.write_bytes(&field_data)?;

        Ok(output.into_bytes())
    }

    /// Deserializes bytes into a Portable object.
    pub fn deserialize(&self, data: &[u8]) -> Result<Box<dyn Portable>> {
        let mut input = ObjectDataInput::new(data);

        let factory_id = input.read_int()?;
        let class_id = input.read_int()?;
        let version = input.read_int()?;
        let field_data_len = input.read_int()? as usize;
        let field_data = input.read_bytes(field_data_len)?;

        let factory = self.factories.get(&factory_id).ok_or_else(|| {
            HazelcastError::Serialization(format!(
                "No factory registered for factory_id={}",
                factory_id
            ))
        })?;

        let mut instance = factory.create(class_id).ok_or_else(|| {
            HazelcastError::Serialization(format!(
                "Factory {} cannot create class_id={}",
                factory_id, class_id
            ))
        })?;

        let mut reader =
            DefaultPortableReader::from_bytes(&field_data, version, self.factories.clone())?;
        instance.read_portable(&mut reader)?;

        Ok(instance)
    }

    /// Deserializes bytes into a specific Portable type.
    pub fn deserialize_typed<P: Portable + Default>(&self, data: &[u8]) -> Result<P> {
        let mut input = ObjectDataInput::new(data);

        let factory_id = input.read_int()?;
        let class_id = input.read_int()?;
        let version = input.read_int()?;
        let field_data_len = input.read_int()? as usize;
        let field_data = input.read_bytes(field_data_len)?;

        let mut instance = P::default();

        if instance.factory_id() != factory_id || instance.class_id() != class_id {
            return Err(HazelcastError::Serialization(format!(
                "Type mismatch: expected factory_id={}, class_id={}, got factory_id={}, class_id={}",
                instance.factory_id(),
                instance.class_id(),
                factory_id,
                class_id
            )));
        }

        let mut reader =
            DefaultPortableReader::from_bytes(&field_data, version, self.factories.clone())?;
        instance.read_portable(&mut reader)?;

        Ok(instance)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialization::portable::{
        FieldDefinition, FieldType, PortableReader, PortableWriter,
    };

    const TEST_FACTORY_ID: i32 = 1;
    const PERSON_CLASS_ID: i32 = 1;
    const ADDRESS_CLASS_ID: i32 = 2;

    #[derive(Debug, Default, PartialEq)]
    struct Person {
        name: String,
        age: i32,
        active: bool,
    }

    impl Portable for Person {
        fn factory_id(&self) -> i32 {
            TEST_FACTORY_ID
        }

        fn class_id(&self) -> i32 {
            PERSON_CLASS_ID
        }

        fn write_portable(&self, writer: &mut DefaultPortableWriter) -> Result<()> {
            writer.write_string("name", Some(&self.name))?;
            writer.write_int("age", self.age)?;
            writer.write_bool("active", self.active)?;
            Ok(())
        }

        fn read_portable(&mut self, reader: &mut DefaultPortableReader) -> Result<()> {
            self.name = reader.read_string("name")?.unwrap_or_default();
            self.age = reader.read_int("age")?;
            self.active = reader.read_bool("active")?;
            Ok(())
        }
    }

    #[derive(Debug, Default, PartialEq)]
    struct Address {
        street: String,
        city: String,
        zip: i32,
    }

    impl Portable for Address {
        fn factory_id(&self) -> i32 {
            TEST_FACTORY_ID
        }

        fn class_id(&self) -> i32 {
            ADDRESS_CLASS_ID
        }

        fn write_portable(&self, writer: &mut DefaultPortableWriter) -> Result<()> {
            writer.write_string("street", Some(&self.street))?;
            writer.write_string("city", Some(&self.city))?;
            writer.write_int("zip", self.zip)?;
            Ok(())
        }

        fn read_portable(&mut self, reader: &mut DefaultPortableReader) -> Result<()> {
            self.street = reader.read_string("street")?.unwrap_or_default();
            self.city = reader.read_string("city")?.unwrap_or_default();
            self.zip = reader.read_int("zip")?;
            Ok(())
        }
    }

    struct TestFactory;

    impl PortableFactory for TestFactory {
        fn factory_id(&self) -> i32 {
            TEST_FACTORY_ID
        }

        fn create(&self, class_id: i32) -> Option<Box<dyn Portable>> {
            match class_id {
                PERSON_CLASS_ID => Some(Box::new(Person::default())),
                ADDRESS_CLASS_ID => Some(Box::new(Address::default())),
                _ => None,
            }
        }
    }

    fn create_person_class_def() -> ClassDefinition {
        ClassDefinition::with_fields(
            TEST_FACTORY_ID,
            PERSON_CLASS_ID,
            1,
            vec![
                FieldDefinition::new("name", FieldType::Utf8, 0),
                FieldDefinition::new("age", FieldType::Int, 1),
                FieldDefinition::new("active", FieldType::Bool, 2),
            ],
        )
    }

    fn create_address_class_def() -> ClassDefinition {
        ClassDefinition::with_fields(
            TEST_FACTORY_ID,
            ADDRESS_CLASS_ID,
            1,
            vec![
                FieldDefinition::new("street", FieldType::Utf8, 0),
                FieldDefinition::new("city", FieldType::Utf8, 1),
                FieldDefinition::new("zip", FieldType::Int, 2),
            ],
        )
    }

    #[test]
    fn test_serialize_deserialize_round_trip() {
        let mut serializer = PortableSerializer::new();
        serializer.register_factory(Arc::new(TestFactory));
        serializer.register_class_definition(create_person_class_def());

        let person = Person {
            name: "Alice".to_string(),
            age: 30,
            active: true,
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result: Person = serializer.deserialize_typed(&bytes).unwrap();

        assert_eq!(result, person);
    }

    #[test]
    fn test_serialize_deserialize_dynamic() {
        let mut serializer = PortableSerializer::new();
        serializer.register_factory(Arc::new(TestFactory));
        serializer.register_class_definition(create_person_class_def());

        let person = Person {
            name: "Bob".to_string(),
            age: 25,
            active: false,
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result = serializer.deserialize(&bytes).unwrap();

        assert_eq!(result.factory_id(), TEST_FACTORY_ID);
        assert_eq!(result.class_id(), PERSON_CLASS_ID);
    }

    #[test]
    fn test_multiple_types() {
        let mut serializer = PortableSerializer::new();
        serializer.register_factory(Arc::new(TestFactory));
        serializer.register_class_definition(create_person_class_def());
        serializer.register_class_definition(create_address_class_def());

        let person = Person {
            name: "Charlie".to_string(),
            age: 35,
            active: true,
        };

        let address = Address {
            street: "123 Main St".to_string(),
            city: "Springfield".to_string(),
            zip: 12345,
        };

        let person_bytes = serializer.serialize(&person).unwrap();
        let address_bytes = serializer.serialize(&address).unwrap();

        let person_result: Person = serializer.deserialize_typed(&person_bytes).unwrap();
        let address_result: Address = serializer.deserialize_typed(&address_bytes).unwrap();

        assert_eq!(person_result, person);
        assert_eq!(address_result, address);
    }

    #[test]
    fn test_missing_factory() {
        let serializer = PortableSerializer::new();

        let person = Person {
            name: "Test".to_string(),
            age: 20,
            active: true,
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result = serializer.deserialize(&bytes);

        assert!(result.is_err());
    }

    #[test]
    fn test_type_mismatch() {
        let mut serializer = PortableSerializer::new();
        serializer.register_factory(Arc::new(TestFactory));
        serializer.register_class_definition(create_person_class_def());
        serializer.register_class_definition(create_address_class_def());

        let person = Person {
            name: "Test".to_string(),
            age: 20,
            active: true,
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result: Result<Address> = serializer.deserialize_typed(&bytes);

        assert!(result.is_err());
    }

    #[test]
    fn test_empty_string_fields() {
        let mut serializer = PortableSerializer::new();
        serializer.register_factory(Arc::new(TestFactory));
        serializer.register_class_definition(create_person_class_def());

        let person = Person {
            name: String::new(),
            age: 0,
            active: false,
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result: Person = serializer.deserialize_typed(&bytes).unwrap();

        assert_eq!(result, person);
    }

    #[test]
    fn test_special_characters_in_string() {
        let mut serializer = PortableSerializer::new();
        serializer.register_factory(Arc::new(TestFactory));
        serializer.register_class_definition(create_person_class_def());

        let person = Person {
            name: "Hello 世界 🚀".to_string(),
            age: 42,
            active: true,
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result: Person = serializer.deserialize_typed(&bytes).unwrap();

        assert_eq!(result, person);
    }

    #[test]
    fn test_extreme_values() {
        let mut serializer = PortableSerializer::new();
        serializer.register_factory(Arc::new(TestFactory));
        serializer.register_class_definition(create_person_class_def());

        let person = Person {
            name: "Max Values".to_string(),
            age: i32::MAX,
            active: true,
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result: Person = serializer.deserialize_typed(&bytes).unwrap();

        assert_eq!(result, person);

        let person_min = Person {
            name: "Min Values".to_string(),
            age: i32::MIN,
            active: false,
        };

        let bytes_min = serializer.serialize(&person_min).unwrap();
        let result_min: Person = serializer.deserialize_typed(&bytes_min).unwrap();

        assert_eq!(result_min, person_min);
    }

    #[test]
    fn test_get_class_definition() {
        let mut serializer = PortableSerializer::new();
        let class_def = create_person_class_def();
        serializer.register_class_definition(class_def.clone());

        let retrieved = serializer.get_class_definition(TEST_FACTORY_ID, PERSON_CLASS_ID, 1);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().class_id(), PERSON_CLASS_ID);

        let not_found = serializer.get_class_definition(999, 999, 1);
        assert!(not_found.is_none());
    }

    #[derive(Debug, Default, PartialEq, Clone)]
    struct NestedAddress {
        city: String,
        zip: i32,
    }

    impl Portable for NestedAddress {
        fn factory_id(&self) -> i32 {
            TEST_FACTORY_ID
        }

        fn class_id(&self) -> i32 {
            ADDRESS_CLASS_ID
        }

        fn write_portable(&self, writer: &mut DefaultPortableWriter) -> Result<()> {
            writer.write_string("city", Some(&self.city))?;
            writer.write_int("zip", self.zip)?;
            Ok(())
        }

        fn read_portable(&mut self, reader: &mut DefaultPortableReader) -> Result<()> {
            self.city = reader.read_string("city")?.unwrap_or_default();
            self.zip = reader.read_int("zip")?;
            Ok(())
        }
    }

    const EMPLOYEE_CLASS_ID: i32 = 3;

    #[derive(Debug, Default, PartialEq)]
    struct Employee {
        name: String,
        home_address: Option<NestedAddress>,
        work_address: Option<NestedAddress>,
    }

    impl Portable for Employee {
        fn factory_id(&self) -> i32 {
            TEST_FACTORY_ID
        }

        fn class_id(&self) -> i32 {
            EMPLOYEE_CLASS_ID
        }

        fn write_portable(&self, writer: &mut DefaultPortableWriter) -> Result<()> {
            writer.write_string("name", Some(&self.name))?;
            writer.write_portable("home_address", self.home_address.as_ref())?;
            writer.write_portable("work_address", self.work_address.as_ref())?;
            Ok(())
        }

        fn read_portable(&mut self, reader: &mut DefaultPortableReader) -> Result<()> {
            self.name = reader.read_string("name")?.unwrap_or_default();
            self.home_address = reader.read_portable("home_address")?;
            self.work_address = reader.read_portable("work_address")?;
            Ok(())
        }
    }

    struct ExtendedTestFactory;

    impl PortableFactory for ExtendedTestFactory {
        fn factory_id(&self) -> i32 {
            TEST_FACTORY_ID
        }

        fn create(&self, class_id: i32) -> Option<Box<dyn Portable>> {
            match class_id {
                PERSON_CLASS_ID => Some(Box::new(Person::default())),
                ADDRESS_CLASS_ID => Some(Box::new(NestedAddress::default())),
                EMPLOYEE_CLASS_ID => Some(Box::new(Employee::default())),
                _ => None,
            }
        }
    }

    fn create_nested_address_class_def() -> ClassDefinition {
        ClassDefinition::with_fields(
            TEST_FACTORY_ID,
            ADDRESS_CLASS_ID,
            1,
            vec![
                FieldDefinition::new("city", FieldType::Utf8, 0),
                FieldDefinition::new("zip", FieldType::Int, 1),
            ],
        )
    }

    fn create_employee_class_def() -> ClassDefinition {
        ClassDefinition::with_fields(
            TEST_FACTORY_ID,
            EMPLOYEE_CLASS_ID,
            1,
            vec![
                FieldDefinition::new("name", FieldType::Utf8, 0),
                FieldDefinition::new_portable(
                    "home_address",
                    1,
                    TEST_FACTORY_ID,
                    ADDRESS_CLASS_ID,
                    1,
                ),
                FieldDefinition::new_portable(
                    "work_address",
                    2,
                    TEST_FACTORY_ID,
                    ADDRESS_CLASS_ID,
                    1,
                ),
            ],
        )
    }

    #[test]
    fn test_nested_portable_serialization() {
        let mut serializer = PortableSerializer::new();
        serializer.register_factory(Arc::new(ExtendedTestFactory));
        serializer.register_class_definition(create_nested_address_class_def());
        serializer.register_class_definition(create_employee_class_def());

        let employee = Employee {
            name: "John Doe".to_string(),
            home_address: Some(NestedAddress {
                city: "New York".to_string(),
                zip: 10001,
            }),
            work_address: Some(NestedAddress {
                city: "Boston".to_string(),
                zip: 02101,
            }),
        };

        let bytes = serializer.serialize(&employee).unwrap();
        let result = serializer.deserialize(&bytes).unwrap();

        assert_eq!(result.factory_id(), TEST_FACTORY_ID);
        assert_eq!(result.class_id(), EMPLOYEE_CLASS_ID);
    }

    #[test]
    fn test_nested_portable_with_null() {
        let mut serializer = PortableSerializer::new();
        serializer.register_factory(Arc::new(ExtendedTestFactory));
        serializer.register_class_definition(create_nested_address_class_def());
        serializer.register_class_definition(create_employee_class_def());

        let employee = Employee {
            name: "Jane Doe".to_string(),
            home_address: Some(NestedAddress {
                city: "Chicago".to_string(),
                zip: 60601,
            }),
            work_address: None,
        };

        let bytes = serializer.serialize(&employee).unwrap();
        let result = serializer.deserialize(&bytes).unwrap();

        assert_eq!(result.factory_id(), TEST_FACTORY_ID);
        assert_eq!(result.class_id(), EMPLOYEE_CLASS_ID);
    }

    #[test]
    fn test_version_mismatch_handling() {
        let mut serializer = PortableSerializer::new();
        serializer.register_factory(Arc::new(TestFactory));

        let v1_class_def = ClassDefinition::with_fields(
            TEST_FACTORY_ID,
            PERSON_CLASS_ID,
            1,
            vec![
                FieldDefinition::new("name", FieldType::Utf8, 0),
                FieldDefinition::new("age", FieldType::Int, 1),
                FieldDefinition::new("active", FieldType::Bool, 2),
            ],
        );
        serializer.register_class_definition(v1_class_def);

        let v2_class_def = ClassDefinition::with_fields(
            TEST_FACTORY_ID,
            PERSON_CLASS_ID,
            2,
            vec![
                FieldDefinition::new("name", FieldType::Utf8, 0),
                FieldDefinition::new("age", FieldType::Int, 1),
                FieldDefinition::new("active", FieldType::Bool, 2),
                FieldDefinition::new("email", FieldType::Utf8, 3),
            ],
        );
        serializer.register_class_definition(v2_class_def);

        let v1_def = serializer.get_class_definition(TEST_FACTORY_ID, PERSON_CLASS_ID, 1);
        let v2_def = serializer.get_class_definition(TEST_FACTORY_ID, PERSON_CLASS_ID, 2);

        assert!(v1_def.is_some());
        assert!(v2_def.is_some());
        assert_eq!(v1_def.unwrap().field_count(), 3);
        assert_eq!(v2_def.unwrap().field_count(), 4);
    }

    #[test]
    fn test_java_format_compatibility() {
        use crate::serialization::{DataOutput, ObjectDataOutput};

        let mut output = ObjectDataOutput::new();
        output.write_int(TEST_FACTORY_ID).unwrap();
        output.write_int(PERSON_CLASS_ID).unwrap();
        output.write_int(1).unwrap();

        let mut field_output = ObjectDataOutput::new();
        field_output.write_int(3).unwrap();

        field_output.write_string("name").unwrap();
        field_output.write_int(FieldType::Utf8.id()).unwrap();
        field_output.write_byte(1).unwrap();
        let name_bytes = {
            let mut o = ObjectDataOutput::new();
            o.write_bool(true).unwrap();
            o.write_string("TestFromJava").unwrap();
            o.into_bytes()
        };
        field_output.write_int(name_bytes.len() as i32).unwrap();
        field_output.write_bytes(&name_bytes).unwrap();

        field_output.write_string("age").unwrap();
        field_output.write_int(FieldType::Int.id()).unwrap();
        field_output.write_byte(1).unwrap();
        let age_bytes = {
            let mut o = ObjectDataOutput::new();
            o.write_int(25).unwrap();
            o.into_bytes()
        };
        field_output.write_int(age_bytes.len() as i32).unwrap();
        field_output.write_bytes(&age_bytes).unwrap();

        field_output.write_string("active").unwrap();
        field_output.write_int(FieldType::Bool.id()).unwrap();
        field_output.write_byte(1).unwrap();
        let active_bytes = {
            let mut o = ObjectDataOutput::new();
            o.write_bool(true).unwrap();
            o.into_bytes()
        };
        field_output.write_int(active_bytes.len() as i32).unwrap();
        field_output.write_bytes(&active_bytes).unwrap();

        let field_data = field_output.into_bytes();
        output.write_int(field_data.len() as i32).unwrap();
        output.write_bytes(&field_data).unwrap();

        let bytes = output.into_bytes();

        let mut serializer = PortableSerializer::new();
        serializer.register_factory(Arc::new(TestFactory));
        serializer.register_class_definition(create_person_class_def());

        let result: Person = serializer.deserialize_typed(&bytes).unwrap();

        assert_eq!(result.name, "TestFromJava");
        assert_eq!(result.age, 25);
        assert!(result.active);
    }
}
