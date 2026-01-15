#![no_main]

use libfuzzer_sys::fuzz_target;
use std::sync::Arc;

use hazelcast_core::serialization::portable::{
    ClassDefinition, FieldDefinition, FieldType, Portable, PortableFactory, PortableReader,
    PortableSerializer, PortableWriter,
};
use hazelcast_core::Result;

#[derive(Debug, Default)]
struct FuzzPortable {
    byte_val: i8,
    bool_val: bool,
    short_val: i16,
    int_val: i32,
    long_val: i64,
    float_val: f32,
    double_val: f64,
    string_val: Option<String>,
}

impl Portable for FuzzPortable {
    fn factory_id(&self) -> i32 {
        1
    }

    fn class_id(&self) -> i32 {
        1
    }

    fn write_portable(&self, writer: &mut dyn PortableWriter) -> Result<()> {
        writer.write_byte("byte", self.byte_val)?;
        writer.write_bool("bool", self.bool_val)?;
        writer.write_short("short", self.short_val)?;
        writer.write_int("int", self.int_val)?;
        writer.write_long("long", self.long_val)?;
        writer.write_float("float", self.float_val)?;
        writer.write_double("double", self.double_val)?;
        writer.write_string("string", self.string_val.as_deref())?;
        Ok(())
    }

    fn read_portable(&mut self, reader: &mut dyn PortableReader) -> Result<()> {
        self.byte_val = reader.read_byte("byte")?;
        self.bool_val = reader.read_bool("bool")?;
        self.short_val = reader.read_short("short")?;
        self.int_val = reader.read_int("int")?;
        self.long_val = reader.read_long("long")?;
        self.float_val = reader.read_float("float")?;
        self.double_val = reader.read_double("double")?;
        self.string_val = reader.read_string("string")?;
        Ok(())
    }
}

struct FuzzFactory;

impl PortableFactory for FuzzFactory {
    fn factory_id(&self) -> i32 {
        1
    }

    fn create(&self, class_id: i32) -> Option<Box<dyn Portable>> {
        match class_id {
            1 => Some(Box::new(FuzzPortable::default())),
            _ => None,
        }
    }
}

fn create_class_definition() -> ClassDefinition {
    ClassDefinition::with_fields(
        1,
        1,
        1,
        vec![
            FieldDefinition::new("byte", FieldType::Byte, 0),
            FieldDefinition::new("bool", FieldType::Bool, 1),
            FieldDefinition::new("short", FieldType::Short, 2),
            FieldDefinition::new("int", FieldType::Int, 3),
            FieldDefinition::new("long", FieldType::Long, 4),
            FieldDefinition::new("float", FieldType::Float, 5),
            FieldDefinition::new("double", FieldType::Double, 6),
            FieldDefinition::new("string", FieldType::Utf8, 7),
        ],
    )
}

fuzz_target!(|data: &[u8]| {
    let mut serializer = PortableSerializer::new();
    serializer.register_factory(Arc::new(FuzzFactory));
    serializer.register_class_definition(create_class_definition());

    let _ = serializer.deserialize(data);
    let _ = serializer.deserialize_typed::<FuzzPortable>(data);
});
