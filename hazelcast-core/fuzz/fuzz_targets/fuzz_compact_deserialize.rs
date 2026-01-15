#![no_main]

use libfuzzer_sys::fuzz_target;

use hazelcast_core::serialization::compact::{
    Compact, CompactReader, CompactSerializer, CompactWriter, DefaultCompactReader,
    FieldDescriptor, FieldKind, Schema,
};
use hazelcast_core::Result;

#[derive(Debug, Default)]
struct FuzzCompact {
    bool_val: bool,
    int8_val: i8,
    int16_val: i16,
    int32_val: i32,
    int64_val: i64,
    float32_val: f32,
    float64_val: f64,
    string_val: Option<String>,
    nullable_int32: Option<i32>,
}

impl Compact for FuzzCompact {
    fn get_type_name() -> &'static str {
        "FuzzCompact"
    }

    fn write(&self, writer: &mut dyn CompactWriter) -> Result<()> {
        writer.write_boolean("bool", self.bool_val)?;
        writer.write_int8("int8", self.int8_val)?;
        writer.write_int16("int16", self.int16_val)?;
        writer.write_int32("int32", self.int32_val)?;
        writer.write_int64("int64", self.int64_val)?;
        writer.write_float32("float32", self.float32_val)?;
        writer.write_float64("float64", self.float64_val)?;
        writer.write_string("string", self.string_val.as_deref())?;
        writer.write_nullable_int32("nullable_int32", self.nullable_int32)?;
        Ok(())
    }

    fn read(&mut self, reader: &mut dyn CompactReader) -> Result<()> {
        self.bool_val = reader.read_boolean("bool")?;
        self.int8_val = reader.read_int8("int8")?;
        self.int16_val = reader.read_int16("int16")?;
        self.int32_val = reader.read_int32("int32")?;
        self.int64_val = reader.read_int64("int64")?;
        self.float32_val = reader.read_float32("float32")?;
        self.float64_val = reader.read_float64("float64")?;
        self.string_val = reader.read_string("string")?;
        self.nullable_int32 = reader.read_nullable_int32("nullable_int32")?;
        Ok(())
    }
}

fn create_schema() -> Schema {
    Schema::with_fields(
        "FuzzCompact",
        vec![
            FieldDescriptor::new("bool", FieldKind::Boolean, 0),
            FieldDescriptor::new("int8", FieldKind::Int8, 1),
            FieldDescriptor::new("int16", FieldKind::Int16, 2),
            FieldDescriptor::new("int32", FieldKind::Int32, 3),
            FieldDescriptor::new("int64", FieldKind::Int64, 4),
            FieldDescriptor::new("float32", FieldKind::Float32, 5),
            FieldDescriptor::new("float64", FieldKind::Float64, 6),
            FieldDescriptor::new("string", FieldKind::String, 7),
            FieldDescriptor::new("nullable_int32", FieldKind::NullableInt32, 8),
        ],
    )
}

fuzz_target!(|data: &[u8]| {
    let mut serializer = CompactSerializer::new();
    serializer.register_schema(create_schema());

    let _ = serializer.deserialize::<FuzzCompact>(data);

    let schema = Schema::new("FuzzCompact");
    let _ = DefaultCompactReader::from_bytes(data, schema);
});
