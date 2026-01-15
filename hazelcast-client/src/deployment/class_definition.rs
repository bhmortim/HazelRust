//! Class definition for user code deployment.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::ConfigError;

/// A resource entry containing name and binary content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceEntry {
    name: String,
    content: Vec<u8>,
}

impl ResourceEntry {
    /// Creates a new resource entry.
    pub fn new(name: impl Into<String>, content: Vec<u8>) -> Self {
        Self {
            name: name.into(),
            content,
        }
    }

    /// Returns the resource name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the resource content.
    pub fn content(&self) -> &[u8] {
        &self.content
    }
}

/// Defines a Java class to be deployed to the Hazelcast cluster.
///
/// A `ClassDefinition` contains the fully-qualified class name and its compiled
/// bytecode. Additional resources (like inner classes or configuration files)
/// can also be included.
///
/// # Java Interoperability Notes
///
/// The bytecode must be valid Java class file format (compiled with `javac` or
/// equivalent). The class should implement the appropriate Hazelcast interface
/// for its intended use:
///
/// | Use Case | Java Interface |
/// |----------|----------------|
/// | Entry Processor | `com.hazelcast.map.EntryProcessor` |
/// | Aggregator | `com.hazelcast.aggregation.Aggregator` |
/// | Predicate | `com.hazelcast.query.Predicate` |
/// | Projection | `com.hazelcast.projection.Projection` |
///
/// # Serialization Considerations
///
/// If your Java class needs to receive data from the Rust client, consider these
/// serialization strategies:
///
/// 1. **Portable Serialization**: Implement `Portable` in Java and use
///    `hazelcast_core::serialization::Portable` in Rust
/// 2. **IdentifiedDataSerializable**: Implement the interface in Java with a
///    matching Rust serializer
/// 3. **JSON**: Use a JSON format that both sides can parse
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::deployment::ClassDefinition;
///
/// // Load bytecode from a compiled .class file
/// let bytecode = std::fs::read("IncrementProcessor.class")?;
///
/// let class_def = ClassDefinition::builder("com.example.IncrementProcessor")
///     .bytecode(bytecode)
///     .add_resource("config.properties", config_bytes)
///     .build()?;
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassDefinition {
    class_name: String,
    bytecode: Vec<u8>,
    resources: HashMap<String, Vec<u8>>,
    inner_classes: Vec<ClassDefinition>,
}

impl ClassDefinition {
    /// Creates a new class definition with the given name and bytecode.
    pub fn new(class_name: impl Into<String>, bytecode: Vec<u8>) -> Self {
        Self {
            class_name: class_name.into(),
            bytecode,
            resources: HashMap::new(),
            inner_classes: Vec::new(),
        }
    }

    /// Creates a new builder for `ClassDefinition`.
    pub fn builder(class_name: impl Into<String>) -> ClassDefinitionBuilder {
        ClassDefinitionBuilder::new(class_name)
    }

    /// Loads a class definition from a `.class` file.
    ///
    /// The class name is inferred from the file path using standard Java
    /// package conventions (e.g., `com/example/MyClass.class` becomes
    /// `com.example.MyClass`).
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or the path is invalid.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let bytecode = std::fs::read(path).map_err(|e| {
            ConfigError::new(format!("failed to read class file '{}': {}", path.display(), e))
        })?;

        let class_name = Self::infer_class_name(path)?;
        Ok(Self::new(class_name, bytecode))
    }

    /// Loads a class definition from a `.class` file with an explicit class name.
    ///
    /// Use this when the file path doesn't follow standard Java package conventions.
    pub fn from_file_with_name(
        path: impl AsRef<Path>,
        class_name: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let bytecode = std::fs::read(path).map_err(|e| {
            ConfigError::new(format!("failed to read class file '{}': {}", path.display(), e))
        })?;

        Ok(Self::new(class_name, bytecode))
    }

    /// Returns the fully-qualified Java class name.
    pub fn class_name(&self) -> &str {
        &self.class_name
    }

    /// Returns the compiled bytecode.
    pub fn bytecode(&self) -> &[u8] {
        &self.bytecode
    }

    /// Returns the additional resources.
    pub fn resources(&self) -> &HashMap<String, Vec<u8>> {
        &self.resources
    }

    /// Returns the inner class definitions.
    pub fn inner_classes(&self) -> &[ClassDefinition] {
        &self.inner_classes
    }

    /// Returns the simple class name (without package prefix).
    pub fn simple_name(&self) -> &str {
        self.class_name
            .rsplit('.')
            .next()
            .unwrap_or(&self.class_name)
    }

    /// Returns the package name, if any.
    pub fn package_name(&self) -> Option<&str> {
        self.class_name.rsplit_once('.').map(|(pkg, _)| pkg)
    }

    /// Returns the internal JVM class name (with `/` separators).
    pub fn internal_name(&self) -> String {
        self.class_name.replace('.', "/")
    }

    /// Converts this definition to bytes for transmission.
    ///
    /// Uses bincode serialization for efficient binary encoding.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConfigError> {
        bincode::serialize(self).map_err(|e| {
            ConfigError::new(format!("failed to serialize class definition: {}", e))
        })
    }

    /// Deserializes a class definition from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConfigError> {
        bincode::deserialize(bytes).map_err(|e| {
            ConfigError::new(format!("failed to deserialize class definition: {}", e))
        })
    }

    fn infer_class_name(path: &Path) -> Result<String, ConfigError> {
        let path_str = path
            .to_str()
            .ok_or_else(|| ConfigError::new("invalid path encoding"))?;

        // Remove .class extension
        let without_ext = path_str
            .strip_suffix(".class")
            .unwrap_or(path_str);

        // Convert path separators to dots
        let class_name = without_ext
            .replace(['/', '\\'], ".");

        // Remove leading dots if path started with separator
        let class_name = class_name.trim_start_matches('.');

        if class_name.is_empty() {
            return Err(ConfigError::new("could not infer class name from path"));
        }

        Ok(class_name.to_string())
    }
}

/// Builder for `ClassDefinition`.
#[derive(Debug, Clone, Default)]
pub struct ClassDefinitionBuilder {
    class_name: String,
    bytecode: Option<Vec<u8>>,
    resources: HashMap<String, Vec<u8>>,
    inner_classes: Vec<ClassDefinition>,
}

impl ClassDefinitionBuilder {
    /// Creates a new builder with the given class name.
    pub fn new(class_name: impl Into<String>) -> Self {
        Self {
            class_name: class_name.into(),
            bytecode: None,
            resources: HashMap::new(),
            inner_classes: Vec::new(),
        }
    }

    /// Sets the bytecode for this class.
    pub fn bytecode(mut self, bytecode: Vec<u8>) -> Self {
        self.bytecode = Some(bytecode);
        self
    }

    /// Loads bytecode from a file.
    pub fn bytecode_from_file(mut self, path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let bytecode = std::fs::read(path).map_err(|e| {
            ConfigError::new(format!("failed to read bytecode from '{}': {}", path.display(), e))
        })?;
        self.bytecode = Some(bytecode);
        Ok(self)
    }

    /// Adds a resource to this class definition.
    pub fn add_resource(mut self, name: impl Into<String>, content: Vec<u8>) -> Self {
        self.resources.insert(name.into(), content);
        self
    }

    /// Adds a resource from a file.
    pub fn add_resource_from_file(
        mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let content = std::fs::read(path).map_err(|e| {
            ConfigError::new(format!("failed to read resource from '{}': {}", path.display(), e))
        })?;
        self.resources.insert(name.into(), content);
        Ok(self)
    }

    /// Adds an inner class definition.
    pub fn add_inner_class(mut self, inner: ClassDefinition) -> Self {
        self.inner_classes.push(inner);
        self
    }

    /// Builds the `ClassDefinition`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The class name is empty
    /// - No bytecode was provided
    pub fn build(self) -> Result<ClassDefinition, ConfigError> {
        if self.class_name.is_empty() {
            return Err(ConfigError::new("class name must not be empty"));
        }

        let bytecode = self.bytecode.ok_or_else(|| {
            ConfigError::new("bytecode is required for class definition")
        })?;

        if bytecode.is_empty() {
            return Err(ConfigError::new("bytecode must not be empty"));
        }

        // Basic validation: Java class files start with magic number 0xCAFEBABE
        if bytecode.len() >= 4 {
            let magic = u32::from_be_bytes([bytecode[0], bytecode[1], bytecode[2], bytecode[3]]);
            if magic != 0xCAFEBABE {
                return Err(ConfigError::new(
                    "invalid Java class file: missing magic number (0xCAFEBABE)",
                ));
            }
        }

        Ok(ClassDefinition {
            class_name: self.class_name,
            bytecode,
            resources: self.resources,
            inner_classes: self.inner_classes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_class_bytecode() -> Vec<u8> {
        // Minimal valid Java class file header
        let mut bytecode = vec![0xCA, 0xFE, 0xBA, 0xBE]; // magic
        bytecode.extend_from_slice(&[0x00, 0x00]); // minor version
        bytecode.extend_from_slice(&[0x00, 0x34]); // major version (Java 8)
        bytecode.extend_from_slice(&[0x00, 0x01]); // constant pool count
        bytecode
    }

    #[test]
    fn test_class_definition_new() {
        let bytecode = valid_class_bytecode();
        let def = ClassDefinition::new("com.example.MyClass", bytecode.clone());

        assert_eq!(def.class_name(), "com.example.MyClass");
        assert_eq!(def.bytecode(), &bytecode);
        assert!(def.resources().is_empty());
        assert!(def.inner_classes().is_empty());
    }

    #[test]
    fn test_class_definition_simple_name() {
        let def = ClassDefinition::new("com.example.MyClass", valid_class_bytecode());
        assert_eq!(def.simple_name(), "MyClass");

        let def_no_pkg = ClassDefinition::new("MyClass", valid_class_bytecode());
        assert_eq!(def_no_pkg.simple_name(), "MyClass");
    }

    #[test]
    fn test_class_definition_package_name() {
        let def = ClassDefinition::new("com.example.MyClass", valid_class_bytecode());
        assert_eq!(def.package_name(), Some("com.example"));

        let def_no_pkg = ClassDefinition::new("MyClass", valid_class_bytecode());
        assert_eq!(def_no_pkg.package_name(), None);
    }

    #[test]
    fn test_class_definition_internal_name() {
        let def = ClassDefinition::new("com.example.MyClass", valid_class_bytecode());
        assert_eq!(def.internal_name(), "com/example/MyClass");
    }

    #[test]
    fn test_class_definition_builder() {
        let bytecode = valid_class_bytecode();
        let def = ClassDefinition::builder("com.example.Test")
            .bytecode(bytecode.clone())
            .add_resource("config.xml", b"<config/>".to_vec())
            .build()
            .unwrap();

        assert_eq!(def.class_name(), "com.example.Test");
        assert_eq!(def.bytecode(), &bytecode);
        assert_eq!(def.resources().len(), 1);
        assert!(def.resources().contains_key("config.xml"));
    }

    #[test]
    fn test_class_definition_builder_empty_name_fails() {
        let result = ClassDefinition::builder("")
            .bytecode(valid_class_bytecode())
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("class name must not be empty"));
    }

    #[test]
    fn test_class_definition_builder_no_bytecode_fails() {
        let result = ClassDefinition::builder("com.example.Test").build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("bytecode is required"));
    }

    #[test]
    fn test_class_definition_builder_empty_bytecode_fails() {
        let result = ClassDefinition::builder("com.example.Test")
            .bytecode(vec![])
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("bytecode must not be empty"));
    }

    #[test]
    fn test_class_definition_builder_invalid_magic_fails() {
        let result = ClassDefinition::builder("com.example.Test")
            .bytecode(vec![0x00, 0x00, 0x00, 0x00])
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing magic number"));
    }

    #[test]
    fn test_class_definition_builder_with_inner_class() {
        let inner = ClassDefinition::new("com.example.Outer$Inner", valid_class_bytecode());
        let outer = ClassDefinition::builder("com.example.Outer")
            .bytecode(valid_class_bytecode())
            .add_inner_class(inner)
            .build()
            .unwrap();

        assert_eq!(outer.inner_classes().len(), 1);
        assert_eq!(outer.inner_classes()[0].class_name(), "com.example.Outer$Inner");
    }

    #[test]
    fn test_class_definition_serialization() {
        let def = ClassDefinition::builder("com.example.Test")
            .bytecode(valid_class_bytecode())
            .add_resource("test.txt", b"hello".to_vec())
            .build()
            .unwrap();

        let bytes = def.to_bytes().unwrap();
        let restored = ClassDefinition::from_bytes(&bytes).unwrap();

        assert_eq!(restored.class_name(), def.class_name());
        assert_eq!(restored.bytecode(), def.bytecode());
        assert_eq!(restored.resources().len(), def.resources().len());
    }

    #[test]
    fn test_class_definition_infer_class_name() {
        let path = Path::new("com/example/MyClass.class");
        let name = ClassDefinition::infer_class_name(path).unwrap();
        assert_eq!(name, "com.example.MyClass");

        let path_win = Path::new("com\\example\\MyClass.class");
        let name_win = ClassDefinition::infer_class_name(path_win).unwrap();
        assert_eq!(name_win, "com.example.MyClass");
    }

    #[test]
    fn test_resource_entry() {
        let entry = ResourceEntry::new("config.xml", b"<config/>".to_vec());
        assert_eq!(entry.name(), "config.xml");
        assert_eq!(entry.content(), b"<config/>");
    }

    #[test]
    fn test_class_definition_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ClassDefinition>();
    }
}
