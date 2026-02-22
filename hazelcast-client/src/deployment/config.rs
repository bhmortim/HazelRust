//! User code deployment configuration.

use serde::{Deserialize, Serialize};

use super::ClassDefinition;
use crate::config::ConfigError;

/// Mode for providing classes to the cluster.
///
/// Determines how classes are resolved when a cluster member needs to load
/// a class that was deployed by the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum ClassProviderMode {
    /// Classes are provided from local definitions and cached by the cluster.
    ///
    /// This is the default mode. When a class is deployed, it's sent to the
    /// cluster and cached for future use by any member that needs it.
    #[default]
    LocalAndCached,

    /// Only locally defined classes are used, no caching.
    ///
    /// Each time a class is needed, it must be provided by the client.
    /// Use this mode when classes change frequently.
    LocalClassesOnly,

    /// Only cached classes are used.
    ///
    /// Classes must have been previously deployed and cached. This mode
    /// is useful for read-only clients that don't deploy new code.
    CachedOnly,
}

/// Configuration for user code deployment.
///
/// User code deployment allows the Rust client to deploy Java classes to the
/// Hazelcast cluster. This enables using custom entry processors, aggregators,
/// predicates, and other server-side logic.
///
/// # Java Interoperability
///
/// **Note**: Hazelcast clusters run on the JVM. Custom server-side code must be
/// written in Java (or another JVM language like Kotlin/Scala) and compiled to
/// bytecode before deployment.
///
/// Rust closures **cannot** be directly serialized and executed on the cluster.
/// Instead:
///
/// 1. Write your custom logic in Java implementing the appropriate interface
/// 2. Compile it to `.class` files
/// 3. Deploy the bytecode using this configuration
/// 4. Reference the Java class from your Rust client
///
/// # Serializing Arguments
///
/// To pass data from Rust to your Java classes, use one of these approaches:
///
/// | Approach | Rust Side | Java Side |
/// |----------|-----------|-----------|
/// | Portable | Implement `Portable` trait | Implement `Portable` interface |
/// | JSON | Use `serde_json` | Use Jackson/Gson |
/// | Custom | Implement `Serializable` trait | Implement `DataSerializable` |
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::deployment::{UserCodeDeploymentConfig, ClassDefinition};
///
/// let processor_bytecode = std::fs::read("target/classes/IncrementProcessor.class")?;
/// let processor = ClassDefinition::new("com.example.IncrementProcessor", processor_bytecode);
///
/// let config = UserCodeDeploymentConfig::builder()
///     .enabled(true)
///     .add_class(processor)
///     .whitelist_prefix("com.example.")
///     .build()?;
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UserCodeDeploymentConfig {
    enabled: bool,
    class_definitions: Vec<ClassDefinition>,
    blacklist_prefixes: Vec<String>,
    whitelist_prefixes: Vec<String>,
    provider_mode: ClassProviderMode,
    provider_filter: Option<String>,
}

impl UserCodeDeploymentConfig {
    /// Creates a new builder for `UserCodeDeploymentConfig`.
    pub fn builder() -> UserCodeDeploymentConfigBuilder {
        UserCodeDeploymentConfigBuilder::new()
    }

    /// Returns whether user code deployment is enabled.
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the class definitions to deploy.
    pub fn class_definitions(&self) -> &[ClassDefinition] {
        &self.class_definitions
    }

    /// Returns the blacklisted class prefixes.
    ///
    /// Classes with names starting with these prefixes will not be deployed.
    pub fn blacklist_prefixes(&self) -> &[String] {
        &self.blacklist_prefixes
    }

    /// Returns the whitelisted class prefixes.
    ///
    /// If non-empty, only classes with names starting with these prefixes
    /// will be deployed.
    pub fn whitelist_prefixes(&self) -> &[String] {
        &self.whitelist_prefixes
    }

    /// Returns the class provider mode.
    pub fn provider_mode(&self) -> ClassProviderMode {
        self.provider_mode
    }

    /// Returns the provider filter, if set.
    ///
    /// The filter is a class name pattern that determines which members
    /// can provide classes.
    pub fn provider_filter(&self) -> Option<&str> {
        self.provider_filter.as_deref()
    }

    /// Checks if a class name is allowed by the whitelist/blacklist rules.
    pub fn is_class_allowed(&self, class_name: &str) -> bool {
        // Check blacklist first
        for prefix in &self.blacklist_prefixes {
            if class_name.starts_with(prefix) {
                return false;
            }
        }

        // If whitelist is empty, allow all non-blacklisted
        if self.whitelist_prefixes.is_empty() {
            return true;
        }

        // Check whitelist
        for prefix in &self.whitelist_prefixes {
            if class_name.starts_with(prefix) {
                return true;
            }
        }

        false
    }

    /// Finds a class definition by name.
    pub fn find_class(&self, class_name: &str) -> Option<&ClassDefinition> {
        self.class_definitions
            .iter()
            .find(|c| c.class_name() == class_name)
    }

    /// Serializes this configuration to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConfigError> {
        bincode::serialize(self).map_err(|e| {
            ConfigError::new(format!("failed to serialize user code deployment config: {}", e))
        })
    }

    /// Deserializes configuration from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConfigError> {
        bincode::deserialize(bytes).map_err(|e| {
            ConfigError::new(format!(
                "failed to deserialize user code deployment config: {}",
                e
            ))
        })
    }
}

/// Builder for `UserCodeDeploymentConfig`.
#[derive(Debug, Clone, Default)]
pub struct UserCodeDeploymentConfigBuilder {
    enabled: Option<bool>,
    class_definitions: Vec<ClassDefinition>,
    blacklist_prefixes: Vec<String>,
    whitelist_prefixes: Vec<String>,
    provider_mode: Option<ClassProviderMode>,
    provider_filter: Option<String>,
}

impl UserCodeDeploymentConfigBuilder {
    /// Creates a new builder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables or disables user code deployment.
    ///
    /// Defaults to `false`.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = Some(enabled);
        self
    }

    /// Adds a class definition to deploy.
    pub fn add_class(mut self, class: ClassDefinition) -> Self {
        self.class_definitions.push(class);
        self
    }

    /// Adds multiple class definitions to deploy.
    pub fn add_classes(mut self, classes: impl IntoIterator<Item = ClassDefinition>) -> Self {
        self.class_definitions.extend(classes);
        self
    }

    /// Adds a blacklist prefix.
    ///
    /// Classes with names starting with this prefix will not be deployed.
    pub fn blacklist_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.blacklist_prefixes.push(prefix.into());
        self
    }

    /// Adds multiple blacklist prefixes.
    pub fn blacklist_prefixes<I, S>(mut self, prefixes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.blacklist_prefixes
            .extend(prefixes.into_iter().map(Into::into));
        self
    }

    /// Adds a whitelist prefix.
    ///
    /// If any whitelist prefixes are set, only classes with names starting
    /// with one of them will be deployed.
    pub fn whitelist_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.whitelist_prefixes.push(prefix.into());
        self
    }

    /// Adds multiple whitelist prefixes.
    pub fn whitelist_prefixes<I, S>(mut self, prefixes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.whitelist_prefixes
            .extend(prefixes.into_iter().map(Into::into));
        self
    }

    /// Sets the class provider mode.
    ///
    /// Defaults to `ClassProviderMode::LocalAndCached`.
    pub fn provider_mode(mut self, mode: ClassProviderMode) -> Self {
        self.provider_mode = Some(mode);
        self
    }

    /// Sets the provider filter pattern.
    ///
    /// The filter determines which cluster members can provide classes.
    pub fn provider_filter(mut self, filter: impl Into<String>) -> Self {
        self.provider_filter = Some(filter.into());
        self
    }

    /// Builds the `UserCodeDeploymentConfig`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - User code deployment is enabled but no classes are provided
    pub fn build(self) -> Result<UserCodeDeploymentConfig, ConfigError> {
        let enabled = self.enabled.unwrap_or(false);

        // Validate class names against whitelist/blacklist
        let whitelist = &self.whitelist_prefixes;
        let blacklist = &self.blacklist_prefixes;

        for class in &self.class_definitions {
            let class_name = class.class_name();

            // Check blacklist
            for prefix in blacklist {
                if class_name.starts_with(prefix) {
                    return Err(ConfigError::new(format!(
                        "class '{}' matches blacklist prefix '{}'",
                        class_name, prefix
                    )));
                }
            }

            // Check whitelist if non-empty
            if !whitelist.is_empty() {
                let allowed = whitelist.iter().any(|p| class_name.starts_with(p));
                if !allowed {
                    return Err(ConfigError::new(format!(
                        "class '{}' does not match any whitelist prefix",
                        class_name
                    )));
                }
            }
        }

        Ok(UserCodeDeploymentConfig {
            enabled,
            class_definitions: self.class_definitions,
            blacklist_prefixes: self.blacklist_prefixes,
            whitelist_prefixes: self.whitelist_prefixes,
            provider_mode: self.provider_mode.unwrap_or_default(),
            provider_filter: self.provider_filter,
        })
    }
}

/// Represents a serializable closure argument for Java entry processors.
///
/// Since Rust closures cannot be directly executed on the JVM, this struct
/// provides a way to serialize data that can be passed to pre-deployed Java
/// classes.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::deployment::SerializableArgument;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct IncrementArg {
///     field_name: String,
///     delta: i64,
/// }
///
/// let arg = SerializableArgument::new(IncrementArg {
///     field_name: "counter".to_string(),
///     delta: 5,
/// })?;
///
/// // Send arg.data() to the Java entry processor
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct SerializableArgument {
    type_name: String,
    data: Vec<u8>,
}

#[allow(dead_code)]
impl SerializableArgument {
    /// Creates a new serializable argument from any serializable value.
    pub fn new<T: Serialize>(value: T) -> Result<Self, ConfigError> {
        let type_name = std::any::type_name::<T>().to_string();
        let data = bincode::serialize(&value).map_err(|e| {
            ConfigError::new(format!("failed to serialize argument: {}", e))
        })?;

        Ok(Self { type_name, data })
    }

    /// Returns the Rust type name of the serialized value.
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Returns the serialized data.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Deserializes the argument back to its original type.
    pub fn deserialize<T: for<'de> Deserialize<'de>>(&self) -> Result<T, ConfigError> {
        bincode::deserialize(&self.data).map_err(|e| {
            ConfigError::new(format!("failed to deserialize argument: {}", e))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_class(name: &str) -> ClassDefinition {
        // Valid Java class file header
        let mut bytecode = vec![0xCA, 0xFE, 0xBA, 0xBE];
        bytecode.extend_from_slice(&[0x00, 0x00, 0x00, 0x34, 0x00, 0x01]);
        ClassDefinition::new(name, bytecode)
    }

    #[test]
    fn test_user_code_deployment_config_defaults() {
        let config = UserCodeDeploymentConfig::builder().build().unwrap();

        assert!(!config.enabled());
        assert!(config.class_definitions().is_empty());
        assert!(config.blacklist_prefixes().is_empty());
        assert!(config.whitelist_prefixes().is_empty());
        assert_eq!(config.provider_mode(), ClassProviderMode::LocalAndCached);
        assert!(config.provider_filter().is_none());
    }

    #[test]
    fn test_user_code_deployment_config_enabled() {
        let config = UserCodeDeploymentConfig::builder()
            .enabled(true)
            .build()
            .unwrap();

        assert!(config.enabled());
    }

    #[test]
    fn test_user_code_deployment_config_with_classes() {
        let class1 = test_class("com.example.Processor1");
        let class2 = test_class("com.example.Processor2");

        let config = UserCodeDeploymentConfig::builder()
            .add_class(class1)
            .add_class(class2)
            .build()
            .unwrap();

        assert_eq!(config.class_definitions().len(), 2);
    }

    #[test]
    fn test_user_code_deployment_config_add_classes() {
        let classes = vec![
            test_class("com.example.A"),
            test_class("com.example.B"),
        ];

        let config = UserCodeDeploymentConfig::builder()
            .add_classes(classes)
            .build()
            .unwrap();

        assert_eq!(config.class_definitions().len(), 2);
    }

    #[test]
    fn test_user_code_deployment_config_blacklist() {
        let config = UserCodeDeploymentConfig::builder()
            .blacklist_prefix("java.")
            .blacklist_prefix("sun.")
            .build()
            .unwrap();

        assert_eq!(config.blacklist_prefixes().len(), 2);
        assert!(!config.is_class_allowed("java.lang.String"));
        assert!(!config.is_class_allowed("sun.misc.Unsafe"));
        assert!(config.is_class_allowed("com.example.MyClass"));
    }

    #[test]
    fn test_user_code_deployment_config_whitelist() {
        let config = UserCodeDeploymentConfig::builder()
            .whitelist_prefix("com.example.")
            .whitelist_prefix("org.myorg.")
            .build()
            .unwrap();

        assert!(config.is_class_allowed("com.example.MyClass"));
        assert!(config.is_class_allowed("org.myorg.AnotherClass"));
        assert!(!config.is_class_allowed("com.other.SomeClass"));
    }

    #[test]
    fn test_user_code_deployment_config_blacklist_overrides_whitelist() {
        let config = UserCodeDeploymentConfig::builder()
            .whitelist_prefix("com.")
            .blacklist_prefix("com.internal.")
            .build()
            .unwrap();

        assert!(config.is_class_allowed("com.example.MyClass"));
        assert!(!config.is_class_allowed("com.internal.Secret"));
    }

    #[test]
    fn test_user_code_deployment_config_provider_mode() {
        let config = UserCodeDeploymentConfig::builder()
            .provider_mode(ClassProviderMode::CachedOnly)
            .build()
            .unwrap();

        assert_eq!(config.provider_mode(), ClassProviderMode::CachedOnly);
    }

    #[test]
    fn test_user_code_deployment_config_provider_filter() {
        let config = UserCodeDeploymentConfig::builder()
            .provider_filter("member-*")
            .build()
            .unwrap();

        assert_eq!(config.provider_filter(), Some("member-*"));
    }

    #[test]
    fn test_user_code_deployment_config_find_class() {
        let class = test_class("com.example.MyProcessor");

        let config = UserCodeDeploymentConfig::builder()
            .add_class(class)
            .build()
            .unwrap();

        assert!(config.find_class("com.example.MyProcessor").is_some());
        assert!(config.find_class("com.example.OtherProcessor").is_none());
    }

    #[test]
    fn test_user_code_deployment_config_blacklist_validation() {
        let class = test_class("java.lang.Exploit");

        let result = UserCodeDeploymentConfig::builder()
            .blacklist_prefix("java.")
            .add_class(class)
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("blacklist"));
    }

    #[test]
    fn test_user_code_deployment_config_whitelist_validation() {
        let class = test_class("org.other.SomeClass");

        let result = UserCodeDeploymentConfig::builder()
            .whitelist_prefix("com.example.")
            .add_class(class)
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("whitelist"));
    }

    #[test]
    fn test_user_code_deployment_config_serialization() {
        let class = test_class("com.example.Test");

        let config = UserCodeDeploymentConfig::builder()
            .enabled(true)
            .add_class(class)
            .provider_mode(ClassProviderMode::LocalClassesOnly)
            .build()
            .unwrap();

        let bytes = config.to_bytes().unwrap();
        let restored = UserCodeDeploymentConfig::from_bytes(&bytes).unwrap();

        assert_eq!(restored.enabled(), config.enabled());
        assert_eq!(restored.provider_mode(), config.provider_mode());
        assert_eq!(
            restored.class_definitions().len(),
            config.class_definitions().len()
        );
    }

    #[test]
    fn test_class_provider_mode_default() {
        let mode: ClassProviderMode = Default::default();
        assert_eq!(mode, ClassProviderMode::LocalAndCached);
    }

    #[test]
    fn test_class_provider_mode_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<ClassProviderMode>();
    }

    #[test]
    fn test_serializable_argument() {
        #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
        struct TestArg {
            name: String,
            value: i64,
        }

        let original = TestArg {
            name: "test".to_string(),
            value: 42,
        };

        let arg = SerializableArgument::new(original.clone()).unwrap();
        assert!(!arg.data().is_empty());
        assert!(arg.type_name().contains("TestArg"));

        let restored: TestArg = arg.deserialize().unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn test_serializable_argument_primitive() {
        let arg = SerializableArgument::new(42i64).unwrap();
        let restored: i64 = arg.deserialize().unwrap();
        assert_eq!(restored, 42);
    }

    #[test]
    fn test_user_code_deployment_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<UserCodeDeploymentConfig>();
    }
}
