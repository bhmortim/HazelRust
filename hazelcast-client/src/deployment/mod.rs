//! User Code Deployment support for deploying custom code to the Hazelcast cluster.
//!
//! This module provides functionality for deploying Java classes and resources to
//! the Hazelcast cluster from the Rust client. This enables execution of custom
//! entry processors, aggregators, and other server-side logic.
//!
//! # Java Interoperability
//!
//! **Important**: Hazelcast clusters run on the JVM, so custom server-side code must
//! be written in Java (or other JVM languages). Rust closures cannot be directly
//! serialized and executed on the cluster.
//!
//! The typical workflow is:
//!
//! 1. Write your custom entry processor/aggregator in Java
//! 2. Compile it to bytecode (`.class` files)
//! 3. Use this module to deploy the bytecode to the cluster
//! 4. Reference the Java class from your Rust client code
//!
//! # Serialization
//!
//! While Rust closures cannot be sent to the JVM, data structures can be serialized
//! using `serde` and sent as arguments to Java classes. The Java classes must be
//! configured to deserialize these arguments using a compatible format (e.g., JSON,
//! MessagePack, or a custom `Portable`/`IdentifiedDataSerializable` implementation).
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client::deployment::{UserCodeDeploymentConfig, ClassDefinition};
//! use hazelcast_client::ClientConfigBuilder;
//!
//! // Load pre-compiled Java bytecode
//! let bytecode = std::fs::read("target/classes/com/example/IncrementProcessor.class")?;
//!
//! let class_def = ClassDefinition::new("com.example.IncrementProcessor", bytecode);
//!
//! let ucd_config = UserCodeDeploymentConfig::builder()
//!     .enabled(true)
//!     .add_class(class_def)
//!     .build()?;
//!
//! let config = ClientConfigBuilder::new()
//!     .user_code_deployment(ucd_config)
//!     .build()?;
//! ```

mod class_definition;
mod config;

pub use class_definition::{ClassDefinition, ClassDefinitionBuilder, ResourceEntry};
pub use config::{
    ClassProviderMode, UserCodeDeploymentConfig, UserCodeDeploymentConfigBuilder,
};
