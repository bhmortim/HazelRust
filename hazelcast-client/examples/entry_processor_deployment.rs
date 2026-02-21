//! Example demonstrating user code deployment for custom entry processors.
//!
//! This example shows how to deploy a custom Java entry processor to a Hazelcast
//! cluster and execute it from a Rust client.
//!
//! # Prerequisites
//!
//! 1. Compile the Java entry processor (see `java/` directory for source)
//! 2. Start a Hazelcast cluster with user code deployment enabled
//! 3. Run this example
//!
//! # Java Entry Processor Example
//!
//! The Java entry processor should implement `EntryProcessor<K, V, R>`:
//!
//! ```java
//! package com.example;
//!
//! import com.hazelcast.map.EntryProcessor;
//! import java.util.Map;
//!
//! public class IncrementProcessor implements EntryProcessor<String, Long, Long> {
//!     private final long delta;
//!
//!     public IncrementProcessor(long delta) {
//!         this.delta = delta;
//!     }
//!
//!     @Override
//!     public Long process(Map.Entry<String, Long> entry) {
//!         Long oldValue = entry.getValue();
//!         if (oldValue == null) {
//!             oldValue = 0L;
//!         }
//!         Long newValue = oldValue + delta;
//!         entry.setValue(newValue);
//!         return newValue;
//!     }
//! }
//! ```

use std::path::Path;

use hazelcast_client::{
    ClientConfigBuilder, ClassDefinition, ClassProviderMode, UserCodeDeploymentConfig,
};
use hazelcast_client::config::ConfigError;
use hazelcast_client::proxy::EntryProcessor;
use hazelcast_core::serialization::{DataOutput, Serializable};

/// A Rust wrapper for the Java IncrementProcessor.
///
/// This struct serializes the delta value that will be passed to the
/// Java entry processor on the cluster.
struct IncrementProcessor {
    /// The Java class name to invoke on the cluster.
    class_name: String,
    /// The amount to increment by.
    delta: i64,
}

impl IncrementProcessor {
    fn new(delta: i64) -> Self {
        Self {
            class_name: "com.example.IncrementProcessor".to_string(),
            delta,
        }
    }
}

impl EntryProcessor for IncrementProcessor {
    type Output = i64;
}

impl Serializable for IncrementProcessor {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> hazelcast_core::Result<()> {
        // Serialize the class name for the cluster to instantiate
        output.write_string(&self.class_name)?;
        // Serialize the delta argument
        output.write_long(self.delta)?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example 1: Create configuration with embedded bytecode
    println!("=== Example 1: User Code Deployment Configuration ===\n");

    // In a real scenario, you would load bytecode from a compiled .class file:
    // let bytecode = std::fs::read("target/classes/com/example/IncrementProcessor.class")?;

    // For this example, we'll demonstrate the configuration without actual bytecode
    let example_bytecode = create_mock_java_class();

    let class_def = ClassDefinition::builder("com.example.IncrementProcessor")
        .bytecode(example_bytecode)
        .build()?;

    println!("Created class definition:");
    println!("  Class name: {}", class_def.class_name());
    println!("  Simple name: {}", class_def.simple_name());
    println!("  Package: {:?}", class_def.package_name());
    println!("  Internal name: {}", class_def.internal_name());
    println!("  Bytecode size: {} bytes", class_def.bytecode().len());

    // Example 2: Configure user code deployment
    println!("\n=== Example 2: Deployment Configuration ===\n");

    let ucd_config = UserCodeDeploymentConfig::builder()
        .enabled(true)
        .add_class(class_def)
        .whitelist_prefix("com.example.")
        .blacklist_prefix("com.example.internal.")
        .provider_mode(ClassProviderMode::LocalAndCached)
        .build()?;

    println!("User code deployment config:");
    println!("  Enabled: {}", ucd_config.enabled());
    println!("  Classes: {}", ucd_config.class_definitions().len());
    println!("  Provider mode: {:?}", ucd_config.provider_mode());
    println!("  Whitelist: {:?}", ucd_config.whitelist_prefixes());
    println!("  Blacklist: {:?}", ucd_config.blacklist_prefixes());

    // Example 3: Build client configuration
    println!("\n=== Example 3: Client Configuration ===\n");

    let client_config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address("127.0.0.1:5701".parse()?)
        .user_code_deployment(ucd_config)
        .build()?;

    println!("Client configuration created:");
    println!("  Cluster: {}", client_config.cluster_name());
    println!(
        "  User code deployment enabled: {}",
        client_config
            .user_code_deployment()
            .map(|c| c.enabled())
            .unwrap_or(false)
    );

    // Example 4: Demonstrate class filtering
    println!("\n=== Example 4: Class Filtering ===\n");

    let filter_config = UserCodeDeploymentConfig::builder()
        .whitelist_prefix("com.mycompany.")
        .blacklist_prefix("com.mycompany.internal.")
        .build()?;

    let test_cases = [
        "com.mycompany.MyProcessor",
        "com.mycompany.internal.Secret",
        "com.other.SomeClass",
        "java.lang.String",
    ];

    for class_name in &test_cases {
        let allowed = filter_config.is_class_allowed(class_name);
        println!("  {} -> {}", class_name, if allowed { "ALLOWED" } else { "BLOCKED" });
    }

    // Example 5: Loading classes from files (pseudo-code)
    println!("\n=== Example 5: Loading from Files (Pseudo-code) ===\n");

    println!("To load a class from a file:");
    println!();
    println!("  // Load a single class");
    println!("  let class_def = ClassDefinition::from_file(");
    println!("      \"target/classes/com/example/MyProcessor.class\"");
    println!("  )?;");
    println!();
    println!("  // Or use the builder with explicit class name");
    println!("  let class_def = ClassDefinition::builder(\"com.example.MyProcessor\")");
    println!("      .bytecode_from_file(\"MyProcessor.class\")?");
    println!("      .add_resource_from_file(\"config.xml\", \"resources/config.xml\")?");
    println!("      .build()?;");

    // Example 6: Using the entry processor
    println!("\n=== Example 6: Entry Processor Usage ===\n");

    let processor = IncrementProcessor::new(5);
    println!("Created IncrementProcessor with delta = 5");
    println!();
    println!("In a real application, you would use it like this:");
    println!();
    println!("  let map: IMap<String, i64> = client.get_map(\"counters\").await?;");
    println!("  let result = map.execute_on_key(&\"my-counter\".to_string(), &processor).await?;");
    println!("  println!(\"New value: {{:?}}\", result);");

    // Example 7: Serialization for transmission
    println!("\n=== Example 7: Serialization ===\n");

    let class_for_serialization = ClassDefinition::builder("com.example.Test")
        .bytecode(create_mock_java_class())
        .build()?;

    let bytes = class_for_serialization.to_bytes()?;
    println!("Serialized class definition: {} bytes", bytes.len());

    let restored = ClassDefinition::from_bytes(&bytes)?;
    println!("Restored class name: {}", restored.class_name());

    println!("\n=== Java Interoperability Notes ===\n");
    println!("IMPORTANT: Rust closures cannot be directly executed on the JVM cluster.");
    println!();
    println!("The workflow for custom server-side logic is:");
    println!("  1. Write your entry processor/aggregator in Java");
    println!("  2. Compile it: javac -cp hazelcast.jar MyProcessor.java");
    println!("  3. Deploy bytecode using UserCodeDeploymentConfig");
    println!("  4. Create a Rust struct that serializes arguments for the Java class");
    println!("  5. Execute via IMap::execute_on_key() or similar methods");
    println!();
    println!("For passing complex arguments to Java:");
    println!("  - Use Portable serialization (implement Portable in both Rust and Java)");
    println!("  - Use JSON (serde_json in Rust, Jackson/Gson in Java)");
    println!("  - Use a custom binary format with matching serializers");

    Ok(())
}

/// Creates a mock Java class file with valid magic number for testing.
///
/// In a real application, this would be actual compiled Java bytecode.
fn create_mock_java_class() -> Vec<u8> {
    let mut bytecode = Vec::new();

    // Java class file magic number
    bytecode.extend_from_slice(&[0xCA, 0xFE, 0xBA, 0xBE]);

    // Minor version (0)
    bytecode.extend_from_slice(&[0x00, 0x00]);

    // Major version (52 = Java 8)
    bytecode.extend_from_slice(&[0x00, 0x34]);

    // Constant pool count (minimal)
    bytecode.extend_from_slice(&[0x00, 0x01]);

    // Padding to make it look like a real class file
    bytecode.extend_from_slice(&[0x00; 50]);

    bytecode
}
