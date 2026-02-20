//! Example: Cluster discovery mechanisms.
//!
//! Demonstrates various ways to discover Hazelcast cluster members,
//! including static addresses, AWS, Kubernetes, and Hazelcast Cloud.
//!
//! Run with:
//!   cargo run --example cluster_discovery
//!   cargo run --example cluster_discovery --features aws
//!   cargo run --example cluster_discovery --features kubernetes
//!   cargo run --example cluster_discovery --features cloud

use hazelcast_client::ClientConfig;

fn main() {
    println!("Hazelcast Cluster Discovery Examples\n");
    println!("=====================================\n");

    // 1. Static address discovery (default)
    static_discovery();

    // 2. AWS EC2 discovery
    #[cfg(feature = "aws")]
    aws_discovery();

    // 3. Kubernetes discovery
    #[cfg(feature = "kubernetes")]
    kubernetes_discovery();

    // 4. Hazelcast Cloud discovery
    #[cfg(feature = "cloud")]
    cloud_discovery();

    println!("\n=====================================");
    println!("Run with feature flags to see platform-specific examples:");
    println!("  --features aws");
    println!("  --features kubernetes");
    println!("  --features cloud");
}

fn static_discovery() {
    println!("1. Static Address Discovery");
    println!("---------------------------");
    println!("Use when cluster addresses are known and stable.\n");

    let _config = ClientConfig::builder()
        .cluster_name("dev")
        .add_address("10.0.0.1:5701".parse().unwrap())
        .add_address("10.0.0.2:5701".parse().unwrap())
        .add_address("10.0.0.3:5701".parse().unwrap())
        .build()
        .expect("valid config");

    println!("  ClientConfig::builder()");
    println!("      .cluster_name(\"dev\")");
    println!("      .add_address(\"10.0.0.1:5701\".parse()?)");
    println!("      .add_address(\"10.0.0.2:5701\".parse()?)");
    println!("      .add_address(\"10.0.0.3:5701\".parse()?)");
    println!("      .build()?;\n");
}

#[cfg(feature = "aws")]
fn aws_discovery() {
    use hazelcast_client::connection::AwsDiscoveryConfig;

    println!("2. AWS EC2 Discovery");
    println!("--------------------");
    println!("Discovers cluster members using EC2 instance tags.\n");

    let aws = AwsDiscoveryConfig::new("us-east-1")
        .with_tag("hazelcast-cluster", "prod-cluster");

    let _config = ClientConfig::builder()
        .cluster_name("production")
        .network(|n| n.aws_discovery(aws))
        .build()
        .expect("valid config");

    println!("  ClientConfig::builder()");
    println!("      .network(|n| n.aws_discovery(aws))");
    println!("      .build()?;\n");
    println!("Required IAM permissions:");
    println!("  - ec2:DescribeInstances");
    println!("  - ec2:DescribeSecurityGroups\n");
}

#[cfg(feature = "kubernetes")]
fn kubernetes_discovery() {
    use hazelcast_client::connection::KubernetesDiscoveryConfig;

    println!("3. Kubernetes Discovery");
    println!("-----------------------");
    println!("Discovers cluster members via Kubernetes API.\n");

    let k8s = KubernetesDiscoveryConfig::new()
        .namespace("hazelcast")
        .service_name("hazelcast-cluster")
        .port(5701);

    let _config = ClientConfig::builder()
        .cluster_name("production")
        .network(|n| n.kubernetes_discovery(k8s))
        .build()
        .expect("valid config");

    println!("  ClientConfig::builder()");
    println!("      .network(|n| n.kubernetes_discovery(k8s))");
    println!("      .build()?;\n");
    println!("Required RBAC permissions:");
    println!("  - pods: get, list");
    println!("  - services: get, list");
    println!("  - endpoints: get, list\n");
}

#[cfg(feature = "cloud")]
fn cloud_discovery() {
    use hazelcast_client::connection::CloudDiscoveryConfig;

    println!("4. Hazelcast Cloud Discovery");
    println!("----------------------------");
    println!("Connects to Hazelcast Cloud managed clusters.\n");

    let cloud = CloudDiscoveryConfig::new("YOUR_DISCOVERY_TOKEN");

    let _config = ClientConfig::builder()
        .cluster_name("my-cloud-cluster")
        .network(|n| n.cloud_discovery(cloud))
        .build()
        .expect("valid config");

    println!("  ClientConfig::builder()");
    println!("      .cluster_name(\"my-cloud-cluster\")");
    println!("      .network(|n| n.cloud_discovery(cloud))");
    println!("      .build()?;\n");
    println!("Get your discovery token from the Hazelcast Cloud console:");
    println!("  https://cloud.hazelcast.com\n");
}
