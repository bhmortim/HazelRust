//! Example: Cluster discovery mechanisms
//!
//! Demonstrates various ways to discover Hazelcast cluster members.
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

    let _config = ClientConfig::new()
        .cluster_name("dev")
        .address("10.0.0.1:5701")
        .address("10.0.0.2:5701")
        .address("10.0.0.3:5701");

    println!("```rust");
    println!("let config = ClientConfig::new()");
    println!("    .cluster_name(\"dev\")");
    println!("    .address(\"10.0.0.1:5701\")");
    println!("    .address(\"10.0.0.2:5701\")");
    println!("    .address(\"10.0.0.3:5701\");");
    println!("```\n");
}

#[cfg(feature = "aws")]
fn aws_discovery() {
    use hazelcast_client::connection::AwsConfig;

    println!("2. AWS EC2 Discovery");
    println!("--------------------");
    println!("Discovers cluster members using EC2 instance tags.\n");

    let _config = ClientConfig::new()
        .cluster_name("production")
        .aws(
            AwsConfig::new()
                .region("us-east-1")
                .tag_key("hazelcast-cluster")
                .tag_value("prod-cluster")
                .security_group("sg-hazelcast")
                .iam_role("hazelcast-discovery-role"),
        );

    println!("```rust");
    println!("let config = ClientConfig::new()");
    println!("    .cluster_name(\"production\")");
    println!("    .aws(AwsConfig::new()");
    println!("        .region(\"us-east-1\")");
    println!("        .tag_key(\"hazelcast-cluster\")");
    println!("        .tag_value(\"prod-cluster\")");
    println!("        .security_group(\"sg-hazelcast\")");
    println!("        .iam_role(\"hazelcast-discovery-role\"));");
    println!("```\n");

    println!("Required IAM permissions:");
    println!("  - ec2:DescribeInstances");
    println!("  - ec2:DescribeSecurityGroups\n");
}

#[cfg(feature = "kubernetes")]
fn kubernetes_discovery() {
    use hazelcast_client::connection::KubernetesConfig;

    println!("3. Kubernetes Discovery");
    println!("-----------------------");
    println!("Discovers cluster members via Kubernetes API.\n");

    let _config = ClientConfig::new()
        .cluster_name("production")
        .kubernetes(
            KubernetesConfig::new()
                .namespace("hazelcast")
                .service_name("hazelcast-cluster")
                .service_port(5701)
                .pod_label_name("app")
                .pod_label_value("hazelcast"),
        );

    println!("```rust");
    println!("let config = ClientConfig::new()");
    println!("    .cluster_name(\"production\")");
    println!("    .kubernetes(KubernetesConfig::new()");
    println!("        .namespace(\"hazelcast\")");
    println!("        .service_name(\"hazelcast-cluster\")");
    println!("        .service_port(5701)");
    println!("        .pod_label_name(\"app\")");
    println!("        .pod_label_value(\"hazelcast\"));");
    println!("```\n");

    println!("Required RBAC permissions:");
    println!("  - pods: get, list");
    println!("  - services: get, list");
    println!("  - endpoints: get, list\n");
}

#[cfg(feature = "cloud")]
fn cloud_discovery() {
    use hazelcast_client::connection::CloudConfig;

    println!("4. Hazelcast Cloud Discovery");
    println!("----------------------------");
    println!("Connects to Hazelcast Cloud managed clusters.\n");

    let _config = ClientConfig::new()
        .cloud(
            CloudConfig::new()
                .cluster_name("my-cloud-cluster")
                .token("YOUR_DISCOVERY_TOKEN"),
        );

    println!("```rust");
    println!("let config = ClientConfig::new()");
    println!("    .cloud(CloudConfig::new()");
    println!("        .cluster_name(\"my-cloud-cluster\")");
    println!("        .token(\"YOUR_DISCOVERY_TOKEN\"));");
    println!("```\n");

    println!("Get your discovery token from the Hazelcast Cloud console:");
    println!("  https://cloud.hazelcast.com\n");
}
