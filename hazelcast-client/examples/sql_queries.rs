//! Example: SQL queries against Hazelcast maps.
//!
//! Demonstrates the SQL service for querying distributed maps, including
//! creating mappings, inserting data, filtering, and aggregation.
//!
//! Run with: `cargo run --example sql_queries`
//!
//! Requires a Hazelcast cluster running on localhost:5701.

use hazelcast_client::{ClientConfig, HazelcastClient, SqlStatement, SqlValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .cluster_name("dev")
        .add_address("127.0.0.1:5701".parse()?)
        .build()?;

    let client = HazelcastClient::new(config).await?;

    // Create mapping for the employees map
    let sql = client.sql();

    // Create the mapping (required for SQL access)
    sql.execute(SqlStatement::new(
        r#"
        CREATE OR REPLACE MAPPING employees (
            __key BIGINT,
            name VARCHAR,
            department VARCHAR,
            salary BIGINT
        )
        TYPE IMap
        OPTIONS (
            'keyFormat' = 'bigint',
            'valueFormat' = 'json-flat'
        )
        "#,
    ))
    .await?;

    println!("Created SQL mapping for 'employees' map\n");

    // Insert sample data
    let employees = vec![
        (1, "Alice", "Engineering", 95000),
        (2, "Bob", "Engineering", 87000),
        (3, "Carol", "Marketing", 72000),
        (4, "David", "Engineering", 110000),
        (5, "Eve", "Marketing", 68000),
        (6, "Frank", "Sales", 82000),
    ];

    for (id, name, dept, salary) in &employees {
        sql.execute(SqlStatement::new(format!(
            "INSERT INTO employees VALUES ({}, '{}', '{}', {})",
            id, name, dept, salary
        )))
        .await?;
    }

    println!("Inserted {} employees\n", employees.len());

    // Query 1: Select all
    println!("--- All Employees ---");
    let mut result = sql
        .execute(SqlStatement::new(
            "SELECT __key, name, department, salary FROM employees ORDER BY __key",
        ))
        .await?;

    while let Some(row) = result.next_row().await? {
        let key = row.get_by_name("__key").map(|v| format!("{:?}", v)).unwrap_or_default();
        let name = row.get_by_name("name").map(|v| format!("{:?}", v)).unwrap_or_default();
        let dept = row.get_by_name("department").map(|v| format!("{:?}", v)).unwrap_or_default();
        let salary = row.get_by_name("salary").map(|v| format!("{:?}", v)).unwrap_or_default();
        println!("  {} {} ({}) - {}", key, name, dept, salary);
    }

    // Query 2: Filter by department
    println!("\n--- Engineering Department ---");
    let mut result = sql
        .execute(SqlStatement::new(
            "SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY salary DESC",
        ))
        .await?;

    while let Some(row) = result.next_row().await? {
        let name = row.get_by_name("name").map(|v| format!("{:?}", v)).unwrap_or_default();
        let salary = row.get_by_name("salary").map(|v| format!("{:?}", v)).unwrap_or_default();
        println!("  {} - {}", name, salary);
    }

    // Query 3: Aggregation
    println!("\n--- Department Statistics ---");
    let mut result = sql
        .execute(SqlStatement::new(
            r#"
            SELECT
                department,
                COUNT(*) as employee_count,
                AVG(salary) as avg_salary,
                MAX(salary) as max_salary
            FROM employees
            GROUP BY department
            ORDER BY avg_salary DESC
            "#,
        ))
        .await?;

    while let Some(row) = result.next_row().await? {
        let dept = row.get_by_name("department").map(|v| format!("{:?}", v)).unwrap_or_default();
        let count = row.get_by_name("employee_count").map(|v| format!("{:?}", v)).unwrap_or_default();
        let avg = row.get_by_name("avg_salary").map(|v| format!("{:?}", v)).unwrap_or_default();
        let max = row.get_by_name("max_salary").map(|v| format!("{:?}", v)).unwrap_or_default();
        println!("  {}: {} employees, avg {}, max {}", dept, count, avg, max);
    }

    // Query 4: High earners
    println!("\n--- High Earners (salary > $80,000) ---");
    let mut result = sql
        .execute(SqlStatement::new(
            "SELECT name, salary FROM employees WHERE salary > 80000 ORDER BY salary DESC",
        ))
        .await?;

    while let Some(row) = result.next_row().await? {
        let name = row.get_by_name("name").map(|v| format!("{:?}", v)).unwrap_or_default();
        let salary = row.get_by_name("salary").map(|v| format!("{:?}", v)).unwrap_or_default();
        println!("  {} - {}", name, salary);
    }

    // Clean up
    sql.execute(SqlStatement::new("DROP MAPPING IF EXISTS employees"))
        .await?;
    let map = client.get_map::<i64, String>("employees");
    map.clear().await?;
    client.shutdown().await?;

    println!("\nDone!");
    Ok(())
}
