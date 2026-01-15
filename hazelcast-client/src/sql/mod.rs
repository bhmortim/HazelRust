//! SQL query execution types and infrastructure.

use std::collections::HashMap;
use std::time::Duration;

/// SQL column data types supported by Hazelcast.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum SqlColumnType {
    /// VARCHAR type for strings.
    Varchar = 0,
    /// BOOLEAN type.
    Boolean = 1,
    /// TINYINT (8-bit signed integer).
    TinyInt = 2,
    /// SMALLINT (16-bit signed integer).
    SmallInt = 3,
    /// INTEGER (32-bit signed integer).
    Integer = 4,
    /// BIGINT (64-bit signed integer).
    BigInt = 5,
    /// DECIMAL for arbitrary precision numbers.
    Decimal = 6,
    /// REAL (32-bit floating point).
    Real = 7,
    /// DOUBLE (64-bit floating point).
    Double = 8,
    /// DATE type.
    Date = 9,
    /// TIME type.
    Time = 10,
    /// TIMESTAMP type.
    Timestamp = 11,
    /// TIMESTAMP WITH TIME ZONE type.
    TimestampWithTimeZone = 12,
    /// OBJECT type for arbitrary serialized objects.
    Object = 13,
    /// NULL type.
    Null = 14,
    /// JSON type.
    Json = 15,
    /// ROW type for nested rows.
    Row = 16,
}

impl SqlColumnType {
    /// Creates a column type from its protocol integer value.
    pub fn from_id(id: i32) -> Option<Self> {
        match id {
            0 => Some(Self::Varchar),
            1 => Some(Self::Boolean),
            2 => Some(Self::TinyInt),
            3 => Some(Self::SmallInt),
            4 => Some(Self::Integer),
            5 => Some(Self::BigInt),
            6 => Some(Self::Decimal),
            7 => Some(Self::Real),
            8 => Some(Self::Double),
            9 => Some(Self::Date),
            10 => Some(Self::Time),
            11 => Some(Self::Timestamp),
            12 => Some(Self::TimestampWithTimeZone),
            13 => Some(Self::Object),
            14 => Some(Self::Null),
            15 => Some(Self::Json),
            16 => Some(Self::Row),
            _ => None,
        }
    }

    /// Returns the protocol integer value for this column type.
    pub fn id(&self) -> i32 {
        *self as i32
    }
}

/// Metadata for a single SQL column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlColumnMetadata {
    name: String,
    column_type: SqlColumnType,
    nullable: bool,
}

impl SqlColumnMetadata {
    /// Creates new column metadata.
    pub fn new(name: String, column_type: SqlColumnType, nullable: bool) -> Self {
        Self {
            name,
            column_type,
            nullable,
        }
    }

    /// Returns the column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the column type.
    pub fn column_type(&self) -> SqlColumnType {
        self.column_type
    }

    /// Returns whether the column is nullable.
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
}

/// Metadata for all columns in a SQL result set.
#[derive(Debug, Clone)]
pub struct SqlRowMetadata {
    columns: Vec<SqlColumnMetadata>,
    column_index: HashMap<String, usize>,
}

impl SqlRowMetadata {
    /// Creates new row metadata from a list of columns.
    pub fn new(columns: Vec<SqlColumnMetadata>) -> Self {
        let column_index = columns
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();
        Self {
            columns,
            column_index,
        }
    }

    /// Returns the number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Returns the column metadata at the given index.
    pub fn column(&self, index: usize) -> Option<&SqlColumnMetadata> {
        self.columns.get(index)
    }

    /// Returns the column index for a given column name.
    pub fn find_column(&self, name: &str) -> Option<usize> {
        self.column_index.get(name).copied()
    }

    /// Returns an iterator over all column metadata.
    pub fn columns(&self) -> impl Iterator<Item = &SqlColumnMetadata> {
        self.columns.iter()
    }
}

/// A value that can appear in a SQL row.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    /// Null value.
    Null,
    /// VARCHAR string value.
    Varchar(String),
    /// Boolean value.
    Boolean(bool),
    /// TINYINT value.
    TinyInt(i8),
    /// SMALLINT value.
    SmallInt(i16),
    /// INTEGER value.
    Integer(i32),
    /// BIGINT value.
    BigInt(i64),
    /// REAL value.
    Real(f32),
    /// DOUBLE value.
    Double(f64),
    /// Raw bytes for types not directly mapped.
    Bytes(Vec<u8>),
}

/// A single row from a SQL result set.
#[derive(Debug, Clone)]
pub struct SqlRow {
    metadata: SqlRowMetadata,
    values: Vec<SqlValue>,
}

impl SqlRow {
    /// Creates a new SQL row.
    pub fn new(metadata: SqlRowMetadata, values: Vec<SqlValue>) -> Self {
        Self { metadata, values }
    }

    /// Returns the row metadata.
    pub fn metadata(&self) -> &SqlRowMetadata {
        &self.metadata
    }

    /// Returns the value at the given column index.
    pub fn get(&self, index: usize) -> Option<&SqlValue> {
        self.values.get(index)
    }

    /// Returns the value for the given column name.
    pub fn get_by_name(&self, name: &str) -> Option<&SqlValue> {
        self.metadata
            .find_column(name)
            .and_then(|idx| self.values.get(idx))
    }

    /// Returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if the row has no values.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// A SQL statement with query text and optional parameters.
#[derive(Debug, Clone)]
pub struct SqlStatement {
    query: String,
    parameters: Vec<SqlValue>,
    cursor_buffer_size: Option<i32>,
    timeout: Option<Duration>,
    schema: Option<String>,
}

impl SqlStatement {
    /// Creates a new SQL statement with the given query.
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            parameters: Vec::new(),
            cursor_buffer_size: None,
            timeout: None,
            schema: None,
        }
    }

    /// Sets the query parameters.
    pub fn with_parameters(mut self, parameters: Vec<SqlValue>) -> Self {
        self.parameters = parameters;
        self
    }

    /// Adds a single parameter to the statement.
    pub fn add_parameter(mut self, parameter: SqlValue) -> Self {
        self.parameters.push(parameter);
        self
    }

    /// Sets the cursor buffer size (number of rows fetched per batch).
    pub fn with_cursor_buffer_size(mut self, size: i32) -> Self {
        self.cursor_buffer_size = Some(size);
        self
    }

    /// Sets the statement timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the schema to use for unqualified object names.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Returns the query string.
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Returns the query parameters.
    pub fn parameters(&self) -> &[SqlValue] {
        &self.parameters
    }

    /// Returns the cursor buffer size.
    pub fn cursor_buffer_size(&self) -> Option<i32> {
        self.cursor_buffer_size
    }

    /// Returns the statement timeout.
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Returns the schema.
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_column_type_roundtrip() {
        for id in 0..=16 {
            let col_type = SqlColumnType::from_id(id).unwrap();
            assert_eq!(col_type.id(), id);
        }
        assert!(SqlColumnType::from_id(100).is_none());
    }

    #[test]
    fn test_sql_row_metadata_find_column() {
        let columns = vec![
            SqlColumnMetadata::new("id".to_string(), SqlColumnType::BigInt, false),
            SqlColumnMetadata::new("name".to_string(), SqlColumnType::Varchar, true),
        ];
        let metadata = SqlRowMetadata::new(columns);

        assert_eq!(metadata.column_count(), 2);
        assert_eq!(metadata.find_column("id"), Some(0));
        assert_eq!(metadata.find_column("name"), Some(1));
        assert_eq!(metadata.find_column("missing"), None);
    }

    #[test]
    fn test_sql_row_get_by_name() {
        let columns = vec![
            SqlColumnMetadata::new("id".to_string(), SqlColumnType::BigInt, false),
            SqlColumnMetadata::new("name".to_string(), SqlColumnType::Varchar, true),
        ];
        let metadata = SqlRowMetadata::new(columns);
        let values = vec![SqlValue::BigInt(42), SqlValue::Varchar("test".to_string())];
        let row = SqlRow::new(metadata, values);

        assert_eq!(row.get_by_name("id"), Some(&SqlValue::BigInt(42)));
        assert_eq!(
            row.get_by_name("name"),
            Some(&SqlValue::Varchar("test".to_string()))
        );
        assert_eq!(row.get_by_name("missing"), None);
    }

    #[test]
    fn test_sql_statement_builder() {
        let stmt = SqlStatement::new("SELECT * FROM users WHERE id = ?")
            .add_parameter(SqlValue::BigInt(1))
            .with_cursor_buffer_size(100)
            .with_timeout(Duration::from_secs(30))
            .with_schema("public");

        assert_eq!(stmt.query(), "SELECT * FROM users WHERE id = ?");
        assert_eq!(stmt.parameters().len(), 1);
        assert_eq!(stmt.cursor_buffer_size(), Some(100));
        assert_eq!(stmt.timeout(), Some(Duration::from_secs(30)));
        assert_eq!(stmt.schema(), Some("public"));
    }
}
