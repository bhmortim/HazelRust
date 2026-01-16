//! Query predicates for filtering distributed data.
//!
//! This module provides predicates for filtering entries in distributed
//! data structures like IMap. Predicates can be combined using logical
//! operators.
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client::query::*;
//!
//! // Simple equality check
//! let pred = Predicates::equal("age", &25i32)?;
//!
//! // Combined predicates
//! let pred = Predicates::and(vec![
//!     Predicates::greater_than("age", &18i32)?,
//!     Predicates::less_than("age", &65i32)?,
//! ]);
//! ```

pub mod aggregations;
pub mod index_config;
pub mod paging_predicate;
pub mod projections;

pub use aggregations::*;
pub use index_config::*;
pub use paging_predicate::{
    AnchorEntry, EntryKeyComparator, EntryValueComparator, IterationType, PagingComparator,
    PagingPredicate, PagingPredicateBuilder, PagingResult,
};
pub use projections::*;

use std::fmt::Debug;

use hazelcast_core::serialization::ObjectDataOutput;
use hazelcast_core::{HazelcastError, Result, Serializable};

/// Factory ID for built-in Hazelcast predicates.
pub const PREDICATE_FACTORY_ID: i32 = -20;

/// Class IDs for predicate types in the Hazelcast protocol.
pub mod class_ids {
    /// SQL predicate class ID.
    pub const SQL_PREDICATE: i32 = 0;
    /// And predicate class ID.
    pub const AND_PREDICATE: i32 = 1;
    /// Between predicate class ID.
    pub const BETWEEN_PREDICATE: i32 = 2;
    /// Equal predicate class ID.
    pub const EQUAL_PREDICATE: i32 = 3;
    /// Greater/less than predicate class ID.
    pub const GREATER_LESS_PREDICATE: i32 = 4;
    /// Like predicate class ID.
    pub const LIKE_PREDICATE: i32 = 5;
    /// Paging predicate class ID.
    pub const PAGING_PREDICATE: i32 = 6;
    /// In predicate class ID.
    pub const IN_PREDICATE: i32 = 7;
    /// Not equal predicate class ID.
    pub const NOT_EQUAL_PREDICATE: i32 = 9;
    /// Not predicate class ID.
    pub const NOT_PREDICATE: i32 = 10;
    /// Or predicate class ID.
    pub const OR_PREDICATE: i32 = 11;
    /// Regex predicate class ID.
    pub const REGEX_PREDICATE: i32 = 12;
    /// False predicate class ID.
    pub const FALSE_PREDICATE: i32 = 13;
    /// True predicate class ID.
    pub const TRUE_PREDICATE: i32 = 14;
}

/// Trait for predicates that can filter map entries.
///
/// Predicates are serialized using Hazelcast's IdentifiedDataSerializable format
/// with factory ID -20.
pub trait Predicate: Debug + Send + Sync {
    /// Returns the class ID for this predicate type.
    fn class_id(&self) -> i32;

    /// Writes the predicate-specific data to the output.
    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()>;

    /// Serializes this predicate to bytes including type information.
    fn to_predicate_data(&self) -> Result<Vec<u8>> {
        let mut output = ObjectDataOutput::new();
        write_i32(&mut output, PREDICATE_FACTORY_ID)?;
        write_i32(&mut output, self.class_id())?;
        self.write_data(&mut output)?;
        Ok(output.into_bytes())
    }
}

/// Helper to write an i32 in big-endian format.
fn write_i32(output: &mut ObjectDataOutput, value: i32) -> Result<()> {
    for byte in value.to_be_bytes() {
        byte.serialize(output)?;
    }
    Ok(())
}

/// Helper to write a bool.
fn write_bool(output: &mut ObjectDataOutput, value: bool) -> Result<()> {
    let byte: i8 = if value { 1 } else { 0 };
    byte.serialize(output)?;
    Ok(())
}

/// Helper to write a UTF string with length prefix.
fn write_string(output: &mut ObjectDataOutput, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    write_i32(output, bytes.len() as i32)?;
    for byte in bytes {
        (*byte as i8).serialize(output)?;
    }
    Ok(())
}

/// Helper to write serialized data bytes with length prefix.
fn write_data_bytes(output: &mut ObjectDataOutput, data: &[u8]) -> Result<()> {
    write_i32(output, data.len() as i32)?;
    for byte in data {
        (*byte as i8).serialize(output)?;
    }
    Ok(())
}

/// Serializes a value to bytes.
fn serialize_value<T: Serializable>(value: &T) -> Result<Vec<u8>> {
    let mut output = ObjectDataOutput::new();
    value.serialize(&mut output)?;
    Ok(output.into_bytes())
}

// ============================================================================
// Index Hints
// ============================================================================

/// Index hint for query optimization.
///
/// Specifies which index should be used when executing a query.
/// This is an optimization hint that may improve query performance
/// when the specified index exists.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::query::*;
///
/// let hint = IndexHint::new("age_idx");
/// let pred = Predicates::with_index_hint(
///     Predicates::greater_than("age", &18i32)?,
///     hint,
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexHint {
    index_name: String,
}

impl IndexHint {
    /// Creates a new index hint with the specified index name.
    pub fn new(index_name: impl Into<String>) -> Self {
        Self {
            index_name: index_name.into(),
        }
    }

    /// Returns the name of the index.
    pub fn index_name(&self) -> &str {
        &self.index_name
    }
}

/// A predicate wrapper that includes an index hint for query optimization.
///
/// Wraps another predicate and associates it with an index hint that
/// suggests which index should be used for the query.
#[derive(Debug)]
pub struct IndexedPredicate {
    inner: Box<dyn Predicate>,
    hint: IndexHint,
}

impl IndexedPredicate {
    /// Creates a new indexed predicate.
    pub fn new<P: Predicate + 'static>(predicate: P, hint: IndexHint) -> Self {
        Self {
            inner: Box::new(predicate),
            hint,
        }
    }

    /// Returns the index hint.
    pub fn hint(&self) -> &IndexHint {
        &self.hint
    }

    /// Returns a reference to the inner predicate.
    pub fn inner(&self) -> &dyn Predicate {
        self.inner.as_ref()
    }
}

impl Predicate for IndexedPredicate {
    fn class_id(&self) -> i32 {
        self.inner.class_id()
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        self.inner.write_data(output)
    }
}

// ============================================================================
// Predicate Implementations
// ============================================================================

/// A predicate that always evaluates to true.
#[derive(Debug, Clone, Default)]
pub struct TruePredicate;

impl TruePredicate {
    /// Creates a new true predicate.
    pub fn new() -> Self {
        Self
    }
}

impl Predicate for TruePredicate {
    fn class_id(&self) -> i32 {
        class_ids::TRUE_PREDICATE
    }

    fn write_data(&self, _output: &mut ObjectDataOutput) -> Result<()> {
        Ok(())
    }
}

/// A predicate that always evaluates to false.
#[derive(Debug, Clone, Default)]
pub struct FalsePredicate;

impl FalsePredicate {
    /// Creates a new false predicate.
    pub fn new() -> Self {
        Self
    }
}

impl Predicate for FalsePredicate {
    fn class_id(&self) -> i32 {
        class_ids::FALSE_PREDICATE
    }

    fn write_data(&self, _output: &mut ObjectDataOutput) -> Result<()> {
        Ok(())
    }
}

/// A predicate that checks for equality.
#[derive(Debug, Clone)]
pub struct EqualPredicate {
    attribute: String,
    value: Vec<u8>,
}

impl EqualPredicate {
    /// Creates a new equal predicate.
    pub fn new<V: Serializable>(attribute: impl Into<String>, value: &V) -> Result<Self> {
        Ok(Self {
            attribute: attribute.into(),
            value: serialize_value(value)?,
        })
    }
}

impl Predicate for EqualPredicate {
    fn class_id(&self) -> i32 {
        class_ids::EQUAL_PREDICATE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_string(output, &self.attribute)?;
        write_data_bytes(output, &self.value)?;
        Ok(())
    }
}

/// A predicate that checks for inequality.
#[derive(Debug, Clone)]
pub struct NotEqualPredicate {
    attribute: String,
    value: Vec<u8>,
}

impl NotEqualPredicate {
    /// Creates a new not-equal predicate.
    pub fn new<V: Serializable>(attribute: impl Into<String>, value: &V) -> Result<Self> {
        Ok(Self {
            attribute: attribute.into(),
            value: serialize_value(value)?,
        })
    }
}

impl Predicate for NotEqualPredicate {
    fn class_id(&self) -> i32 {
        class_ids::NOT_EQUAL_PREDICATE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_string(output, &self.attribute)?;
        write_data_bytes(output, &self.value)?;
        Ok(())
    }
}

/// A predicate that checks if a value is less than a threshold.
#[derive(Debug, Clone)]
pub struct LessThanPredicate {
    attribute: String,
    value: Vec<u8>,
    equal: bool,
}

impl LessThanPredicate {
    /// Creates a new less-than predicate.
    pub fn new<V: Serializable>(attribute: impl Into<String>, value: &V) -> Result<Self> {
        Ok(Self {
            attribute: attribute.into(),
            value: serialize_value(value)?,
            equal: false,
        })
    }

    /// Creates a new less-than-or-equal predicate.
    pub fn or_equal<V: Serializable>(attribute: impl Into<String>, value: &V) -> Result<Self> {
        Ok(Self {
            attribute: attribute.into(),
            value: serialize_value(value)?,
            equal: true,
        })
    }
}

impl Predicate for LessThanPredicate {
    fn class_id(&self) -> i32 {
        class_ids::GREATER_LESS_PREDICATE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_string(output, &self.attribute)?;
        write_data_bytes(output, &self.value)?;
        write_bool(output, self.equal)?;
        write_bool(output, true)?; // isLess = true
        Ok(())
    }
}

/// A predicate that checks if a value is greater than a threshold.
#[derive(Debug, Clone)]
pub struct GreaterThanPredicate {
    attribute: String,
    value: Vec<u8>,
    equal: bool,
}

impl GreaterThanPredicate {
    /// Creates a new greater-than predicate.
    pub fn new<V: Serializable>(attribute: impl Into<String>, value: &V) -> Result<Self> {
        Ok(Self {
            attribute: attribute.into(),
            value: serialize_value(value)?,
            equal: false,
        })
    }

    /// Creates a new greater-than-or-equal predicate.
    pub fn or_equal<V: Serializable>(attribute: impl Into<String>, value: &V) -> Result<Self> {
        Ok(Self {
            attribute: attribute.into(),
            value: serialize_value(value)?,
            equal: true,
        })
    }
}

impl Predicate for GreaterThanPredicate {
    fn class_id(&self) -> i32 {
        class_ids::GREATER_LESS_PREDICATE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_string(output, &self.attribute)?;
        write_data_bytes(output, &self.value)?;
        write_bool(output, self.equal)?;
        write_bool(output, false)?; // isLess = false
        Ok(())
    }
}

/// A predicate that checks if a value is between two bounds.
#[derive(Debug, Clone)]
pub struct BetweenPredicate {
    attribute: String,
    from: Vec<u8>,
    to: Vec<u8>,
}

impl BetweenPredicate {
    /// Creates a new between predicate (inclusive on both ends).
    pub fn new<V: Serializable>(
        attribute: impl Into<String>,
        from: &V,
        to: &V,
    ) -> Result<Self> {
        Ok(Self {
            attribute: attribute.into(),
            from: serialize_value(from)?,
            to: serialize_value(to)?,
        })
    }
}

impl Predicate for BetweenPredicate {
    fn class_id(&self) -> i32 {
        class_ids::BETWEEN_PREDICATE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_string(output, &self.attribute)?;
        write_data_bytes(output, &self.from)?;
        write_data_bytes(output, &self.to)?;
        Ok(())
    }
}

/// A predicate that checks if a value is in a set of values.
#[derive(Debug, Clone)]
pub struct InPredicate {
    attribute: String,
    values: Vec<Vec<u8>>,
}

impl InPredicate {
    /// Creates a new in predicate.
    pub fn new<V: Serializable>(
        attribute: impl Into<String>,
        values: &[V],
    ) -> Result<Self> {
        let serialized: Result<Vec<Vec<u8>>> = values
            .iter()
            .map(|v| serialize_value(v))
            .collect();
        Ok(Self {
            attribute: attribute.into(),
            values: serialized?,
        })
    }
}

impl Predicate for InPredicate {
    fn class_id(&self) -> i32 {
        class_ids::IN_PREDICATE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_string(output, &self.attribute)?;
        write_i32(output, self.values.len() as i32)?;
        for value in &self.values {
            write_data_bytes(output, value)?;
        }
        Ok(())
    }
}

/// A predicate that performs SQL LIKE pattern matching.
#[derive(Debug, Clone)]
pub struct LikePredicate {
    attribute: String,
    pattern: String,
}

impl LikePredicate {
    /// Creates a new like predicate.
    ///
    /// The pattern supports `%` as a multi-character wildcard and `_` as a
    /// single-character wildcard.
    pub fn new(attribute: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self {
            attribute: attribute.into(),
            pattern: pattern.into(),
        }
    }
}

impl Predicate for LikePredicate {
    fn class_id(&self) -> i32 {
        class_ids::LIKE_PREDICATE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_string(output, &self.attribute)?;
        write_string(output, &self.pattern)?;
        Ok(())
    }
}

/// A predicate that performs regular expression matching.
#[derive(Debug, Clone)]
pub struct RegexPredicate {
    attribute: String,
    pattern: String,
}

impl RegexPredicate {
    /// Creates a new regex predicate.
    pub fn new(attribute: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self {
            attribute: attribute.into(),
            pattern: pattern.into(),
        }
    }
}

impl Predicate for RegexPredicate {
    fn class_id(&self) -> i32 {
        class_ids::REGEX_PREDICATE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_string(output, &self.attribute)?;
        write_string(output, &self.pattern)?;
        Ok(())
    }
}

/// A predicate for querying JSON documents using JSON path expressions.
///
/// This predicate allows querying `HazelcastJsonValue` entries using JSON path
/// expressions like `$.user.name` or `$.items[0].price`.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::query::*;
///
/// // Check if a JSON field equals a value
/// let pred = JsonPredicate::value_equals("$.user.name", "John");
///
/// // Check if a JSON field contains a substring
/// let pred = JsonPredicate::contains("$.description", "important");
/// ```
#[derive(Debug, Clone)]
pub struct JsonPredicate {
    sql: String,
}

impl JsonPredicate {
    /// Creates a predicate that checks if the value at the JSON path equals the specified value.
    ///
    /// # Arguments
    /// * `path` - A JSON path expression (e.g., `$.user.name`)
    /// * `value` - The value to compare against
    pub fn value_equals(path: impl Into<String>, value: impl Into<String>) -> Self {
        let path = path.into();
        let value = Self::escape_sql_string(&value.into());
        Self {
            sql: format!("JSON_VALUE(this, '{}') = '{}'", path, value),
        }
    }

    /// Creates a predicate that checks if the JSON value at the path contains the substring.
    ///
    /// # Arguments
    /// * `path` - A JSON path expression (e.g., `$.user.name`)
    /// * `substring` - The substring to search for
    pub fn contains(path: impl Into<String>, substring: impl Into<String>) -> Self {
        let path = path.into();
        let substring = Self::escape_sql_string(&substring.into());
        Self {
            sql: format!("JSON_VALUE(this, '{}') LIKE '%{}%'", path, substring),
        }
    }

    /// Creates a predicate using a raw SQL expression for JSON queries.
    ///
    /// Use this for complex JSON queries not covered by other methods.
    pub fn raw_sql(sql: impl Into<String>) -> Self {
        Self { sql: sql.into() }
    }

    fn escape_sql_string(s: &str) -> String {
        s.replace('\'', "''")
    }
}

impl Predicate for JsonPredicate {
    fn class_id(&self) -> i32 {
        class_ids::SQL_PREDICATE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_string(output, &self.sql)?;
        Ok(())
    }
}

/// A predicate that uses a SQL WHERE clause.
#[derive(Debug, Clone)]
pub struct SqlPredicate {
    sql: String,
}

impl SqlPredicate {
    /// Creates a new SQL predicate.
    ///
    /// The SQL string should be a valid WHERE clause expression.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let pred = SqlPredicate::new("age > 18 AND status = 'active'");
    /// ```
    pub fn new(sql: impl Into<String>) -> Self {
        Self { sql: sql.into() }
    }
}

impl Predicate for SqlPredicate {
    fn class_id(&self) -> i32 {
        class_ids::SQL_PREDICATE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_string(output, &self.sql)?;
        Ok(())
    }
}

// ============================================================================
// Compound Predicates
// ============================================================================

/// A predicate that combines multiple predicates with AND logic.
#[derive(Debug)]
pub struct AndPredicate {
    predicates: Vec<Box<dyn Predicate>>,
}

impl AndPredicate {
    /// Creates a new AND predicate from a list of predicates.
    pub fn new(predicates: Vec<Box<dyn Predicate>>) -> Self {
        Self { predicates }
    }

    /// Creates an AND predicate from two predicates.
    pub fn of<P1: Predicate + 'static, P2: Predicate + 'static>(left: P1, right: P2) -> Self {
        Self {
            predicates: vec![Box::new(left), Box::new(right)],
        }
    }
}

impl Predicate for AndPredicate {
    fn class_id(&self) -> i32 {
        class_ids::AND_PREDICATE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_i32(output, self.predicates.len() as i32)?;
        for predicate in &self.predicates {
            let data = predicate.to_predicate_data()?;
            write_data_bytes(output, &data)?;
        }
        Ok(())
    }
}

/// A predicate that combines multiple predicates with OR logic.
#[derive(Debug)]
pub struct OrPredicate {
    predicates: Vec<Box<dyn Predicate>>,
}

impl OrPredicate {
    /// Creates a new OR predicate from a list of predicates.
    pub fn new(predicates: Vec<Box<dyn Predicate>>) -> Self {
        Self { predicates }
    }

    /// Creates an OR predicate from two predicates.
    pub fn of<P1: Predicate + 'static, P2: Predicate + 'static>(left: P1, right: P2) -> Self {
        Self {
            predicates: vec![Box::new(left), Box::new(right)],
        }
    }
}

impl Predicate for OrPredicate {
    fn class_id(&self) -> i32 {
        class_ids::OR_PREDICATE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_i32(output, self.predicates.len() as i32)?;
        for predicate in &self.predicates {
            let data = predicate.to_predicate_data()?;
            write_data_bytes(output, &data)?;
        }
        Ok(())
    }
}

/// A predicate that negates another predicate.
#[derive(Debug)]
pub struct NotPredicate {
    inner: Box<dyn Predicate>,
}

impl NotPredicate {
    /// Creates a new NOT predicate.
    pub fn new<P: Predicate + 'static>(predicate: P) -> Self {
        Self {
            inner: Box::new(predicate),
        }
    }
}

impl Predicate for NotPredicate {
    fn class_id(&self) -> i32 {
        class_ids::NOT_PREDICATE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        let data = self.inner.to_predicate_data()?;
        write_data_bytes(output, &data)?;
        Ok(())
    }
}

// ============================================================================
// Predicates Factory
// ============================================================================

/// Factory for creating predicates.
///
/// Provides convenient static methods for creating all predicate types.
pub struct Predicates;

impl Predicates {
    /// Returns a predicate that always evaluates to true.
    pub fn always_true() -> TruePredicate {
        TruePredicate::new()
    }

    /// Returns a predicate that always evaluates to false.
    pub fn always_false() -> FalsePredicate {
        FalsePredicate::new()
    }

    /// Returns a predicate that checks for equality.
    pub fn equal<V: Serializable>(attribute: impl Into<String>, value: &V) -> Result<EqualPredicate> {
        EqualPredicate::new(attribute, value)
    }

    /// Returns a predicate that checks for inequality.
    pub fn not_equal<V: Serializable>(attribute: impl Into<String>, value: &V) -> Result<NotEqualPredicate> {
        NotEqualPredicate::new(attribute, value)
    }

    /// Returns a predicate that checks if value is less than threshold.
    pub fn less_than<V: Serializable>(attribute: impl Into<String>, value: &V) -> Result<LessThanPredicate> {
        LessThanPredicate::new(attribute, value)
    }

    /// Returns a predicate that checks if value is less than or equal to threshold.
    pub fn less_than_or_equal<V: Serializable>(attribute: impl Into<String>, value: &V) -> Result<LessThanPredicate> {
        LessThanPredicate::or_equal(attribute, value)
    }

    /// Returns a predicate that checks if value is greater than threshold.
    pub fn greater_than<V: Serializable>(attribute: impl Into<String>, value: &V) -> Result<GreaterThanPredicate> {
        GreaterThanPredicate::new(attribute, value)
    }

    /// Returns a predicate that checks if value is greater than or equal to threshold.
    pub fn greater_than_or_equal<V: Serializable>(attribute: impl Into<String>, value: &V) -> Result<GreaterThanPredicate> {
        GreaterThanPredicate::or_equal(attribute, value)
    }

    /// Returns a predicate that checks if value is between two bounds (inclusive).
    pub fn between<V: Serializable>(
        attribute: impl Into<String>,
        from: &V,
        to: &V,
    ) -> Result<BetweenPredicate> {
        BetweenPredicate::new(attribute, from, to)
    }

    /// Returns a predicate that checks if value is in a set of values.
    pub fn is_in<V: Serializable>(
        attribute: impl Into<String>,
        values: &[V],
    ) -> Result<InPredicate> {
        InPredicate::new(attribute, values)
    }

    /// Returns a predicate that performs SQL LIKE pattern matching.
    pub fn like(attribute: impl Into<String>, pattern: impl Into<String>) -> LikePredicate {
        LikePredicate::new(attribute, pattern)
    }

    /// Returns a predicate that performs regex pattern matching.
    pub fn regex(attribute: impl Into<String>, pattern: impl Into<String>) -> RegexPredicate {
        RegexPredicate::new(attribute, pattern)
    }

    /// Returns a predicate using a SQL WHERE clause.
    pub fn sql(expression: impl Into<String>) -> SqlPredicate {
        SqlPredicate::new(expression)
    }

    /// Returns a predicate that checks if the JSON value at the path equals the specified value.
    ///
    /// # Arguments
    /// * `path` - A JSON path expression (e.g., `$.user.name`)
    /// * `value` - The value to compare against
    pub fn json_value_equals(
        path: impl Into<String>,
        value: impl Into<String>,
    ) -> JsonPredicate {
        JsonPredicate::value_equals(path, value)
    }

    /// Returns a predicate that checks if the JSON value at the path contains the substring.
    ///
    /// # Arguments
    /// * `path` - A JSON path expression (e.g., `$.user.name`)
    /// * `substring` - The substring to search for
    pub fn json_contains(
        path: impl Into<String>,
        substring: impl Into<String>,
    ) -> JsonPredicate {
        JsonPredicate::contains(path, substring)
    }

    /// Returns a predicate that combines predicates with AND logic.
    pub fn and(predicates: Vec<Box<dyn Predicate>>) -> AndPredicate {
        AndPredicate::new(predicates)
    }

    /// Returns a predicate that combines predicates with OR logic.
    pub fn or(predicates: Vec<Box<dyn Predicate>>) -> OrPredicate {
        OrPredicate::new(predicates)
    }

    /// Returns a predicate that negates another predicate.
    pub fn not<P: Predicate + 'static>(predicate: P) -> NotPredicate {
        NotPredicate::new(predicate)
    }

    /// Wraps a predicate with an index hint for query optimization.
    ///
    /// The index hint suggests which index should be used when executing
    /// the query, potentially improving performance.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::*;
    ///
    /// let pred = Predicates::with_index_hint(
    ///     Predicates::greater_than("age", &18i32)?,
    ///     IndexHint::new("age_idx"),
    /// );
    /// ```
    pub fn with_index_hint<P: Predicate + 'static>(
        predicate: P,
        hint: IndexHint,
    ) -> IndexedPredicate {
        IndexedPredicate::new(predicate, hint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_true_predicate() {
        let pred = TruePredicate::new();
        assert_eq!(pred.class_id(), class_ids::TRUE_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_false_predicate() {
        let pred = FalsePredicate::new();
        assert_eq!(pred.class_id(), class_ids::FALSE_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_equal_predicate() {
        let pred = EqualPredicate::new("name", &"John".to_string()).unwrap();
        assert_eq!(pred.class_id(), class_ids::EQUAL_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_not_equal_predicate() {
        let pred = NotEqualPredicate::new("status", &"inactive".to_string()).unwrap();
        assert_eq!(pred.class_id(), class_ids::NOT_EQUAL_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_less_than_predicate() {
        let pred = LessThanPredicate::new("age", &"30".to_string()).unwrap();
        assert_eq!(pred.class_id(), class_ids::GREATER_LESS_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_greater_than_predicate() {
        let pred = GreaterThanPredicate::new("score", &"100".to_string()).unwrap();
        assert_eq!(pred.class_id(), class_ids::GREATER_LESS_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_between_predicate() {
        let pred = BetweenPredicate::new("price", &"10".to_string(), &"100".to_string()).unwrap();
        assert_eq!(pred.class_id(), class_ids::BETWEEN_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_in_predicate() {
        let values = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let pred = InPredicate::new("category", &values).unwrap();
        assert_eq!(pred.class_id(), class_ids::IN_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_like_predicate() {
        let pred = LikePredicate::new("name", "John%");
        assert_eq!(pred.class_id(), class_ids::LIKE_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_regex_predicate() {
        let pred = RegexPredicate::new("email", r".*@example\.com");
        assert_eq!(pred.class_id(), class_ids::REGEX_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_sql_predicate() {
        let pred = SqlPredicate::new("age > 18 AND status = 'active'");
        assert_eq!(pred.class_id(), class_ids::SQL_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_and_predicate() {
        let pred = AndPredicate::of(TruePredicate::new(), FalsePredicate::new());
        assert_eq!(pred.class_id(), class_ids::AND_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_or_predicate() {
        let pred = OrPredicate::of(TruePredicate::new(), FalsePredicate::new());
        assert_eq!(pred.class_id(), class_ids::OR_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_not_predicate() {
        let pred = NotPredicate::new(TruePredicate::new());
        assert_eq!(pred.class_id(), class_ids::NOT_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_predicates_factory() {
        let _ = Predicates::always_true();
        let _ = Predicates::always_false();
        let _ = Predicates::equal("a", &"b".to_string()).unwrap();
        let _ = Predicates::not_equal("a", &"b".to_string()).unwrap();
        let _ = Predicates::less_than("a", &"b".to_string()).unwrap();
        let _ = Predicates::greater_than("a", &"b".to_string()).unwrap();
        let _ = Predicates::between("a", &"1".to_string(), &"10".to_string()).unwrap();
        let _ = Predicates::like("a", "b%");
        let _ = Predicates::regex("a", ".*");
        let _ = Predicates::sql("a = 1");
    }

    #[test]
    fn test_json_predicate_value_equals() {
        let pred = JsonPredicate::value_equals("$.user.name", "John");
        assert_eq!(pred.class_id(), class_ids::SQL_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_json_predicate_contains() {
        let pred = JsonPredicate::contains("$.description", "important");
        assert_eq!(pred.class_id(), class_ids::SQL_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_json_predicate_escapes_quotes() {
        let pred = JsonPredicate::value_equals("$.name", "O'Brien");
        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_json_predicate_raw_sql() {
        let pred = JsonPredicate::raw_sql("JSON_VALUE(this, '$.age') > 18");
        assert_eq!(pred.class_id(), class_ids::SQL_PREDICATE);

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_predicates_json_factory() {
        let _ = Predicates::json_value_equals("$.user.name", "John");
        let _ = Predicates::json_contains("$.tags", "rust");
    }

    #[test]
    fn test_complex_predicate() {
        let age_check = Box::new(GreaterThanPredicate::or_equal("age", &"18".to_string()).unwrap());
        let status_check = Box::new(EqualPredicate::new("status", &"active".to_string()).unwrap());

        let and_pred = Predicates::and(vec![age_check, status_check]);
        let final_pred = Predicates::not(and_pred);

        let data = final_pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_predicate_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TruePredicate>();
        assert_send_sync::<FalsePredicate>();
        assert_send_sync::<EqualPredicate>();
        assert_send_sync::<LikePredicate>();
        assert_send_sync::<SqlPredicate>();
    }

    #[test]
    fn test_index_hint_new() {
        let hint = IndexHint::new("my_index");
        assert_eq!(hint.index_name(), "my_index");
    }

    #[test]
    fn test_index_hint_equality() {
        let hint1 = IndexHint::new("idx");
        let hint2 = IndexHint::new("idx");
        let hint3 = IndexHint::new("other");
        assert_eq!(hint1, hint2);
        assert_ne!(hint1, hint3);
    }

    #[test]
    fn test_indexed_predicate() {
        let pred = EqualPredicate::new("name", &"John".to_string()).unwrap();
        let hint = IndexHint::new("name_idx");
        let indexed = IndexedPredicate::new(pred, hint);

        assert_eq!(indexed.hint().index_name(), "name_idx");
        assert_eq!(indexed.class_id(), class_ids::EQUAL_PREDICATE);

        let data = indexed.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_predicates_with_index_hint() {
        let pred = Predicates::greater_than("age", &18i32).unwrap();
        let indexed = Predicates::with_index_hint(pred, IndexHint::new("age_idx"));

        assert_eq!(indexed.hint().index_name(), "age_idx");
        assert_eq!(indexed.class_id(), class_ids::GREATER_LESS_PREDICATE);
    }

    #[test]
    fn test_indexed_predicate_with_compound() {
        let and_pred = AndPredicate::of(TruePredicate::new(), FalsePredicate::new());
        let indexed = Predicates::with_index_hint(and_pred, IndexHint::new("compound_idx"));

        assert_eq!(indexed.class_id(), class_ids::AND_PREDICATE);
        let data = indexed.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }
}
