//! Aggregation functions for distributed data processing.
//!
//! This module provides aggregators for computing aggregate values over
//! distributed data structures like IMap.
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client::query::{Aggregators, Predicates};
//!
//! // Count all entries
//! let count = map.aggregate(&Aggregators::count()).await?;
//!
//! // Sum values with a predicate filter
//! let predicate = Predicates::greater_than("age", &18i32)?;
//! let sum = map.aggregate_with_predicate(&Aggregators::sum("salary"), &predicate).await?;
//! ```

use std::fmt::Debug;
use std::marker::PhantomData;

use hazelcast_core::serialization::{DataOutput, ObjectDataOutput};
use hazelcast_core::{Result, Serializable};

/// Factory ID for built-in Hazelcast aggregators.
pub const AGGREGATOR_FACTORY_ID: i32 = -29;

/// Class IDs for aggregator types in the Hazelcast protocol.
pub mod class_ids {
    /// Count aggregator class ID.
    pub const COUNT: i32 = 0;
    /// Double average aggregator class ID.
    pub const DOUBLE_AVG: i32 = 1;
    /// Double sum aggregator class ID.
    pub const DOUBLE_SUM: i32 = 2;
    /// Fixed-point sum aggregator class ID.
    pub const FIXED_POINT_SUM: i32 = 3;
    /// Floating-point sum aggregator class ID.
    pub const FLOATING_POINT_SUM: i32 = 4;
    /// Max aggregator class ID.
    pub const MAX: i32 = 5;
    /// Min aggregator class ID.
    pub const MIN: i32 = 6;
    /// Integer average aggregator class ID.
    pub const INT_AVG: i32 = 7;
    /// Integer sum aggregator class ID.
    pub const INT_SUM: i32 = 8;
    /// Long average aggregator class ID.
    pub const LONG_AVG: i32 = 9;
    /// Long sum aggregator class ID.
    pub const LONG_SUM: i32 = 10;
    /// Number average aggregator class ID.
    pub const NUMBER_AVG: i32 = 11;
    /// Distinct values aggregator class ID.
    pub const DISTINCT: i32 = 12;
    /// Max by aggregator class ID.
    pub const MAX_BY: i32 = 13;
    /// Min by aggregator class ID.
    pub const MIN_BY: i32 = 14;
}

/// Trait for aggregators that compute aggregate values over map entries.
///
/// Aggregators are serialized using Hazelcast's IdentifiedDataSerializable format
/// with factory ID -29.
pub trait Aggregator: Debug + Send + Sync {
    /// The output type of this aggregator.
    type Output;

    /// Returns the class ID for this aggregator type.
    fn class_id(&self) -> i32;

    /// Writes the aggregator-specific data to the output.
    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()>;

    /// Serializes this aggregator to bytes including type information.
    fn to_aggregator_data(&self) -> Result<Vec<u8>> {
        let mut output = ObjectDataOutput::new();
        write_i32(&mut output, AGGREGATOR_FACTORY_ID)?;
        write_i32(&mut output, self.class_id())?;
        self.write_data(&mut output)?;
        Ok(output.into_bytes())
    }
}

/// Helper to write an i32 in big-endian format.
fn write_i32(output: &mut ObjectDataOutput, value: i32) -> Result<()> {
    for byte in value.to_be_bytes() {
        output.write_byte(byte as i8)?;
    }
    Ok(())
}

/// Helper to write a UTF string with length prefix.
fn write_string(output: &mut ObjectDataOutput, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    write_i32(output, bytes.len() as i32)?;
    for byte in bytes {
        output.write_byte(*byte as i8)?;
    }
    Ok(())
}

/// Helper to write an optional attribute name.
fn write_attribute(output: &mut ObjectDataOutput, attribute: &Option<String>) -> Result<()> {
    match attribute {
        Some(attr) => {
            (1i8).serialize(output)?;
            write_string(output, attr)?;
        }
        None => {
            (0i8).serialize(output)?;
        }
    }
    Ok(())
}

// ============================================================================
// Aggregator Implementations
// ============================================================================

/// An aggregator that counts entries.
#[derive(Debug, Clone, Default)]
pub struct CountAggregator {
    attribute: Option<String>,
}

impl CountAggregator {
    /// Creates a new count aggregator that counts all entries.
    pub fn new() -> Self {
        Self { attribute: None }
    }

    /// Creates a new count aggregator that counts non-null values of an attribute.
    pub fn for_attribute(attribute: impl Into<String>) -> Self {
        Self {
            attribute: Some(attribute.into()),
        }
    }
}

impl Aggregator for CountAggregator {
    type Output = i64;

    fn class_id(&self) -> i32 {
        class_ids::COUNT
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_attribute(output, &self.attribute)
    }
}

/// An aggregator that computes the sum of integer values.
#[derive(Debug, Clone)]
pub struct IntegerSumAggregator {
    attribute: Option<String>,
}

impl IntegerSumAggregator {
    /// Creates a new integer sum aggregator for an attribute.
    pub fn new(attribute: impl Into<String>) -> Self {
        Self {
            attribute: Some(attribute.into()),
        }
    }
}

impl Aggregator for IntegerSumAggregator {
    type Output = i64;

    fn class_id(&self) -> i32 {
        class_ids::INT_SUM
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_attribute(output, &self.attribute)
    }
}

/// An aggregator that computes the sum of long values.
#[derive(Debug, Clone)]
pub struct LongSumAggregator {
    attribute: Option<String>,
}

impl LongSumAggregator {
    /// Creates a new long sum aggregator for an attribute.
    pub fn new(attribute: impl Into<String>) -> Self {
        Self {
            attribute: Some(attribute.into()),
        }
    }
}

impl Aggregator for LongSumAggregator {
    type Output = i64;

    fn class_id(&self) -> i32 {
        class_ids::LONG_SUM
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_attribute(output, &self.attribute)
    }
}

/// An aggregator that computes the sum of double/floating-point values.
#[derive(Debug, Clone)]
pub struct DoubleSumAggregator {
    attribute: Option<String>,
}

impl DoubleSumAggregator {
    /// Creates a new double sum aggregator for an attribute.
    pub fn new(attribute: impl Into<String>) -> Self {
        Self {
            attribute: Some(attribute.into()),
        }
    }
}

impl Aggregator for DoubleSumAggregator {
    type Output = f64;

    fn class_id(&self) -> i32 {
        class_ids::DOUBLE_SUM
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_attribute(output, &self.attribute)
    }
}

/// An aggregator that computes the sum of fixed-point (decimal) values.
#[derive(Debug, Clone)]
pub struct FixedPointSumAggregator {
    attribute: Option<String>,
}

impl FixedPointSumAggregator {
    /// Creates a new fixed-point sum aggregator for an attribute.
    pub fn new(attribute: impl Into<String>) -> Self {
        Self {
            attribute: Some(attribute.into()),
        }
    }
}

impl Aggregator for FixedPointSumAggregator {
    type Output = i64;

    fn class_id(&self) -> i32 {
        class_ids::FIXED_POINT_SUM
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_attribute(output, &self.attribute)
    }
}

/// An aggregator that computes the sum of floating-point values.
#[derive(Debug, Clone)]
pub struct FloatingPointSumAggregator {
    attribute: Option<String>,
}

impl FloatingPointSumAggregator {
    /// Creates a new floating-point sum aggregator for an attribute.
    pub fn new(attribute: impl Into<String>) -> Self {
        Self {
            attribute: Some(attribute.into()),
        }
    }
}

impl Aggregator for FloatingPointSumAggregator {
    type Output = f64;

    fn class_id(&self) -> i32 {
        class_ids::FLOATING_POINT_SUM
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_attribute(output, &self.attribute)
    }
}

/// An aggregator that computes the average of integer values.
#[derive(Debug, Clone)]
pub struct IntegerAvgAggregator {
    attribute: Option<String>,
}

impl IntegerAvgAggregator {
    /// Creates a new integer average aggregator for an attribute.
    pub fn new(attribute: impl Into<String>) -> Self {
        Self {
            attribute: Some(attribute.into()),
        }
    }
}

impl Aggregator for IntegerAvgAggregator {
    type Output = f64;

    fn class_id(&self) -> i32 {
        class_ids::INT_AVG
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_attribute(output, &self.attribute)
    }
}

/// An aggregator that computes the average of long values.
#[derive(Debug, Clone)]
pub struct LongAvgAggregator {
    attribute: Option<String>,
}

impl LongAvgAggregator {
    /// Creates a new long average aggregator for an attribute.
    pub fn new(attribute: impl Into<String>) -> Self {
        Self {
            attribute: Some(attribute.into()),
        }
    }
}

impl Aggregator for LongAvgAggregator {
    type Output = f64;

    fn class_id(&self) -> i32 {
        class_ids::LONG_AVG
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_attribute(output, &self.attribute)
    }
}

/// An aggregator that computes the average of double values.
#[derive(Debug, Clone)]
pub struct DoubleAvgAggregator {
    attribute: Option<String>,
}

impl DoubleAvgAggregator {
    /// Creates a new double average aggregator for an attribute.
    pub fn new(attribute: impl Into<String>) -> Self {
        Self {
            attribute: Some(attribute.into()),
        }
    }
}

impl Aggregator for DoubleAvgAggregator {
    type Output = f64;

    fn class_id(&self) -> i32 {
        class_ids::DOUBLE_AVG
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_attribute(output, &self.attribute)
    }
}

/// An aggregator that computes the average of numeric values.
#[derive(Debug, Clone)]
pub struct NumberAvgAggregator {
    attribute: Option<String>,
}

impl NumberAvgAggregator {
    /// Creates a new number average aggregator for an attribute.
    pub fn new(attribute: impl Into<String>) -> Self {
        Self {
            attribute: Some(attribute.into()),
        }
    }
}

impl Aggregator for NumberAvgAggregator {
    type Output = f64;

    fn class_id(&self) -> i32 {
        class_ids::NUMBER_AVG
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_attribute(output, &self.attribute)
    }
}

/// An aggregator that finds the minimum value.
#[derive(Debug, Clone)]
pub struct MinAggregator<T> {
    attribute: Option<String>,
    _phantom: PhantomData<T>,
}

impl<T> MinAggregator<T> {
    /// Creates a new min aggregator for an attribute.
    pub fn new(attribute: impl Into<String>) -> Self {
        Self {
            attribute: Some(attribute.into()),
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + Sync + Debug> Aggregator for MinAggregator<T> {
    type Output = T;

    fn class_id(&self) -> i32 {
        class_ids::MIN
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_attribute(output, &self.attribute)
    }
}

/// An aggregator that finds the maximum value.
#[derive(Debug, Clone)]
pub struct MaxAggregator<T> {
    attribute: Option<String>,
    _phantom: PhantomData<T>,
}

impl<T> MaxAggregator<T> {
    /// Creates a new max aggregator for an attribute.
    pub fn new(attribute: impl Into<String>) -> Self {
        Self {
            attribute: Some(attribute.into()),
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + Sync + Debug> Aggregator for MaxAggregator<T> {
    type Output = T;

    fn class_id(&self) -> i32 {
        class_ids::MAX
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_attribute(output, &self.attribute)
    }
}

/// An aggregator that collects distinct values.
#[derive(Debug, Clone)]
pub struct DistinctAggregator<T> {
    attribute: Option<String>,
    _phantom: PhantomData<T>,
}

impl<T> DistinctAggregator<T> {
    /// Creates a new distinct values aggregator for an attribute.
    pub fn new(attribute: impl Into<String>) -> Self {
        Self {
            attribute: Some(attribute.into()),
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + Sync + Debug> Aggregator for DistinctAggregator<T> {
    type Output = Vec<T>;

    fn class_id(&self) -> i32 {
        class_ids::DISTINCT
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        write_attribute(output, &self.attribute)
    }
}

// ============================================================================
// Type Aliases for Common Aggregators
// ============================================================================

/// Type alias for the most common sum aggregator (long/i64 values).
///
/// Use this when you want to sum integer values and get an `i64` result.
/// For other numeric types, use the specific aggregators like
/// [`IntegerSumAggregator`] or [`DoubleSumAggregator`].
pub type SumAggregator = LongSumAggregator;

/// Type alias for the most common average aggregator.
///
/// Use this when you want to compute the average of numeric values and
/// get an `f64` result. For specific input types, use [`IntegerAvgAggregator`],
/// [`LongAvgAggregator`], or [`NumberAvgAggregator`].
pub type AvgAggregator = DoubleAvgAggregator;

// ============================================================================
// Aggregators Factory
// ============================================================================

/// Factory for creating aggregators.
///
/// Provides convenient static methods for creating all aggregator types.
pub struct Aggregators;

impl Aggregators {
    /// Returns an aggregator that counts all entries.
    pub fn count() -> CountAggregator {
        CountAggregator::new()
    }

    /// Returns an aggregator that counts non-null values of an attribute.
    pub fn count_for(attribute: impl Into<String>) -> CountAggregator {
        CountAggregator::for_attribute(attribute)
    }

    /// Returns an aggregator that computes the sum of integer values.
    pub fn integer_sum(attribute: impl Into<String>) -> IntegerSumAggregator {
        IntegerSumAggregator::new(attribute)
    }

    /// Returns an aggregator that computes the sum of long values.
    pub fn long_sum(attribute: impl Into<String>) -> LongSumAggregator {
        LongSumAggregator::new(attribute)
    }

    /// Returns an aggregator that computes the sum of double values.
    pub fn double_sum(attribute: impl Into<String>) -> DoubleSumAggregator {
        DoubleSumAggregator::new(attribute)
    }

    /// Returns an aggregator that computes the sum of fixed-point values.
    pub fn fixed_point_sum(attribute: impl Into<String>) -> FixedPointSumAggregator {
        FixedPointSumAggregator::new(attribute)
    }

    /// Returns an aggregator that computes the sum of floating-point values.
    pub fn floating_point_sum(attribute: impl Into<String>) -> FloatingPointSumAggregator {
        FloatingPointSumAggregator::new(attribute)
    }

    /// Returns an aggregator that computes the average of integer values.
    pub fn integer_avg(attribute: impl Into<String>) -> IntegerAvgAggregator {
        IntegerAvgAggregator::new(attribute)
    }

    /// Returns an aggregator that computes the average of long values.
    pub fn long_avg(attribute: impl Into<String>) -> LongAvgAggregator {
        LongAvgAggregator::new(attribute)
    }

    /// Returns an aggregator that computes the average of double values.
    pub fn double_avg(attribute: impl Into<String>) -> DoubleAvgAggregator {
        DoubleAvgAggregator::new(attribute)
    }

    /// Returns an aggregator that computes the average of numeric values.
    pub fn number_avg(attribute: impl Into<String>) -> NumberAvgAggregator {
        NumberAvgAggregator::new(attribute)
    }

    /// Returns an aggregator that finds the minimum value.
    pub fn min<T>(attribute: impl Into<String>) -> MinAggregator<T> {
        MinAggregator::new(attribute)
    }

    /// Returns an aggregator that finds the maximum value.
    pub fn max<T>(attribute: impl Into<String>) -> MaxAggregator<T> {
        MaxAggregator::new(attribute)
    }

    /// Returns an aggregator that collects distinct values.
    pub fn distinct<T>(attribute: impl Into<String>) -> DistinctAggregator<T> {
        DistinctAggregator::new(attribute)
    }

    /// Returns an aggregator that computes the sum of values (as i64).
    ///
    /// This is a convenience method that returns a [`SumAggregator`] (alias for
    /// [`LongSumAggregator`]). For other numeric types, use [`integer_sum`](Self::integer_sum),
    /// [`double_sum`](Self::double_sum), etc.
    pub fn sum(attribute: impl Into<String>) -> SumAggregator {
        LongSumAggregator::new(attribute)
    }

    /// Returns an aggregator that computes the average of values (as f64).
    ///
    /// This is a convenience method that returns an [`AvgAggregator`] (alias for
    /// [`DoubleAvgAggregator`]). For specific input types, use [`integer_avg`](Self::integer_avg),
    /// [`long_avg`](Self::long_avg), etc.
    pub fn avg(attribute: impl Into<String>) -> AvgAggregator {
        DoubleAvgAggregator::new(attribute)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_aggregator() {
        let agg = CountAggregator::new();
        assert_eq!(agg.class_id(), class_ids::COUNT);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_count_aggregator_with_attribute() {
        let agg = CountAggregator::for_attribute("name");
        assert_eq!(agg.class_id(), class_ids::COUNT);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_integer_sum_aggregator() {
        let agg = IntegerSumAggregator::new("value");
        assert_eq!(agg.class_id(), class_ids::INT_SUM);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_long_sum_aggregator() {
        let agg = LongSumAggregator::new("value");
        assert_eq!(agg.class_id(), class_ids::LONG_SUM);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_double_sum_aggregator() {
        let agg = DoubleSumAggregator::new("value");
        assert_eq!(agg.class_id(), class_ids::DOUBLE_SUM);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_fixed_point_sum_aggregator() {
        let agg = FixedPointSumAggregator::new("value");
        assert_eq!(agg.class_id(), class_ids::FIXED_POINT_SUM);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_floating_point_sum_aggregator() {
        let agg = FloatingPointSumAggregator::new("value");
        assert_eq!(agg.class_id(), class_ids::FLOATING_POINT_SUM);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_integer_avg_aggregator() {
        let agg = IntegerAvgAggregator::new("value");
        assert_eq!(agg.class_id(), class_ids::INT_AVG);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_long_avg_aggregator() {
        let agg = LongAvgAggregator::new("value");
        assert_eq!(agg.class_id(), class_ids::LONG_AVG);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_double_avg_aggregator() {
        let agg = DoubleAvgAggregator::new("value");
        assert_eq!(agg.class_id(), class_ids::DOUBLE_AVG);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_number_avg_aggregator() {
        let agg = NumberAvgAggregator::new("value");
        assert_eq!(agg.class_id(), class_ids::NUMBER_AVG);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_min_aggregator() {
        let agg: MinAggregator<i64> = MinAggregator::new("value");
        assert_eq!(agg.class_id(), class_ids::MIN);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_max_aggregator() {
        let agg: MaxAggregator<i64> = MaxAggregator::new("value");
        assert_eq!(agg.class_id(), class_ids::MAX);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_distinct_aggregator() {
        let agg: DistinctAggregator<String> = DistinctAggregator::new("category");
        assert_eq!(agg.class_id(), class_ids::DISTINCT);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_aggregators_factory() {
        let _ = Aggregators::count();
        let _ = Aggregators::count_for("field");
        let _ = Aggregators::integer_sum("value");
        let _ = Aggregators::long_sum("value");
        let _ = Aggregators::double_sum("value");
        let _ = Aggregators::fixed_point_sum("value");
        let _ = Aggregators::floating_point_sum("value");
        let _ = Aggregators::integer_avg("value");
        let _ = Aggregators::long_avg("value");
        let _ = Aggregators::double_avg("value");
        let _ = Aggregators::number_avg("value");
        let _: MinAggregator<i64> = Aggregators::min("value");
        let _: MaxAggregator<i64> = Aggregators::max("value");
        let _: DistinctAggregator<String> = Aggregators::distinct("value");
        let _ = Aggregators::sum("value");
        let _ = Aggregators::avg("value");
    }

    #[test]
    fn test_sum_aggregator_alias() {
        let agg: SumAggregator = Aggregators::sum("amount");
        assert_eq!(agg.class_id(), class_ids::LONG_SUM);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_avg_aggregator_alias() {
        let agg: AvgAggregator = Aggregators::avg("price");
        assert_eq!(agg.class_id(), class_ids::DOUBLE_AVG);

        let data = agg.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_aggregator_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CountAggregator>();
        assert_send_sync::<IntegerSumAggregator>();
        assert_send_sync::<LongSumAggregator>();
        assert_send_sync::<DoubleSumAggregator>();
        assert_send_sync::<IntegerAvgAggregator>();
        assert_send_sync::<LongAvgAggregator>();
        assert_send_sync::<DoubleAvgAggregator>();
        assert_send_sync::<MinAggregator<i64>>();
        assert_send_sync::<MaxAggregator<i64>>();
        assert_send_sync::<DistinctAggregator<String>>();
    }

    #[test]
    fn test_aggregator_clone() {
        let count = CountAggregator::new();
        let cloned = count.clone();
        assert_eq!(count.class_id(), cloned.class_id());

        let sum = IntegerSumAggregator::new("value");
        let cloned = sum.clone();
        assert_eq!(sum.class_id(), cloned.class_id());

        let min: MinAggregator<i64> = MinAggregator::new("value");
        let cloned = min.clone();
        assert_eq!(min.class_id(), cloned.class_id());
    }

    #[test]
    fn test_count_aggregator_default() {
        let agg = CountAggregator::default();
        assert_eq!(agg.class_id(), class_ids::COUNT);
    }

    #[test]
    fn test_aggregator_data_format() {
        let agg = CountAggregator::new();
        let data = agg.to_aggregator_data().unwrap();

        assert!(data.len() >= 8);
        let factory_id = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        assert_eq!(factory_id, AGGREGATOR_FACTORY_ID);

        let class_id = i32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        assert_eq!(class_id, class_ids::COUNT);
    }

    #[test]
    fn test_aggregator_with_attribute_data_format() {
        let agg = IntegerSumAggregator::new("value");
        let data = agg.to_aggregator_data().unwrap();

        assert!(data.len() > 8);
        let factory_id = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        assert_eq!(factory_id, AGGREGATOR_FACTORY_ID);

        let class_id = i32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        assert_eq!(class_id, class_ids::INT_SUM);

        assert_eq!(data[8], 1);
    }
}
