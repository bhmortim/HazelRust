//! Paging predicate for paginated query results.
//!
//! `PagingPredicate` allows fetching query results in pages, which is useful
//! for large result sets that would be impractical to load all at once.
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client::query::{PagingPredicate, Predicates};
//!
//! // Create a paging predicate with 10 items per page
//! let mut paging = PagingPredicate::<String, User>::new(10);
//!
//! // Optionally add a filter predicate
//! paging.set_predicate(Predicates::greater_than("age", &18i32)?);
//!
//! // Fetch first page
//! let page1 = map.entries_with_paging_predicate(&paging).await?;
//!
//! // Move to next page and fetch
//! paging.next_page();
//! let page2 = map.entries_with_paging_predicate(&paging).await?;
//! ```

use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use hazelcast_core::serialization::ObjectDataOutput;
use hazelcast_core::{Result, Serializable};

use super::{class_ids, write_data_bytes, write_i32, Predicate};

/// Iteration type for paging predicate results.
///
/// Determines what data is returned and how ordering is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IterationType {
    /// Iterate over keys only.
    Key = 0,
    /// Iterate over values only.
    Value = 1,
    /// Iterate over entries (key-value pairs).
    #[default]
    Entry = 2,
}

impl IterationType {
    fn as_byte(self) -> u8 {
        self as u8
    }
}

/// Trait for custom comparators used to order paging predicate results.
///
/// Implement this trait to define custom ordering for paginated queries.
/// The comparator is serialized and sent to the cluster for server-side ordering.
pub trait PagingComparator<K, V>: Debug + Send + Sync {
    /// Returns the factory ID for this comparator.
    fn factory_id(&self) -> i32;

    /// Returns the class ID for this comparator.
    fn class_id(&self) -> i32;

    /// Compares two entries for ordering.
    fn compare(&self, a: (&K, &V), b: (&K, &V)) -> Ordering;

    /// Writes comparator-specific data to the output.
    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()>;

    /// Serializes this comparator to bytes.
    fn to_comparator_data(&self) -> Result<Vec<u8>> {
        let mut output = ObjectDataOutput::new();
        write_i32(&mut output, self.factory_id())?;
        write_i32(&mut output, self.class_id())?;
        self.write_data(&mut output)?;
        Ok(output.into_bytes())
    }
}

/// A comparator that orders entries by their keys in ascending (natural) order.
///
/// Note: This comparator requires a corresponding server-side implementation
/// to be deployed for actual use. The factory_id and class_id must match
/// the server-side configuration.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::query::{PagingPredicate, EntryKeyComparator};
///
/// let comparator = EntryKeyComparator::<String, i32>::new(1, 1); // factory_id, class_id
/// let predicate = PagingPredicate::with_comparator(10, &comparator)?;
/// ```
#[derive(Debug, Clone)]
pub struct EntryKeyComparator<K, V> {
    factory_id: i32,
    class_id: i32,
    descending: bool,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> EntryKeyComparator<K, V> {
    /// Creates a new key comparator with the specified factory and class IDs.
    ///
    /// The IDs must match the server-side comparator implementation.
    pub fn new(factory_id: i32, class_id: i32) -> Self {
        Self {
            factory_id,
            class_id,
            descending: false,
            _phantom: PhantomData,
        }
    }

    /// Creates a descending key comparator.
    pub fn descending(factory_id: i32, class_id: i32) -> Self {
        Self {
            factory_id,
            class_id,
            descending: true,
            _phantom: PhantomData,
        }
    }

    /// Returns whether this comparator sorts in descending order.
    pub fn is_descending(&self) -> bool {
        self.descending
    }
}

impl<K, V> PagingComparator<K, V> for EntryKeyComparator<K, V>
where
    K: Ord + Debug + Send + Sync,
    V: Debug + Send + Sync,
{
    fn factory_id(&self) -> i32 {
        self.factory_id
    }

    fn class_id(&self) -> i32 {
        self.class_id
    }

    fn compare(&self, a: (&K, &V), b: (&K, &V)) -> Ordering {
        let result = a.0.cmp(b.0);
        if self.descending {
            result.reverse()
        } else {
            result
        }
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        let flag: i8 = if self.descending { 1 } else { 0 };
        flag.serialize(output)?;
        Ok(())
    }
}

/// A comparator that orders entries by their values.
///
/// Note: This comparator requires a corresponding server-side implementation
/// to be deployed for actual use.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::query::{PagingPredicate, EntryValueComparator};
///
/// let comparator = EntryValueComparator::<String, i32>::new(1, 2);
/// let predicate = PagingPredicate::with_comparator(10, &comparator)?;
/// ```
#[derive(Debug, Clone)]
pub struct EntryValueComparator<K, V> {
    factory_id: i32,
    class_id: i32,
    descending: bool,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> EntryValueComparator<K, V> {
    /// Creates a new value comparator with the specified factory and class IDs.
    pub fn new(factory_id: i32, class_id: i32) -> Self {
        Self {
            factory_id,
            class_id,
            descending: false,
            _phantom: PhantomData,
        }
    }

    /// Creates a descending value comparator.
    pub fn descending(factory_id: i32, class_id: i32) -> Self {
        Self {
            factory_id,
            class_id,
            descending: true,
            _phantom: PhantomData,
        }
    }

    /// Returns whether this comparator sorts in descending order.
    pub fn is_descending(&self) -> bool {
        self.descending
    }
}

impl<K, V> PagingComparator<K, V> for EntryValueComparator<K, V>
where
    K: Debug + Send + Sync,
    V: Ord + Debug + Send + Sync,
{
    fn factory_id(&self) -> i32 {
        self.factory_id
    }

    fn class_id(&self) -> i32 {
        self.class_id
    }

    fn compare(&self, a: (&K, &V), b: (&K, &V)) -> Ordering {
        let result = a.1.cmp(b.1);
        if self.descending {
            result.reverse()
        } else {
            result
        }
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        let flag: i8 = if self.descending { 1 } else { 0 };
        flag.serialize(output)?;
        Ok(())
    }
}

/// An anchor entry marking a page boundary in serialized form.
///
/// Anchors are used internally to track pagination state and enable
/// efficient page navigation on the server side.
#[derive(Debug, Clone, Default)]
pub struct AnchorEntry {
    page: usize,
    key_data: Option<Vec<u8>>,
    value_data: Option<Vec<u8>>,
}

impl AnchorEntry {
    /// Creates a new anchor entry.
    pub fn new(page: usize, key_data: Option<Vec<u8>>, value_data: Option<Vec<u8>>) -> Self {
        Self {
            page,
            key_data,
            value_data,
        }
    }

    /// Returns the page number this anchor belongs to.
    pub fn page(&self) -> usize {
        self.page
    }

    /// Returns the serialized key data, if present.
    pub fn key_data(&self) -> Option<&[u8]> {
        self.key_data.as_deref()
    }

    /// Returns the serialized value data, if present.
    pub fn value_data(&self) -> Option<&[u8]> {
        self.value_data.as_deref()
    }
}

/// A predicate that supports pagination of query results.
///
/// `PagingPredicate` wraps an optional inner predicate and adds pagination
/// support with configurable page size and optional custom ordering.
///
/// # Thread Safety
///
/// The page counter uses atomic operations for thread-safe page navigation.
/// However, anchor updates should be performed from a single thread to avoid
/// race conditions.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::query::PagingPredicate;
///
/// // Create with 25 items per page
/// let mut predicate: PagingPredicate<String, i32> = PagingPredicate::new(25);
///
/// // Navigate pages
/// predicate.next_page();     // Go to page 1
/// predicate.next_page();     // Go to page 2
/// predicate.previous_page(); // Back to page 1
/// predicate.set_page(5);     // Jump to page 5
/// ```
pub struct PagingPredicate<K, V> {
    inner: Option<Box<dyn Predicate>>,
    page_size: usize,
    current_page: AtomicUsize,
    iteration_type: IterationType,
    comparator_data: Option<Vec<u8>>,
    anchors: Vec<AnchorEntry>,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Debug for PagingPredicate<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PagingPredicate")
            .field("inner", &self.inner)
            .field("page_size", &self.page_size)
            .field("current_page", &self.current_page.load(AtomicOrdering::Acquire))
            .field("iteration_type", &self.iteration_type)
            .field("has_comparator", &self.comparator_data.is_some())
            .field("anchor_count", &self.anchors.len())
            .finish()
    }
}

impl<K, V> PagingPredicate<K, V> {
    /// Creates a new paging predicate with the specified page size.
    ///
    /// # Arguments
    ///
    /// * `page_size` - The number of entries per page. Must be greater than 0.
    ///
    /// # Panics
    ///
    /// Panics if `page_size` is 0.
    pub fn new(page_size: usize) -> Self {
        assert!(page_size > 0, "page_size must be greater than 0");
        Self {
            inner: None,
            page_size,
            current_page: AtomicUsize::new(0),
            iteration_type: IterationType::Entry,
            comparator_data: None,
            anchors: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Creates a new paging predicate with the specified page size and inner predicate.
    ///
    /// # Arguments
    ///
    /// * `page_size` - The number of entries per page. Must be greater than 0.
    /// * `predicate` - The filter predicate to apply before pagination.
    pub fn with_predicate<P: Predicate + 'static>(page_size: usize, predicate: P) -> Self {
        assert!(page_size > 0, "page_size must be greater than 0");
        Self {
            inner: Some(Box::new(predicate)),
            page_size,
            current_page: AtomicUsize::new(0),
            iteration_type: IterationType::Entry,
            comparator_data: None,
            anchors: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Creates a new paging predicate with a comparator for custom ordering.
    ///
    /// # Arguments
    ///
    /// * `page_size` - The number of entries per page. Must be greater than 0.
    /// * `comparator` - The comparator for ordering results.
    pub fn with_comparator<C>(page_size: usize, comparator: &C) -> Result<Self>
    where
        C: PagingComparator<K, V>,
    {
        assert!(page_size > 0, "page_size must be greater than 0");
        Ok(Self {
            inner: None,
            page_size,
            current_page: AtomicUsize::new(0),
            iteration_type: IterationType::Entry,
            comparator_data: Some(comparator.to_comparator_data()?),
            anchors: Vec::new(),
            _phantom: PhantomData,
        })
    }

    /// Returns the page size (number of entries per page).
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Returns the current page number (0-indexed).
    pub fn get_page(&self) -> usize {
        self.current_page.load(AtomicOrdering::Acquire)
    }

    /// Sets the current page number.
    ///
    /// # Arguments
    ///
    /// * `page` - The page number to set (0-indexed).
    pub fn set_page(&self, page: usize) {
        self.current_page.store(page, AtomicOrdering::Release);
    }

    /// Advances to the next page.
    ///
    /// Returns the new page number.
    pub fn next_page(&self) -> usize {
        self.current_page.fetch_add(1, AtomicOrdering::AcqRel) + 1
    }

    /// Goes back to the previous page.
    ///
    /// Returns the new page number. If already on page 0, stays on page 0.
    pub fn previous_page(&self) -> usize {
        let current = self.current_page.load(AtomicOrdering::Acquire);
        if current > 0 {
            self.current_page.fetch_sub(1, AtomicOrdering::AcqRel) - 1
        } else {
            0
        }
    }

    /// Resets to the first page (page 0).
    pub fn reset(&self) {
        self.current_page.store(0, AtomicOrdering::Release);
    }

    /// Returns the iteration type.
    pub fn iteration_type(&self) -> IterationType {
        self.iteration_type
    }

    /// Sets the iteration type.
    pub fn set_iteration_type(&mut self, iteration_type: IterationType) {
        self.iteration_type = iteration_type;
    }

    /// Sets the inner filter predicate.
    pub fn set_predicate<P: Predicate + 'static>(&mut self, predicate: P) {
        self.inner = Some(Box::new(predicate));
    }

    /// Clears the inner filter predicate.
    pub fn clear_predicate(&mut self) {
        self.inner = None;
    }

    /// Returns the anchor list for pagination state tracking.
    ///
    /// Anchors mark page boundaries and enable efficient navigation.
    pub fn get_anchor_list(&self) -> &[AnchorEntry] {
        &self.anchors
    }

    /// Updates the anchor list with new entries from a query response.
    ///
    /// This is typically called internally after executing a paging query.
    pub fn update_anchors(&mut self, anchors: Vec<AnchorEntry>) {
        self.anchors = anchors;
    }

    /// Adds an anchor entry for the current page.
    pub fn add_anchor(&mut self, key_data: Option<Vec<u8>>, value_data: Option<Vec<u8>>) {
        let page = self.get_page();
        self.anchors.push(AnchorEntry::new(page, key_data, value_data));
    }

    /// Clears all anchor entries.
    pub fn clear_anchors(&mut self) {
        self.anchors.clear();
    }

    /// Returns the nearest anchor for the current page.
    ///
    /// Returns `None` if on the first page or no suitable anchor exists.
    pub fn nearest_anchor(&self) -> Option<&AnchorEntry> {
        let current = self.get_page();
        if current == 0 {
            return None;
        }
        self.anchors.iter().rev().find(|a| a.page < current)
    }

    /// Returns `true` if a comparator is set.
    pub fn has_comparator(&self) -> bool {
        self.comparator_data.is_some()
    }
}

impl<K, V> Predicate for PagingPredicate<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    fn class_id(&self) -> i32 {
        class_ids::PAGING_PREDICATE
    }

    fn write_data(&self, output: &mut ObjectDataOutput) -> Result<()> {
        // Write inner predicate (nullable)
        if let Some(ref inner) = self.inner {
            let predicate_data = inner.to_predicate_data()?;
            write_data_bytes(output, &predicate_data)?;
        } else {
            write_i32(output, -1)?; // null marker
        }

        // Write comparator (nullable)
        if let Some(ref comparator_data) = self.comparator_data {
            write_data_bytes(output, comparator_data)?;
        } else {
            write_i32(output, -1)?; // null marker
        }

        // Write page number
        write_i32(output, self.get_page() as i32)?;

        // Write page size
        write_i32(output, self.page_size as i32)?;

        // Write iteration type
        (self.iteration_type.as_byte() as i8).serialize(output)?;

        // Write anchor list
        write_i32(output, self.anchors.len() as i32)?;
        for anchor in &self.anchors {
            write_i32(output, anchor.page as i32)?;

            // Write key data (nullable)
            if let Some(ref key_data) = anchor.key_data {
                write_data_bytes(output, key_data)?;
            } else {
                write_i32(output, -1)?;
            }

            // Write value data (nullable)
            if let Some(ref value_data) = anchor.value_data {
                write_data_bytes(output, value_data)?;
            } else {
                write_i32(output, -1)?;
            }
        }

        Ok(())
    }
}

unsafe impl<K: Send, V: Send> Send for PagingPredicate<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for PagingPredicate<K, V> {}

/// Builder for creating `PagingPredicate` instances.
#[derive(Debug)]
pub struct PagingPredicateBuilder<K, V> {
    page_size: usize,
    inner: Option<Box<dyn Predicate>>,
    comparator_data: Option<Vec<u8>>,
    iteration_type: IterationType,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> PagingPredicateBuilder<K, V> {
    /// Creates a new builder with the specified page size.
    pub fn new(page_size: usize) -> Self {
        Self {
            page_size,
            inner: None,
            comparator_data: None,
            iteration_type: IterationType::Entry,
            _phantom: PhantomData,
        }
    }

    /// Sets the filter predicate.
    pub fn predicate<P: Predicate + 'static>(mut self, predicate: P) -> Self {
        self.inner = Some(Box::new(predicate));
        self
    }

    /// Sets the comparator for custom ordering.
    pub fn comparator<C: PagingComparator<K, V>>(mut self, comparator: &C) -> Result<Self> {
        self.comparator_data = Some(comparator.to_comparator_data()?);
        Ok(self)
    }

    /// Sets the iteration type.
    pub fn iteration_type(mut self, iteration_type: IterationType) -> Self {
        self.iteration_type = iteration_type;
        self
    }

    /// Sets the iteration type to Key.
    pub fn order_by_key(mut self) -> Self {
        self.iteration_type = IterationType::Key;
        self
    }

    /// Sets the iteration type to Value.
    pub fn order_by_value(mut self) -> Self {
        self.iteration_type = IterationType::Value;
        self
    }

    /// Sets the iteration type to Entry.
    pub fn order_by_entry(mut self) -> Self {
        self.iteration_type = IterationType::Entry;
        self
    }

    /// Builds the `PagingPredicate`.
    pub fn build(self) -> PagingPredicate<K, V> {
        PagingPredicate {
            inner: self.inner,
            page_size: self.page_size,
            current_page: AtomicUsize::new(0),
            iteration_type: self.iteration_type,
            comparator_data: self.comparator_data,
            anchors: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

/// Result of a paging query including the data and pagination metadata.
#[derive(Debug)]
pub struct PagingResult<T> {
    /// The data for the current page.
    pub data: Vec<T>,
    /// The page number of these results.
    pub page: usize,
    /// The total number of pages (if known).
    pub total_pages: Option<usize>,
    /// Updated anchor entries from the server.
    pub anchors: Vec<AnchorEntry>,
}

impl<T> PagingResult<T> {
    /// Creates a new paging result.
    pub fn new(data: Vec<T>, page: usize, anchors: Vec<AnchorEntry>) -> Self {
        Self {
            data,
            page,
            total_pages: None,
            anchors,
        }
    }

    /// Returns `true` if there are more pages after this one.
    ///
    /// This is a heuristic based on whether the page is full.
    pub fn has_next(&self, page_size: usize) -> bool {
        self.data.len() >= page_size
    }

    /// Returns `true` if this is the first page.
    pub fn is_first(&self) -> bool {
        self.page == 0
    }

    /// Returns the number of items in this page.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if this page is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::TruePredicate;

    #[test]
    fn test_paging_predicate_new() {
        let pred: PagingPredicate<String, i32> = PagingPredicate::new(10);
        assert_eq!(pred.page_size(), 10);
        assert_eq!(pred.get_page(), 0);
        assert_eq!(pred.iteration_type(), IterationType::Entry);
    }

    #[test]
    #[should_panic(expected = "page_size must be greater than 0")]
    fn test_paging_predicate_zero_page_size() {
        let _: PagingPredicate<String, i32> = PagingPredicate::new(0);
    }

    #[test]
    fn test_paging_predicate_with_predicate() {
        let pred: PagingPredicate<String, i32> =
            PagingPredicate::with_predicate(25, TruePredicate::new());
        assert_eq!(pred.page_size(), 25);
        assert!(pred.inner.is_some());
    }

    #[test]
    fn test_page_navigation() {
        let pred: PagingPredicate<String, i32> = PagingPredicate::new(10);

        assert_eq!(pred.get_page(), 0);

        assert_eq!(pred.next_page(), 1);
        assert_eq!(pred.get_page(), 1);

        assert_eq!(pred.next_page(), 2);
        assert_eq!(pred.get_page(), 2);

        assert_eq!(pred.previous_page(), 1);
        assert_eq!(pred.get_page(), 1);

        pred.set_page(5);
        assert_eq!(pred.get_page(), 5);

        pred.reset();
        assert_eq!(pred.get_page(), 0);
    }

    #[test]
    fn test_previous_page_at_zero() {
        let pred: PagingPredicate<String, i32> = PagingPredicate::new(10);
        assert_eq!(pred.get_page(), 0);
        assert_eq!(pred.previous_page(), 0);
        assert_eq!(pred.get_page(), 0);
    }

    #[test]
    fn test_iteration_type() {
        let mut pred: PagingPredicate<String, i32> = PagingPredicate::new(10);
        assert_eq!(pred.iteration_type(), IterationType::Entry);

        pred.set_iteration_type(IterationType::Key);
        assert_eq!(pred.iteration_type(), IterationType::Key);

        pred.set_iteration_type(IterationType::Value);
        assert_eq!(pred.iteration_type(), IterationType::Value);
    }

    #[test]
    fn test_anchor_management() {
        let mut pred: PagingPredicate<String, i32> = PagingPredicate::new(10);

        assert!(pred.get_anchor_list().is_empty());

        pred.add_anchor(Some(vec![1, 2, 3]), Some(vec![4, 5, 6]));
        assert_eq!(pred.get_anchor_list().len(), 1);

        pred.set_page(1);
        pred.add_anchor(Some(vec![7, 8, 9]), None);
        assert_eq!(pred.get_anchor_list().len(), 2);

        let anchor = &pred.get_anchor_list()[1];
        assert_eq!(anchor.page(), 1);
        assert_eq!(anchor.key_data(), Some(&[7u8, 8, 9][..]));
        assert!(anchor.value_data().is_none());

        pred.clear_anchors();
        assert!(pred.get_anchor_list().is_empty());
    }

    #[test]
    fn test_nearest_anchor() {
        let mut pred: PagingPredicate<String, i32> = PagingPredicate::new(10);

        assert!(pred.nearest_anchor().is_none());

        pred.update_anchors(vec![
            AnchorEntry::new(0, Some(vec![1]), None),
            AnchorEntry::new(1, Some(vec![2]), None),
            AnchorEntry::new(2, Some(vec![3]), None),
        ]);

        pred.set_page(3);
        let anchor = pred.nearest_anchor().unwrap();
        assert_eq!(anchor.page(), 2);

        pred.set_page(1);
        let anchor = pred.nearest_anchor().unwrap();
        assert_eq!(anchor.page(), 0);

        pred.set_page(0);
        assert!(pred.nearest_anchor().is_none());
    }

    #[test]
    fn test_predicate_class_id() {
        let pred: PagingPredicate<String, i32> = PagingPredicate::new(10);
        assert_eq!(pred.class_id(), class_ids::PAGING_PREDICATE);
    }

    #[test]
    fn test_serialization() {
        let pred: PagingPredicate<String, i32> = PagingPredicate::new(10);
        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_serialization_with_inner_predicate() {
        let pred: PagingPredicate<String, i32> =
            PagingPredicate::with_predicate(10, TruePredicate::new());
        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_builder() {
        let pred: PagingPredicate<String, i32> = PagingPredicateBuilder::new(20)
            .predicate(TruePredicate::new())
            .iteration_type(IterationType::Key)
            .build();

        assert_eq!(pred.page_size(), 20);
        assert_eq!(pred.iteration_type(), IterationType::Key);
        assert!(pred.inner.is_some());
    }

    #[test]
    fn test_paging_result() {
        let result: PagingResult<i32> = PagingResult::new(vec![1, 2, 3], 0, Vec::new());
        assert_eq!(result.len(), 3);
        assert!(!result.is_empty());
        assert!(result.is_first());
        assert!(!result.has_next(10));
        assert!(result.has_next(3));
    }

    #[test]
    fn test_paging_result_empty() {
        let result: PagingResult<i32> = PagingResult::new(Vec::new(), 5, Vec::new());
        assert_eq!(result.len(), 0);
        assert!(result.is_empty());
        assert!(!result.is_first());
        assert!(!result.has_next(10));
    }

    #[test]
    fn test_set_and_clear_predicate() {
        let mut pred: PagingPredicate<String, i32> = PagingPredicate::new(10);
        assert!(pred.inner.is_none());

        pred.set_predicate(TruePredicate::new());
        assert!(pred.inner.is_some());

        pred.clear_predicate();
        assert!(pred.inner.is_none());
    }

    #[test]
    fn test_iteration_type_values() {
        assert_eq!(IterationType::Key.as_byte(), 0);
        assert_eq!(IterationType::Value.as_byte(), 1);
        assert_eq!(IterationType::Entry.as_byte(), 2);
    }

    #[test]
    fn test_debug_impl() {
        let pred: PagingPredicate<String, i32> = PagingPredicate::new(10);
        let debug_str = format!("{:?}", pred);
        assert!(debug_str.contains("PagingPredicate"));
        assert!(debug_str.contains("page_size"));
    }

    #[test]
    fn test_anchor_entry_new() {
        let anchor = AnchorEntry::new(5, Some(vec![1, 2]), Some(vec![3, 4]));
        assert_eq!(anchor.page(), 5);
        assert_eq!(anchor.key_data(), Some(&[1u8, 2][..]));
        assert_eq!(anchor.value_data(), Some(&[3u8, 4][..]));
    }

    #[test]
    fn test_anchor_entry_default() {
        let anchor = AnchorEntry::default();
        assert_eq!(anchor.page(), 0);
        assert!(anchor.key_data().is_none());
        assert!(anchor.value_data().is_none());
    }

    #[test]
    fn test_paging_predicate_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PagingPredicate<String, i32>>();
    }

    #[test]
    fn test_has_comparator() {
        let pred: PagingPredicate<String, i32> = PagingPredicate::new(10);
        assert!(!pred.has_comparator());
    }

    #[test]
    fn test_update_anchors() {
        let mut pred: PagingPredicate<String, i32> = PagingPredicate::new(10);
        pred.add_anchor(Some(vec![1]), None);
        assert_eq!(pred.get_anchor_list().len(), 1);

        pred.update_anchors(vec![
            AnchorEntry::new(0, Some(vec![2]), None),
            AnchorEntry::new(1, Some(vec![3]), None),
        ]);
        assert_eq!(pred.get_anchor_list().len(), 2);
        assert_eq!(pred.get_anchor_list()[0].key_data(), Some(&[2u8][..]));
    }

    #[test]
    fn test_entry_key_comparator_new() {
        let comp: EntryKeyComparator<String, i32> = EntryKeyComparator::new(100, 1);
        assert_eq!(comp.factory_id(), 100);
        assert_eq!(comp.class_id(), 1);
        assert!(!comp.is_descending());
    }

    #[test]
    fn test_entry_key_comparator_descending() {
        let comp: EntryKeyComparator<String, i32> = EntryKeyComparator::descending(100, 1);
        assert!(comp.is_descending());
    }

    #[test]
    fn test_entry_key_comparator_compare() {
        let comp: EntryKeyComparator<String, i32> = EntryKeyComparator::new(100, 1);

        let key1 = "apple".to_string();
        let key2 = "banana".to_string();
        let val = 42;

        assert_eq!(comp.compare((&key1, &val), (&key2, &val)), Ordering::Less);
        assert_eq!(comp.compare((&key2, &val), (&key1, &val)), Ordering::Greater);
        assert_eq!(comp.compare((&key1, &val), (&key1, &val)), Ordering::Equal);
    }

    #[test]
    fn test_entry_key_comparator_compare_descending() {
        let comp: EntryKeyComparator<String, i32> = EntryKeyComparator::descending(100, 1);

        let key1 = "apple".to_string();
        let key2 = "banana".to_string();
        let val = 42;

        assert_eq!(comp.compare((&key1, &val), (&key2, &val)), Ordering::Greater);
        assert_eq!(comp.compare((&key2, &val), (&key1, &val)), Ordering::Less);
    }

    #[test]
    fn test_entry_value_comparator_new() {
        let comp: EntryValueComparator<String, i32> = EntryValueComparator::new(100, 2);
        assert_eq!(comp.factory_id(), 100);
        assert_eq!(comp.class_id(), 2);
        assert!(!comp.is_descending());
    }

    #[test]
    fn test_entry_value_comparator_compare() {
        let comp: EntryValueComparator<String, i32> = EntryValueComparator::new(100, 2);

        let key = "key".to_string();
        let val1 = 10;
        let val2 = 20;

        assert_eq!(comp.compare((&key, &val1), (&key, &val2)), Ordering::Less);
        assert_eq!(comp.compare((&key, &val2), (&key, &val1)), Ordering::Greater);
        assert_eq!(comp.compare((&key, &val1), (&key, &val1)), Ordering::Equal);
    }

    #[test]
    fn test_entry_value_comparator_compare_descending() {
        let comp: EntryValueComparator<String, i32> = EntryValueComparator::descending(100, 2);

        let key = "key".to_string();
        let val1 = 10;
        let val2 = 20;

        assert_eq!(comp.compare((&key, &val1), (&key, &val2)), Ordering::Greater);
        assert_eq!(comp.compare((&key, &val2), (&key, &val1)), Ordering::Less);
    }

    #[test]
    fn test_comparator_serialization() {
        let comp: EntryKeyComparator<String, i32> = EntryKeyComparator::new(100, 1);
        let data = comp.to_comparator_data().unwrap();

        assert_eq!(data.len(), 9);
        assert_eq!(&data[0..4], &100i32.to_be_bytes());
        assert_eq!(&data[4..8], &1i32.to_be_bytes());
        assert_eq!(data[8], 0);
    }

    #[test]
    fn test_comparator_serialization_descending() {
        let comp: EntryKeyComparator<String, i32> = EntryKeyComparator::descending(100, 1);
        let data = comp.to_comparator_data().unwrap();

        assert_eq!(data.len(), 9);
        assert_eq!(data[8], 1);
    }

    #[test]
    fn test_paging_predicate_with_key_comparator() {
        let comp: EntryKeyComparator<String, i32> = EntryKeyComparator::new(100, 1);
        let pred = PagingPredicate::<String, i32>::with_comparator(10, &comp).unwrap();

        assert!(pred.has_comparator());
        assert_eq!(pred.page_size(), 10);
    }

    #[test]
    fn test_paging_predicate_with_value_comparator() {
        let comp: EntryValueComparator<String, i32> = EntryValueComparator::descending(100, 2);
        let pred = PagingPredicate::<String, i32>::with_comparator(20, &comp).unwrap();

        assert!(pred.has_comparator());
        assert_eq!(pred.page_size(), 20);
    }

    #[test]
    fn test_builder_order_by_key() {
        let pred: PagingPredicate<String, i32> = PagingPredicateBuilder::new(10)
            .order_by_key()
            .build();

        assert_eq!(pred.iteration_type(), IterationType::Key);
    }

    #[test]
    fn test_builder_order_by_value() {
        let pred: PagingPredicate<String, i32> = PagingPredicateBuilder::new(10)
            .order_by_value()
            .build();

        assert_eq!(pred.iteration_type(), IterationType::Value);
    }

    #[test]
    fn test_builder_order_by_entry() {
        let pred: PagingPredicate<String, i32> = PagingPredicateBuilder::new(10)
            .order_by_entry()
            .build();

        assert_eq!(pred.iteration_type(), IterationType::Entry);
    }

    #[test]
    fn test_builder_with_comparator_and_predicate() {
        let comp: EntryKeyComparator<String, i32> = EntryKeyComparator::descending(100, 1);
        let pred: PagingPredicate<String, i32> = PagingPredicateBuilder::new(15)
            .predicate(TruePredicate::new())
            .comparator(&comp)
            .unwrap()
            .order_by_key()
            .build();

        assert_eq!(pred.page_size(), 15);
        assert!(pred.has_comparator());
        assert!(pred.inner.is_some());
        assert_eq!(pred.iteration_type(), IterationType::Key);
    }

    #[test]
    fn test_comparators_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EntryKeyComparator<String, i32>>();
        assert_send_sync::<EntryValueComparator<String, i32>>();
    }

    #[test]
    fn test_serialization_with_comparator() {
        let comp: EntryKeyComparator<String, i32> = EntryKeyComparator::new(100, 1);
        let pred = PagingPredicate::<String, i32>::with_comparator(10, &comp).unwrap();

        let data = pred.to_predicate_data().unwrap();
        assert!(!data.is_empty());

        let pred_no_comp: PagingPredicate<String, i32> = PagingPredicate::new(10);
        let data_no_comp = pred_no_comp.to_predicate_data().unwrap();

        assert!(data.len() > data_no_comp.len());
    }
}
