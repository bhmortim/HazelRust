//! Diagnostics and monitoring utilities.

mod slow_ops;

pub use slow_ops::{OperationTracker, SlowOperationDetector};
