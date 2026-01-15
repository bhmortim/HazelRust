//! Distributed data structure proxies.

mod list;
mod map;
mod queue;
mod set;

pub use list::IList;
pub use map::IMap;
pub use queue::IQueue;
pub use set::ISet;
