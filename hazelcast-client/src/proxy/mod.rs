//! Distributed data structure proxies.

mod list;
mod map;
mod multimap;
mod queue;
mod set;

pub use list::IList;
pub use map::IMap;
pub use multimap::MultiMap;
pub use queue::IQueue;
pub use set::ISet;
