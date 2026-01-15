//! Distributed data structure proxies.

mod atomic_long;
mod fenced_lock;
mod flake_id;
mod list;
mod map;
mod multimap;
mod pn_counter;
mod queue;
mod replicated_map;
mod ringbuffer;
mod set;
mod topic;

pub use atomic_long::AtomicLong;
pub use fenced_lock::FencedLock;
pub use flake_id::{FlakeIdGenerator, IdBatch};
pub use list::IList;
pub use map::IMap;
pub use multimap::MultiMap;
pub use pn_counter::PNCounter;
pub use queue::IQueue;
pub use replicated_map::ReplicatedMap;
pub use ringbuffer::{OverflowPolicy, Ringbuffer};
pub use set::ISet;
pub use topic::{ITopic, TopicMessage};
