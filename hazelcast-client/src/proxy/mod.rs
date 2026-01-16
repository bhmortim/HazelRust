//! Distributed data structure proxies.

mod atomic_long;
mod cardinality_estimator;
mod countdown_latch;
mod entry_processor;
mod fenced_lock;
mod flake_id;
mod list;
mod map;
mod multimap;
mod pn_counter;
mod queue;
mod reliable_topic;
mod replicated_map;
mod ringbuffer;
mod semaphore;
mod set;
mod topic;

pub use atomic_long::AtomicLong;
pub use cardinality_estimator::CardinalityEstimator;
pub use countdown_latch::CountDownLatch;
pub use entry_processor::{EntryProcessor, EntryProcessorResult};
pub use fenced_lock::FencedLock;
pub use flake_id::{FlakeIdGenerator, IdBatch};
pub use list::IList;
pub use map::{IMap, IndexConfig, IndexConfigBuilder, IndexType};
pub use multimap::MultiMap;
pub use pn_counter::PNCounter;
pub use queue::IQueue;
pub use reliable_topic::{ReliableTopic, ReliableTopicConfig, ReliableTopicMessage, ReliableTopicStats};
pub use replicated_map::ReplicatedMap;
pub use ringbuffer::{OverflowPolicy, Ringbuffer};
pub use semaphore::Semaphore;
pub use set::ISet;
pub use topic::{ITopic, TopicMessage};
