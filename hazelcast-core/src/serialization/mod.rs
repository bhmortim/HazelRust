//! Serialization framework for Hazelcast's binary format.

mod data_input;
mod data_output;
mod traits;

#[cfg(feature = "serde")]
mod serde;

pub use data_input::{DataInput, ObjectDataInput};
pub use data_output::{DataOutput, ObjectDataOutput};
pub use traits::{Deserializable, Serializable};

#[cfg(feature = "serde")]
pub use self::serde::Serde;
