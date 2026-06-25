//! Hazelcast Open Binary Protocol implementation.
//!
//! This module provides the core protocol types for communicating with
//! Hazelcast clusters using the Open Binary Protocol.

mod client_message;
mod codec;
pub mod constants;
mod frame;

pub use client_message::{
    compute_partition_hash, next_correlation_id, partition_id_for_hash, partition_id_for_key_data,
    ClientMessage,
};
pub use codec::ClientMessageCodec;
pub use constants::*;
pub use frame::Frame;
