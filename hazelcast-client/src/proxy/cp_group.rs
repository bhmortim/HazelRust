//! Shared CP (Raft) subsystem helpers used by the CP proxies (CountDownLatch,
//! FencedLock, Semaphore, AtomicLong/Reference).
//!
//! Every CP request must carry the Raft `groupId`, encoded as a data structure
//! (`BEGIN` / `[seed,id]` / name / `END`), and fixed-size params (longs, ints,
//! UUIDs) live in the request's initial frame after the header. See issue #12.

use bytes::BytesMut;
use hazelcast_core::protocol::constants::{
    BEGIN_DATA_STRUCTURE_FLAG, CP_SUBSYSTEM_GET_GROUP_IDS, END_DATA_STRUCTURE_FLAG,
};
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, HazelcastError, Result};

use crate::connection::ConnectionManager;

/// A resolved CP Raft group identifier.
#[derive(Debug, Clone)]
pub struct RaftGroupId {
    pub seed: i64,
    pub id: i64,
    pub name: String,
}

/// The object name without any `@group` suffix (what CP ops send as `name`).
pub fn object_name(proxy: &str) -> &str {
    match proxy.split_once('@') {
        Some((obj, _)) => obj.trim(),
        None => proxy,
    }
}

/// The CP group name owning this object (`default` unless `@group` is present).
pub fn group_name(proxy: &str) -> &str {
    match proxy.split_once('@') {
        Some((_, grp)) if !grp.trim().is_empty() => grp.trim(),
        _ => "default",
    }
}

pub fn string_frame(s: &str) -> Frame {
    Frame::with_content(BytesMut::from(s.as_bytes()))
}

/// Resolves the Raft `groupId` for `proxy`'s CP group via `CPGroupCreateCPGroup`
/// (`0x1E0100`), which creates the group if needed.
pub async fn resolve_group(cm: &ConnectionManager, proxy: &str) -> Result<RaftGroupId> {
    let mut request = ClientMessage::create_for_encode_any_partition(CP_SUBSYSTEM_GET_GROUP_IDS);
    request.add_frame(string_frame(group_name(proxy)));
    let response = cm.invoke_on_random(request).await?;
    decode_group_id(&response)
}

/// Decodes `[initial] BEGIN [seed(8) id(8)] name END`.
pub fn decode_group_id(response: &ClientMessage) -> Result<RaftGroupId> {
    let frames = response.frames();
    if frames.len() < 4 {
        return Err(HazelcastError::Protocol(
            "CP group id response too short (CP subsystem unavailable?)".to_string(),
        ));
    }
    let fixed = frames[2].content();
    if fixed.len() < 16 {
        return Err(HazelcastError::Protocol(
            "CP group id fixed frame too short".to_string(),
        ));
    }
    let seed = i64::from_le_bytes(fixed[0..8].try_into().unwrap());
    let id = i64::from_le_bytes(fixed[8..16].try_into().unwrap());
    let name = String::from_utf8_lossy(frames[3].content()).to_string();
    Ok(RaftGroupId { seed, id, name })
}

/// Encodes a `RaftGroupId` as a data structure: `BEGIN` / `[seed id]` / name / `END`.
pub fn encode_group_id(message: &mut ClientMessage, group: &RaftGroupId) {
    message.add_frame(Frame::with_flags(BEGIN_DATA_STRUCTURE_FLAG));
    let mut fixed = BytesMut::with_capacity(16);
    fixed.extend_from_slice(&group.seed.to_le_bytes());
    fixed.extend_from_slice(&group.id.to_le_bytes());
    message.add_frame(Frame::with_content(fixed));
    message.add_frame(string_frame(&group.name));
    message.add_frame(Frame::with_flags(END_DATA_STRUCTURE_FLAG));
}

/// Appends a fixed `i64` (little-endian) to the request's initial frame.
pub fn put_long(message: &mut ClientMessage, value: i64) {
    if let Some(initial) = message.frames_mut().first_mut() {
        initial.content.extend_from_slice(&value.to_le_bytes());
    }
}

/// Appends a fixed `i32` (little-endian) to the request's initial frame.
pub fn put_int(message: &mut ClientMessage, value: i32) {
    if let Some(initial) = message.frames_mut().first_mut() {
        initial.content.extend_from_slice(&value.to_le_bytes());
    }
}

/// Appends a Hazelcast fixed-size UUID (`[not-null:1=0][msb:8 LE][lsb:8 LE]`,
/// 17 bytes) to the request's initial frame.
pub fn put_uuid(message: &mut ClientMessage, msb: i64, lsb: i64) {
    if let Some(initial) = message.frames_mut().first_mut() {
        initial.content.extend_from_slice(&[0u8]); // not null
        initial.content.extend_from_slice(&msb.to_le_bytes());
        initial.content.extend_from_slice(&lsb.to_le_bytes());
    }
}

/// Generates a process-unique (msb, lsb) pair for an invocation UUID.
pub fn random_uuid() -> (i64, i64) {
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicI64 = AtomicI64::new(1);
    let msb = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    let lsb =
        (std::process::id() as i64) << 32 | (COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFFFF_FFFF);
    (msb, lsb)
}

/// Reads an `i64` response from the initial frame at `RESPONSE_HEADER_SIZE`.
pub fn decode_long(response: &ClientMessage) -> Result<i64> {
    use hazelcast_core::protocol::constants::RESPONSE_HEADER_SIZE;
    let f = response
        .frames()
        .first()
        .ok_or_else(|| HazelcastError::Serialization("empty response".to_string()))?;
    if f.content.len() < RESPONSE_HEADER_SIZE + 8 {
        return Err(HazelcastError::Protocol(
            "CP response too short (i64)".to_string(),
        ));
    }
    Ok(i64::from_le_bytes(
        f.content[RESPONSE_HEADER_SIZE..RESPONSE_HEADER_SIZE + 8]
            .try_into()
            .unwrap(),
    ))
}

/// Reads an `i32` response from the initial frame at `RESPONSE_HEADER_SIZE`.
pub fn decode_int(response: &ClientMessage) -> Result<i32> {
    use hazelcast_core::protocol::constants::RESPONSE_HEADER_SIZE;
    let f = response
        .frames()
        .first()
        .ok_or_else(|| HazelcastError::Serialization("empty response".to_string()))?;
    if f.content.len() < RESPONSE_HEADER_SIZE + 4 {
        return Err(HazelcastError::Protocol(
            "CP response too short (i32)".to_string(),
        ));
    }
    Ok(i32::from_le_bytes(
        f.content[RESPONSE_HEADER_SIZE..RESPONSE_HEADER_SIZE + 4]
            .try_into()
            .unwrap(),
    ))
}

/// Reads a `bool` response from the initial frame at `RESPONSE_HEADER_SIZE`.
pub fn decode_bool(response: &ClientMessage) -> Result<bool> {
    use hazelcast_core::protocol::constants::RESPONSE_HEADER_SIZE;
    let f = response
        .frames()
        .first()
        .ok_or_else(|| HazelcastError::Serialization("empty response".to_string()))?;
    if f.content.len() <= RESPONSE_HEADER_SIZE {
        return Err(HazelcastError::Protocol(
            "CP response too short (bool)".to_string(),
        ));
    }
    Ok(f.content[RESPONSE_HEADER_SIZE] != 0)
}
