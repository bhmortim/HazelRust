//! `ClientAddClusterViewListener` codec (message type `0x000300`).
//!
//! In the Hazelcast 5.x client protocol a smart client learns the cluster
//! topology by registering this single listener immediately after
//! authentication. The member then streams two kinds of events on the
//! registration's correlation id:
//!
//! * **members-view** (event type `0x000302`): an `i32 version` followed by a
//!   `List<MemberInfo>` (each member's uuid + address + lite flag + attributes).
//! * **partitions-view** (event type `0x000303`): an `i32 version` followed by an
//!   `EntryList<UUID, List<Integer>>` mapping each member uuid to the partition
//!   ids it owns.
//!
//! Decoding these events is what populates the connection manager's `members`
//! map and `partition_table` in production — without it both stay empty and
//! every partition op falls back to `addresses[0]` (smart routing is a no-op).
//!
//! Wire layout was extracted from the EE 5.7 jar codecs
//! (`ClientAddClusterViewListenerCodec`, `MemberInfoCodec`, `AddressCodec`,
//! `EntryListUUIDListIntegerCodec`) and confirmed against a live hexdump.

use std::net::SocketAddr;

use uuid::Uuid;

use hazelcast_core::protocol::constants::{
    BEGIN_DATA_STRUCTURE_FLAG, CLIENT_ADD_MEMBERSHIP_LISTENER, END_DATA_STRUCTURE_FLAG,
};
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, HazelcastError, Result};

/// Event message type carried on the cluster-view listener correlation id: the
/// member list (`EVENT_MEMBERS_VIEW_MESSAGE_TYPE` in the Java codec).
pub const EVENT_MEMBERS_VIEW: i32 = 0x000302; // 770
/// Event message type carried on the cluster-view listener correlation id: the
/// partition table (`EVENT_PARTITIONS_VIEW_MESSAGE_TYPE` in the Java codec).
pub const EVENT_PARTITIONS_VIEW: i32 = 0x000303; // 771

/// Content offset of the event `version` field: after type(4) + correlationId(8)
/// + partitionId(4) = 16 (`EVENT_*_VIEW_VERSION_FIELD_OFFSET` in the Java codec).
const EVENT_VERSION_OFFSET: usize = 16;

/// A fixed UUID occupies 17 bytes on the wire: a 1-byte not-null flag followed
/// by the 8-byte most-significant and 8-byte least-significant halves (little
/// endian). (`FixedSizeTypesCodec.UUID_SIZE_IN_BYTES = 17`.)
const UUID_FIXED_SIZE: usize = 17;

/// A decoded cluster member: uuid, the address the client should connect to,
/// and whether it is a lite member.
#[derive(Debug, Clone)]
pub struct DecodedMember {
    pub uuid: Uuid,
    pub address: SocketAddr,
    pub lite: bool,
}

/// Builds the `ClientAddClusterViewListener` request (message type `0x000300`).
/// The request carries no parameters beyond the standard header.
pub fn encode_add_cluster_view_listener_request() -> ClientMessage {
    ClientMessage::create_for_encode_any_partition(CLIENT_ADD_MEMBERSHIP_LISTENER)
}

/// A forward cursor over a message's frames with nesting-aware fast-forward,
/// mirroring the Java `ForwardFrameIterator` + `CodecUtil.fastForwardToEndFrame`.
struct FrameCursor<'a> {
    frames: &'a [Frame],
    idx: usize,
}

impl<'a> FrameCursor<'a> {
    fn new(frames: &'a [Frame]) -> Self {
        Self { frames, idx: 0 }
    }

    fn next(&mut self) -> Option<&'a Frame> {
        let f = self.frames.get(self.idx);
        if f.is_some() {
            self.idx += 1;
        }
        f
    }

    fn next_or_err(&mut self, what: &str) -> Result<&'a Frame> {
        self.next().ok_or_else(|| {
            HazelcastError::Serialization(format!("cluster-view decode: truncated, missing {what}"))
        })
    }

    /// True when the next frame begins the end of the current data structure
    /// (or the message is exhausted), used as the list-loop terminator.
    fn peek_is_end_data_structure(&self) -> bool {
        match self.frames.get(self.idx) {
            Some(f) => f.flags & END_DATA_STRUCTURE_FLAG != 0,
            None => true,
        }
    }

    /// Consumes frames until the `END_DATA_STRUCTURE` frame matching the already
    /// consumed opening `BEGIN_DATA_STRUCTURE`, tracking nesting depth. Used to
    /// skip the inner fields of a struct (attributes/version/addressMap) we do
    /// not need without losing frame alignment.
    fn fast_forward_to_end_frame(&mut self) {
        let mut depth = 1i32;
        while depth != 0 {
            let frame = match self.next() {
                Some(f) => f,
                None => return,
            };
            if frame.flags & END_DATA_STRUCTURE_FLAG != 0 {
                depth -= 1;
            } else if frame.flags & BEGIN_DATA_STRUCTURE_FLAG != 0 {
                depth += 1;
            }
        }
    }
}

fn read_i32_le(content: &[u8], offset: usize) -> Result<i32> {
    if content.len() < offset + 4 {
        return Err(HazelcastError::Serialization(format!(
            "cluster-view decode: i32 at offset {offset} out of bounds (len {})",
            content.len()
        )));
    }
    Ok(i32::from_le_bytes([
        content[offset],
        content[offset + 1],
        content[offset + 2],
        content[offset + 3],
    ]))
}

/// Decodes a fixed 17-byte UUID at `offset`: `[not_null_flag:1][msb:8 LE][lsb:8 LE]`.
fn decode_uuid_fixed(content: &[u8], offset: usize) -> Result<Uuid> {
    if content.len() < offset + UUID_FIXED_SIZE {
        return Err(HazelcastError::Serialization(format!(
            "cluster-view decode: fixed UUID at offset {offset} out of bounds (len {})",
            content.len()
        )));
    }
    let msb = u64::from_le_bytes([
        content[offset + 1],
        content[offset + 2],
        content[offset + 3],
        content[offset + 4],
        content[offset + 5],
        content[offset + 6],
        content[offset + 7],
        content[offset + 8],
    ]);
    let lsb = u64::from_le_bytes([
        content[offset + 9],
        content[offset + 10],
        content[offset + 11],
        content[offset + 12],
        content[offset + 13],
        content[offset + 14],
        content[offset + 15],
        content[offset + 16],
    ]);
    Ok(Uuid::from_u64_pair(msb, lsb))
}

/// Decodes an `Address` struct: `BEGIN`, initial frame `port:i32 @0`, host
/// string frame, then fast-forward to the struct `END`.
fn decode_address(cur: &mut FrameCursor) -> Result<SocketAddr> {
    cur.next_or_err("address begin frame")?; // BEGIN_DATA_STRUCTURE
    let initial = cur.next_or_err("address initial frame")?;
    let port = read_i32_le(&initial.content, 0)? as u16;
    let host_frame = cur.next_or_err("address host frame")?;
    let host = std::str::from_utf8(&host_frame.content)
        .map_err(|e| HazelcastError::Serialization(format!("address host not UTF-8: {e}")))?
        .to_string();
    cur.fast_forward_to_end_frame();
    let addr: SocketAddr = format!("{host}:{port}").parse().map_err(|e| {
        HazelcastError::Serialization(format!("address '{host}:{port}' not a SocketAddr: {e}"))
    })?;
    Ok(addr)
}

/// Decodes a `MemberInfo` struct: `BEGIN`, initial frame `uuid@0(17) lite@17`,
/// `AddressCodec`, then fast-forward past attributes/version/addressMap to `END`.
fn decode_member_info(cur: &mut FrameCursor) -> Result<DecodedMember> {
    cur.next_or_err("member begin frame")?; // BEGIN_DATA_STRUCTURE
    let initial = cur.next_or_err("member initial frame")?;
    if initial.content.len() < 18 {
        return Err(HazelcastError::Serialization(format!(
            "member initial frame too short ({} bytes, need 18)",
            initial.content.len()
        )));
    }
    let uuid = decode_uuid_fixed(&initial.content, 0)?;
    let lite = initial.content[17] != 0;
    let address = decode_address(cur)?;
    // Skip attributes (Map<String,String>), version (MemberVersion), and
    // addressMap (Map<EndpointQualifier,Address>) — not needed for routing —
    // and consume this member's END frame.
    cur.fast_forward_to_end_frame();
    Ok(DecodedMember {
        uuid,
        address,
        lite,
    })
}

/// Decodes a members-view event (`0x000302`): `i32 version` + `List<MemberInfo>`.
pub fn decode_members_view(msg: &ClientMessage) -> Result<(i32, Vec<DecodedMember>)> {
    let frames = msg.frames();
    let initial = frames
        .first()
        .ok_or_else(|| HazelcastError::Serialization("members-view: no frames".to_string()))?;
    let version = read_i32_le(&initial.content, EVENT_VERSION_OFFSET)?;

    let mut cur = FrameCursor::new(frames);
    cur.next_or_err("members-view initial frame")?;
    // List<MemberInfo>: BEGIN, items..., END.
    cur.next_or_err("members list begin")?;
    let mut members = Vec::new();
    while !cur.peek_is_end_data_structure() {
        members.push(decode_member_info(&mut cur)?);
    }
    Ok((version, members))
}

/// Decodes a partitions-view event (`0x000303`): `i32 version` +
/// `EntryList<UUID, List<Integer>>`. The entry list encodes the *values*
/// (`List<List<Integer>>`) first, then the *keys* (`List<UUID>`); entry `i` is
/// `(keys[i], values[i])`.
pub fn decode_partitions_view(msg: &ClientMessage) -> Result<(i32, Vec<(Uuid, Vec<i32>)>)> {
    let frames = msg.frames();
    let initial = frames
        .first()
        .ok_or_else(|| HazelcastError::Serialization("partitions-view: no frames".to_string()))?;
    let version = read_i32_le(&initial.content, EVENT_VERSION_OFFSET)?;

    let mut cur = FrameCursor::new(frames);
    cur.next_or_err("partitions-view initial frame")?;

    // values: List<List<Integer>> via ListMultiFrameCodec(ListIntegerCodec).
    cur.next_or_err("partition values list begin")?;
    let mut value_lists: Vec<Vec<i32>> = Vec::new();
    while !cur.peek_is_end_data_structure() {
        let f = cur.next_or_err("partition value list frame")?;
        let n = f.content.len() / 4;
        let mut ints = Vec::with_capacity(n);
        for i in 0..n {
            ints.push(read_i32_le(&f.content, i * 4)?);
        }
        value_lists.push(ints);
    }
    cur.next_or_err("partition values list end")?;

    // keys: List<UUID> — a single fixed-size frame of N*17 bytes (or a null frame).
    let key_frame = cur.next_or_err("partition keys frame")?;
    let keys: Vec<Uuid> = if key_frame.is_null_frame() {
        Vec::new()
    } else {
        let count = key_frame.content.len() / UUID_FIXED_SIZE;
        let mut keys = Vec::with_capacity(count);
        for i in 0..count {
            keys.push(decode_uuid_fixed(&key_frame.content, i * UUID_FIXED_SIZE)?);
        }
        keys
    };

    if keys.len() != value_lists.len() {
        return Err(HazelcastError::Serialization(format!(
            "partitions-view: {} keys but {} value-lists",
            keys.len(),
            value_lists.len()
        )));
    }

    let entries = keys.into_iter().zip(value_lists).collect();
    Ok((version, entries))
}

/// Inverts the partitions-view (uuid -> [partition_ids]) into a
/// partition_id -> owner_uuid table.
pub fn invert_partitions(entries: &[(Uuid, Vec<i32>)]) -> std::collections::HashMap<i32, Uuid> {
    let mut table = std::collections::HashMap::new();
    for (uuid, pids) in entries {
        for &pid in pids {
            table.insert(pid, *uuid);
        }
    }
    table
}

/// Env-gated (`HZ_DEBUG_CLUSTER_VIEW=1`) hexdump of a raw event message — used
/// to ground-truth the wire framing against a live member. No-op otherwise.
pub fn debug_dump_event(event: &ClientMessage) {
    if std::env::var("HZ_DEBUG_CLUSTER_VIEW").is_err() {
        return;
    }
    eprintln!(
        "[cluster-view] event type={:?} corr={:?} frames={}",
        event.message_type(),
        event.correlation_id(),
        event.frame_count()
    );
    for (i, f) in event.frames().iter().enumerate() {
        let hex: String = f
            .content
            .iter()
            .take(48)
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .join(" ");
        eprintln!(
            "  frame[{i:02}] flags=0x{:04x} len={:3} {}{}",
            f.flags,
            f.content.len(),
            hex,
            if f.content.len() > 48 { " ..." } else { "" }
        );
    }
}
