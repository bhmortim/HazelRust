//! XA Transaction support for distributed two-phase commit.
//!
//! This module implements the XA (eXtended Architecture) transaction interface
//! for distributed transaction processing, enabling two-phase commit across
//! Hazelcast cluster members.
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client::transaction::{XATransaction, Xid};
//!
//! let xid = Xid::new(0, b"global-txn-id", b"branch-001");
//! let mut xa_txn = client.new_xa_transaction(xid);
//!
//! xa_txn.start(XA_TMNOFLAGS).await?;
//! // ... perform operations ...
//! xa_txn.end(XA_TMSUCCESS).await?;
//!
//! let vote = xa_txn.prepare().await?;
//! if vote == XA_OK {
//!     xa_txn.commit(false).await?;
//! } else {
//!     xa_txn.rollback().await?;
//! }
//! ```

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use uuid::Uuid;

use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, HazelcastError, Result};

use crate::connection::ConnectionManager;

// ============================================================================
// XA Flags (from X/Open XA specification)
// ============================================================================

/// No flags set.
pub const XA_TMNOFLAGS: i32 = 0x00000000;

/// Caller is joining existing transaction branch.
pub const XA_TMJOIN: i32 = 0x00200000;

/// Caller is resuming association with suspended transaction branch.
pub const XA_TMRESUME: i32 = 0x08000000;

/// Dissociate caller from transaction branch - successful.
pub const XA_TMSUCCESS: i32 = 0x04000000;

/// Dissociate caller from transaction branch - failed.
pub const XA_TMFAIL: i32 = 0x20000000;

/// Caller is suspending (not ending) its association with transaction branch.
pub const XA_TMSUSPEND: i32 = 0x02000000;

/// Start a recovery scan.
pub const XA_TMSTARTRSCAN: i32 = 0x01000000;

/// End a recovery scan.
pub const XA_TMENDRSCAN: i32 = 0x00800000;

/// Use one-phase commit optimization.
pub const XA_TMONEPHASE: i32 = 0x40000000;

// ============================================================================
// XA Return Codes
// ============================================================================

/// Normal execution.
pub const XA_OK: i32 = 0;

/// The transaction branch has been read-only and has been committed.
pub const XA_RDONLY: i32 = 3;

/// The transaction work was rolled back (heuristically).
pub const XA_HEURRB: i32 = 6;

/// The transaction work was committed (heuristically).
pub const XA_HEURCOM: i32 = 7;

/// The transaction work may have been committed or rolled back (heuristic hazard).
pub const XA_HEURHAZ: i32 = 8;

/// The transaction work was partially committed and partially rolled back (heuristic mixed).
pub const XA_HEURMIX: i32 = 5;

/// Routine returned with no effect and may be reissued.
pub const XA_RETRY: i32 = 4;

// ============================================================================
// XA Error Codes
// ============================================================================

/// Resource manager error.
pub const XA_RBBASE: i32 = 100;

/// Rollback was caused by unspecified reason.
pub const XA_RBROLLBACK: i32 = XA_RBBASE;

/// Rollback was caused by communication failure.
pub const XA_RBCOMMFAIL: i32 = XA_RBBASE + 1;

/// A deadlock was detected.
pub const XA_RBDEADLOCK: i32 = XA_RBBASE + 2;

/// A condition that violates the integrity of the resource was detected.
pub const XA_RBINTEGRITY: i32 = XA_RBBASE + 3;

/// The resource manager rolled back for a reason not listed.
pub const XA_RBOTHER: i32 = XA_RBBASE + 4;

/// A protocol error occurred in the resource manager.
pub const XA_RBPROTO: i32 = XA_RBBASE + 5;

/// A transaction branch took too long.
pub const XA_RBTIMEOUT: i32 = XA_RBBASE + 6;

/// May retry the transaction branch.
pub const XA_RBTRANSIENT: i32 = XA_RBBASE + 7;

/// Upper bound of rollback error codes.
pub const XA_RBEND: i32 = XA_RBTRANSIENT;

// ============================================================================
// XA Transaction Identifier (Xid)
// ============================================================================

/// XA Transaction Identifier following X/Open XA specification.
///
/// An Xid uniquely identifies a global transaction and its branches.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Xid {
    format_id: i32,
    global_transaction_id: Vec<u8>,
    branch_qualifier: Vec<u8>,
}

impl Xid {
    /// Maximum length for global transaction ID.
    pub const MAXGTRIDSIZE: usize = 64;
    /// Maximum length for branch qualifier.
    pub const MAXBQUALSIZE: usize = 64;

    /// Creates a new XA transaction identifier.
    ///
    /// # Arguments
    ///
    /// * `format_id` - Format identifier (use 0 for default Hazelcast format)
    /// * `global_transaction_id` - Global transaction ID (max 64 bytes)
    /// * `branch_qualifier` - Branch qualifier (max 64 bytes)
    ///
    /// # Panics
    ///
    /// Panics if global_transaction_id or branch_qualifier exceeds maximum size.
    pub fn new(format_id: i32, global_transaction_id: &[u8], branch_qualifier: &[u8]) -> Self {
        assert!(
            global_transaction_id.len() <= Self::MAXGTRIDSIZE,
            "Global transaction ID exceeds maximum size of {} bytes",
            Self::MAXGTRIDSIZE
        );
        assert!(
            branch_qualifier.len() <= Self::MAXBQUALSIZE,
            "Branch qualifier exceeds maximum size of {} bytes",
            Self::MAXBQUALSIZE
        );

        Self {
            format_id,
            global_transaction_id: global_transaction_id.to_vec(),
            branch_qualifier: branch_qualifier.to_vec(),
        }
    }

    /// Creates an Xid with default format ID (0).
    pub fn with_default_format(global_transaction_id: &[u8], branch_qualifier: &[u8]) -> Self {
        Self::new(0, global_transaction_id, branch_qualifier)
    }

    /// Generates a new random Xid.
    pub fn generate() -> Self {
        let uuid = Uuid::new_v4();
        Self::new(0, uuid.as_bytes(), &[0u8; 8])
    }

    /// Returns the format identifier.
    pub fn format_id(&self) -> i32 {
        self.format_id
    }

    /// Returns the global transaction identifier.
    pub fn global_transaction_id(&self) -> &[u8] {
        &self.global_transaction_id
    }

    /// Returns the branch qualifier.
    pub fn branch_qualifier(&self) -> &[u8] {
        &self.branch_qualifier
    }

    /// Serializes the Xid to bytes for protocol transmission.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            4 + 4 + self.global_transaction_id.len() + 4 + self.branch_qualifier.len(),
        );
        bytes.extend_from_slice(&self.format_id.to_le_bytes());
        bytes.extend_from_slice(&(self.global_transaction_id.len() as i32).to_le_bytes());
        bytes.extend_from_slice(&self.global_transaction_id);
        bytes.extend_from_slice(&(self.branch_qualifier.len() as i32).to_le_bytes());
        bytes.extend_from_slice(&self.branch_qualifier);
        bytes
    }

    /// Deserializes an Xid from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 12 {
            return Err(HazelcastError::Serialization(
                "Xid data too short".to_string(),
            ));
        }

        let format_id = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let gtrid_len = i32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;

        if bytes.len() < 8 + gtrid_len + 4 {
            return Err(HazelcastError::Serialization(
                "Xid data too short for global transaction ID".to_string(),
            ));
        }

        let global_transaction_id = bytes[8..8 + gtrid_len].to_vec();
        let bqual_offset = 8 + gtrid_len;
        let bqual_len = i32::from_le_bytes([
            bytes[bqual_offset],
            bytes[bqual_offset + 1],
            bytes[bqual_offset + 2],
            bytes[bqual_offset + 3],
        ]) as usize;

        if bytes.len() < bqual_offset + 4 + bqual_len {
            return Err(HazelcastError::Serialization(
                "Xid data too short for branch qualifier".to_string(),
            ));
        }

        let branch_qualifier = bytes[bqual_offset + 4..bqual_offset + 4 + bqual_len].to_vec();

        Ok(Self {
            format_id,
            global_transaction_id,
            branch_qualifier,
        })
    }
}

impl Default for Xid {
    fn default() -> Self {
        Self::generate()
    }
}

// ============================================================================
// XA Transaction State
// ============================================================================

/// The state of an XA transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XaTransactionState {
    /// Transaction has not been started.
    NotStarted,
    /// Transaction is active (between start and end).
    Active,
    /// Transaction is idle (after end, before prepare).
    Idle,
    /// Transaction has been suspended.
    Suspended,
    /// Transaction has been prepared (vote received).
    Prepared,
    /// Transaction has been committed.
    Committed,
    /// Transaction has been rolled back.
    RolledBack,
    /// Transaction was heuristically completed.
    HeuristicallyCompleted,
}

impl XaTransactionState {
    /// Returns true if the transaction can be started.
    pub fn can_start(&self) -> bool {
        matches!(self, Self::NotStarted | Self::Suspended)
    }

    /// Returns true if the transaction can be ended.
    pub fn can_end(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// Returns true if the transaction can be prepared.
    pub fn can_prepare(&self) -> bool {
        matches!(self, Self::Idle)
    }

    /// Returns true if the transaction can be committed.
    pub fn can_commit(&self) -> bool {
        matches!(self, Self::Idle | Self::Prepared)
    }

    /// Returns true if the transaction can be rolled back.
    pub fn can_rollback(&self) -> bool {
        matches!(self, Self::Active | Self::Idle | Self::Prepared)
    }

    /// Returns true if the transaction is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Committed | Self::RolledBack | Self::HeuristicallyCompleted
        )
    }
}

// ============================================================================
// XA Resource Trait
// ============================================================================

/// XA Resource interface following the X/Open XA specification.
///
/// This trait defines the contract for XA-compliant resource managers
/// that participate in distributed transactions.
#[allow(async_fn_in_trait)]
pub trait XAResource: Send + Sync {
    /// Starts work on behalf of a transaction branch.
    ///
    /// # Arguments
    ///
    /// * `flags` - One of XA_TMNOFLAGS, XA_TMJOIN, or XA_TMRESUME
    async fn start(&mut self, flags: i32) -> Result<()>;

    /// Ends work on behalf of a transaction branch.
    ///
    /// # Arguments
    ///
    /// * `flags` - One of XA_TMSUCCESS, XA_TMFAIL, or XA_TMSUSPEND
    async fn end(&mut self, flags: i32) -> Result<()>;

    /// Prepares the transaction branch for commit.
    ///
    /// Returns XA_OK if the transaction can be committed, XA_RDONLY if
    /// the transaction was read-only and has been committed.
    async fn prepare(&mut self) -> Result<i32>;

    /// Commits the transaction branch.
    ///
    /// # Arguments
    ///
    /// * `one_phase` - If true, use one-phase commit optimization
    async fn commit(&mut self, one_phase: bool) -> Result<()>;

    /// Rolls back the transaction branch.
    async fn rollback(&mut self) -> Result<()>;

    /// Forgets about a heuristically completed transaction branch.
    async fn forget(&mut self) -> Result<()>;

    /// Obtains a list of prepared transaction branches.
    ///
    /// # Arguments
    ///
    /// * `flags` - One of XA_TMSTARTRSCAN, XA_TMENDRSCAN, or XA_TMNOFLAGS
    async fn recover(&self, flags: i32) -> Result<Vec<Xid>>;

    /// Returns the transaction timeout value.
    fn get_transaction_timeout(&self) -> Duration;

    /// Sets the transaction timeout value.
    ///
    /// Returns true if the timeout was set successfully.
    fn set_transaction_timeout(&mut self, timeout: Duration) -> bool;

    /// Determines if this resource manager is the same as another.
    fn is_same_rm(&self, other: &Self) -> bool;

    /// Returns the XA transaction identifier.
    fn xid(&self) -> &Xid;
}

// ============================================================================
// XA Transaction Implementation
// ============================================================================

/// XA Transaction implementation for Hazelcast.
///
/// Manages the lifecycle of an XA transaction with support for two-phase commit.
#[derive(Debug)]
pub struct XATransaction {
    connection_manager: Arc<ConnectionManager>,
    xid: Xid,
    txn_id: Uuid,
    thread_id: i64,
    state: XaTransactionState,
    timeout: Duration,
    start_time: Option<std::time::Instant>,
}

impl XATransaction {
    /// Creates a new XA transaction.
    pub fn new(connection_manager: Arc<ConnectionManager>, xid: Xid) -> Self {
        static THREAD_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

        Self {
            connection_manager,
            xid,
            txn_id: Uuid::new_v4(),
            thread_id: THREAD_ID_COUNTER.fetch_add(1, Ordering::Relaxed) as i64,
            state: XaTransactionState::NotStarted,
            timeout: Duration::from_secs(120),
            start_time: None,
        }
    }

    /// Creates a new XA transaction with auto-generated Xid.
    pub fn with_generated_xid(connection_manager: Arc<ConnectionManager>) -> Self {
        Self::new(connection_manager, Xid::generate())
    }

    /// Returns the transaction ID.
    pub fn txn_id(&self) -> Uuid {
        self.txn_id
    }

    /// Returns the current transaction state.
    pub fn state(&self) -> XaTransactionState {
        self.state
    }

    /// Returns true if the transaction is active.
    pub fn is_active(&self) -> bool {
        self.state == XaTransactionState::Active
    }

    /// Returns true if the transaction is prepared.
    pub fn is_prepared(&self) -> bool {
        self.state == XaTransactionState::Prepared
    }

    /// Returns the elapsed time since the transaction started.
    pub fn elapsed(&self) -> Option<Duration> {
        self.start_time.map(|t| t.elapsed())
    }

    /// Checks if the transaction has timed out.
    pub fn is_timed_out(&self) -> bool {
        self.elapsed().map(|e| e > self.timeout).unwrap_or(false)
    }

    fn validate_state_for_start(&self, flags: i32) -> Result<()> {
        if flags & XA_TMJOIN != 0 || flags & XA_TMRESUME != 0 {
            if self.state != XaTransactionState::Suspended
                && self.state != XaTransactionState::NotStarted
            {
                return Err(HazelcastError::Protocol(format!(
                    "Cannot join/resume XA transaction in state {:?}",
                    self.state
                )));
            }
        } else if !self.state.can_start() {
            return Err(HazelcastError::Protocol(format!(
                "Cannot start XA transaction in state {:?}",
                self.state
            )));
        }
        Ok(())
    }

    fn validate_state_for_end(&self) -> Result<()> {
        if !self.state.can_end() {
            return Err(HazelcastError::Protocol(format!(
                "Cannot end XA transaction in state {:?}",
                self.state
            )));
        }
        Ok(())
    }

    fn validate_state_for_prepare(&self) -> Result<()> {
        if !self.state.can_prepare() {
            return Err(HazelcastError::Protocol(format!(
                "Cannot prepare XA transaction in state {:?}",
                self.state
            )));
        }
        Ok(())
    }

    fn validate_state_for_commit(&self, one_phase: bool) -> Result<()> {
        if one_phase {
            if !matches!(self.state, XaTransactionState::Idle) {
                return Err(HazelcastError::Protocol(format!(
                    "Cannot one-phase commit XA transaction in state {:?}",
                    self.state
                )));
            }
        } else if !self.state.can_commit() {
            return Err(HazelcastError::Protocol(format!(
                "Cannot commit XA transaction in state {:?}",
                self.state
            )));
        }
        Ok(())
    }

    fn validate_state_for_rollback(&self) -> Result<()> {
        if !self.state.can_rollback() {
            return Err(HazelcastError::Protocol(format!(
                "Cannot rollback XA transaction in state {:?}",
                self.state
            )));
        }
        Ok(())
    }

    fn long_frame(value: i64) -> Frame {
        let mut buf = BytesMut::with_capacity(8);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
    }

    fn int_frame(value: i32) -> Frame {
        let mut buf = BytesMut::with_capacity(4);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
    }

    fn uuid_frame(uuid: Uuid) -> Frame {
        let mut buf = BytesMut::with_capacity(16);
        buf.extend_from_slice(uuid.as_bytes());
        Frame::with_content(buf)
    }

    fn data_frame(data: &[u8]) -> Frame {
        Frame::with_content(BytesMut::from(data))
    }

    fn bool_frame(value: bool) -> Frame {
        let mut buf = BytesMut::with_capacity(1);
        buf.extend_from_slice(&[if value { 1u8 } else { 0u8 }]);
        Frame::with_content(buf)
    }

    async fn invoke(&self, message: ClientMessage) -> Result<ClientMessage> {
        let address = self.get_connection_address().await?;
        self.connection_manager.send_to(address, message).await?;
        self.connection_manager
            .receive_from(address)
            .await?
            .ok_or_else(|| HazelcastError::Connection("connection closed unexpectedly".to_string()))
    }

    async fn get_connection_address(&self) -> Result<SocketAddr> {
        let addresses = self.connection_manager.connected_addresses().await;
        addresses
            .into_iter()
            .next()
            .ok_or_else(|| HazelcastError::Connection("no connections available".to_string()))
    }

    fn decode_int_response(response: &ClientMessage) -> Result<i32> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("empty response".to_string()));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 4 {
            let offset = RESPONSE_HEADER_SIZE;
            Ok(i32::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
            ]))
        } else {
            Ok(0)
        }
    }
}

impl XAResource for XATransaction {
    async fn start(&mut self, flags: i32) -> Result<()> {
        self.validate_state_for_start(flags)?;

        let timeout_ms = self.timeout.as_millis() as i64;

        let mut message = ClientMessage::create_for_encode_any_partition(XA_TXN_CREATE);
        message.add_frame(Self::data_frame(&self.xid.to_bytes()));
        message.add_frame(Self::long_frame(timeout_ms));
        message.add_frame(Self::long_frame(self.thread_id));

        let _response = self.invoke(message).await?;

        self.state = XaTransactionState::Active;
        self.start_time = Some(std::time::Instant::now());

        Ok(())
    }

    async fn end(&mut self, flags: i32) -> Result<()> {
        self.validate_state_for_end()?;

        if flags & XA_TMSUSPEND != 0 {
            self.state = XaTransactionState::Suspended;
        } else if flags & XA_TMFAIL != 0 {
            self.state = XaTransactionState::Idle;
        } else {
            self.state = XaTransactionState::Idle;
        }

        Ok(())
    }

    async fn prepare(&mut self) -> Result<i32> {
        self.validate_state_for_prepare()?;

        let mut message = ClientMessage::create_for_encode_any_partition(XA_TXN_PREPARE);
        message.add_frame(Self::data_frame(&self.xid.to_bytes()));
        message.add_frame(Self::uuid_frame(self.txn_id));
        message.add_frame(Self::long_frame(self.thread_id));

        let response = self.invoke(message).await?;
        let vote = Self::decode_int_response(&response)?;

        if vote == XA_OK || vote == XA_RDONLY {
            self.state = XaTransactionState::Prepared;
        }

        Ok(vote)
    }

    async fn commit(&mut self, one_phase: bool) -> Result<()> {
        self.validate_state_for_commit(one_phase)?;

        let mut message = ClientMessage::create_for_encode_any_partition(XA_TXN_COMMIT);
        message.add_frame(Self::data_frame(&self.xid.to_bytes()));
        message.add_frame(Self::uuid_frame(self.txn_id));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::bool_frame(one_phase));

        let _response = self.invoke(message).await?;

        self.state = XaTransactionState::Committed;
        Ok(())
    }

    async fn rollback(&mut self) -> Result<()> {
        self.validate_state_for_rollback()?;

        let mut message = ClientMessage::create_for_encode_any_partition(XA_TXN_ROLLBACK);
        message.add_frame(Self::data_frame(&self.xid.to_bytes()));
        message.add_frame(Self::uuid_frame(self.txn_id));
        message.add_frame(Self::long_frame(self.thread_id));

        let _response = self.invoke(message).await?;

        self.state = XaTransactionState::RolledBack;
        Ok(())
    }

    async fn forget(&mut self) -> Result<()> {
        if !self.state.is_terminal() {
            return Err(HazelcastError::Protocol(
                "Can only forget heuristically completed transactions".to_string(),
            ));
        }

        let mut message = ClientMessage::create_for_encode_any_partition(XA_TXN_CLEAR_REMOTE);
        message.add_frame(Self::data_frame(&self.xid.to_bytes()));

        let _response = self.invoke(message).await?;

        Ok(())
    }

    async fn recover(&self, flags: i32) -> Result<Vec<Xid>> {
        let mut message =
            ClientMessage::create_for_encode_any_partition(XA_TXN_COLLECT_TRANSACTIONS);
        message.add_frame(Self::int_frame(flags));

        let response = self.invoke(message).await?;

        let frames = response.frames();
        let mut xids = Vec::new();

        for frame in frames.iter().skip(1) {
            if frame.flags & IS_NULL_FLAG != 0 || frame.content.is_empty() {
                continue;
            }
            if let Ok(xid) = Xid::from_bytes(&frame.content) {
                xids.push(xid);
            }
        }

        Ok(xids)
    }

    fn get_transaction_timeout(&self) -> Duration {
        self.timeout
    }

    fn set_transaction_timeout(&mut self, timeout: Duration) -> bool {
        self.timeout = timeout;
        true
    }

    fn is_same_rm(&self, other: &Self) -> bool {
        self.xid == *other.xid()
    }

    fn xid(&self) -> &Xid {
        &self.xid
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Xid Tests
    // ========================================================================

    #[test]
    fn test_xid_new() {
        let xid = Xid::new(42, b"global-txn-123", b"branch-001");
        assert_eq!(xid.format_id(), 42);
        assert_eq!(xid.global_transaction_id(), b"global-txn-123");
        assert_eq!(xid.branch_qualifier(), b"branch-001");
    }

    #[test]
    fn test_xid_with_default_format() {
        let xid = Xid::with_default_format(b"test-gtrid", b"test-bqual");
        assert_eq!(xid.format_id(), 0);
        assert_eq!(xid.global_transaction_id(), b"test-gtrid");
        assert_eq!(xid.branch_qualifier(), b"test-bqual");
    }

    #[test]
    fn test_xid_generate() {
        let xid1 = Xid::generate();
        let xid2 = Xid::generate();
        assert_ne!(xid1.global_transaction_id(), xid2.global_transaction_id());
    }

    #[test]
    fn test_xid_serialization_roundtrip() {
        let original = Xid::new(123, b"my-global-txn-id", b"my-branch");
        let bytes = original.to_bytes();
        let restored = Xid::from_bytes(&bytes).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_xid_empty_qualifiers() {
        let xid = Xid::new(0, b"", b"");
        let bytes = xid.to_bytes();
        let restored = Xid::from_bytes(&bytes).unwrap();
        assert_eq!(xid, restored);
    }

    #[test]
    #[should_panic(expected = "Global transaction ID exceeds maximum size")]
    fn test_xid_gtrid_too_long() {
        let long_gtrid = vec![0u8; Xid::MAXGTRIDSIZE + 1];
        Xid::new(0, &long_gtrid, b"");
    }

    #[test]
    #[should_panic(expected = "Branch qualifier exceeds maximum size")]
    fn test_xid_bqual_too_long() {
        let long_bqual = vec![0u8; Xid::MAXBQUALSIZE + 1];
        Xid::new(0, b"", &long_bqual);
    }

    #[test]
    fn test_xid_max_size() {
        let max_gtrid = vec![0xABu8; Xid::MAXGTRIDSIZE];
        let max_bqual = vec![0xCDu8; Xid::MAXBQUALSIZE];
        let xid = Xid::new(0, &max_gtrid, &max_bqual);
        assert_eq!(xid.global_transaction_id().len(), Xid::MAXGTRIDSIZE);
        assert_eq!(xid.branch_qualifier().len(), Xid::MAXBQUALSIZE);
    }

    #[test]
    fn test_xid_from_bytes_too_short() {
        let short_bytes = vec![0u8; 8];
        assert!(Xid::from_bytes(&short_bytes).is_err());
    }

    #[test]
    fn test_xid_clone() {
        let xid = Xid::new(1, b"test", b"branch");
        let cloned = xid.clone();
        assert_eq!(xid, cloned);
    }

    #[test]
    fn test_xid_hash() {
        use std::collections::HashSet;
        let xid1 = Xid::new(1, b"test", b"branch");
        let xid2 = Xid::new(1, b"test", b"branch");
        let xid3 = Xid::new(2, b"test", b"branch");

        let mut set = HashSet::new();
        set.insert(xid1.clone());
        assert!(set.contains(&xid2));
        assert!(!set.contains(&xid3));
    }

    #[test]
    fn test_xid_default() {
        let xid = Xid::default();
        assert_eq!(xid.format_id(), 0);
        assert!(!xid.global_transaction_id().is_empty());
    }

    // ========================================================================
    // XA Transaction State Tests
    // ========================================================================

    #[test]
    fn test_state_can_start() {
        assert!(XaTransactionState::NotStarted.can_start());
        assert!(XaTransactionState::Suspended.can_start());
        assert!(!XaTransactionState::Active.can_start());
        assert!(!XaTransactionState::Idle.can_start());
        assert!(!XaTransactionState::Prepared.can_start());
        assert!(!XaTransactionState::Committed.can_start());
        assert!(!XaTransactionState::RolledBack.can_start());
    }

    #[test]
    fn test_state_can_end() {
        assert!(XaTransactionState::Active.can_end());
        assert!(!XaTransactionState::NotStarted.can_end());
        assert!(!XaTransactionState::Idle.can_end());
        assert!(!XaTransactionState::Prepared.can_end());
        assert!(!XaTransactionState::Committed.can_end());
    }

    #[test]
    fn test_state_can_prepare() {
        assert!(XaTransactionState::Idle.can_prepare());
        assert!(!XaTransactionState::NotStarted.can_prepare());
        assert!(!XaTransactionState::Active.can_prepare());
        assert!(!XaTransactionState::Prepared.can_prepare());
        assert!(!XaTransactionState::Committed.can_prepare());
    }

    #[test]
    fn test_state_can_commit() {
        assert!(XaTransactionState::Idle.can_commit());
        assert!(XaTransactionState::Prepared.can_commit());
        assert!(!XaTransactionState::NotStarted.can_commit());
        assert!(!XaTransactionState::Active.can_commit());
        assert!(!XaTransactionState::Committed.can_commit());
    }

    #[test]
    fn test_state_can_rollback() {
        assert!(XaTransactionState::Active.can_rollback());
        assert!(XaTransactionState::Idle.can_rollback());
        assert!(XaTransactionState::Prepared.can_rollback());
        assert!(!XaTransactionState::NotStarted.can_rollback());
        assert!(!XaTransactionState::Committed.can_rollback());
        assert!(!XaTransactionState::RolledBack.can_rollback());
    }

    #[test]
    fn test_state_is_terminal() {
        assert!(XaTransactionState::Committed.is_terminal());
        assert!(XaTransactionState::RolledBack.is_terminal());
        assert!(XaTransactionState::HeuristicallyCompleted.is_terminal());
        assert!(!XaTransactionState::NotStarted.is_terminal());
        assert!(!XaTransactionState::Active.is_terminal());
        assert!(!XaTransactionState::Idle.is_terminal());
        assert!(!XaTransactionState::Prepared.is_terminal());
    }

    // ========================================================================
    // XA Flags and Return Codes Tests
    // ========================================================================

    #[test]
    fn test_xa_flags_values() {
        assert_eq!(XA_TMNOFLAGS, 0x00000000);
        assert_eq!(XA_TMJOIN, 0x00200000);
        assert_eq!(XA_TMRESUME, 0x08000000);
        assert_eq!(XA_TMSUCCESS, 0x04000000);
        assert_eq!(XA_TMFAIL, 0x20000000);
        assert_eq!(XA_TMSUSPEND, 0x02000000);
        assert_eq!(XA_TMSTARTRSCAN, 0x01000000);
        assert_eq!(XA_TMENDRSCAN, 0x00800000);
        assert_eq!(XA_TMONEPHASE, 0x40000000);
    }

    #[test]
    fn test_xa_return_codes() {
        assert_eq!(XA_OK, 0);
        assert_eq!(XA_RDONLY, 3);
        assert_eq!(XA_HEURRB, 6);
        assert_eq!(XA_HEURCOM, 7);
        assert_eq!(XA_HEURHAZ, 8);
        assert_eq!(XA_HEURMIX, 5);
        assert_eq!(XA_RETRY, 4);
    }

    #[test]
    fn test_xa_error_codes() {
        assert_eq!(XA_RBBASE, 100);
        assert_eq!(XA_RBROLLBACK, 100);
        assert_eq!(XA_RBCOMMFAIL, 101);
        assert_eq!(XA_RBDEADLOCK, 102);
        assert_eq!(XA_RBINTEGRITY, 103);
        assert_eq!(XA_RBOTHER, 104);
        assert_eq!(XA_RBPROTO, 105);
        assert_eq!(XA_RBTIMEOUT, 106);
        assert_eq!(XA_RBTRANSIENT, 107);
        assert_eq!(XA_RBEND, XA_RBTRANSIENT);
    }

    // ========================================================================
    // XA Transaction State Machine Tests
    // ========================================================================

    #[test]
    fn test_state_transitions_happy_path() {
        let mut state = XaTransactionState::NotStarted;

        assert!(state.can_start());
        state = XaTransactionState::Active;

        assert!(state.can_end());
        state = XaTransactionState::Idle;

        assert!(state.can_prepare());
        state = XaTransactionState::Prepared;

        assert!(state.can_commit());
        state = XaTransactionState::Committed;

        assert!(state.is_terminal());
    }

    #[test]
    fn test_state_transitions_rollback_from_active() {
        let mut state = XaTransactionState::NotStarted;

        state = XaTransactionState::Active;
        assert!(state.can_rollback());
        state = XaTransactionState::RolledBack;
        assert!(state.is_terminal());
    }

    #[test]
    fn test_state_transitions_rollback_from_idle() {
        let mut state = XaTransactionState::Idle;
        assert!(state.can_rollback());
        state = XaTransactionState::RolledBack;
        assert!(state.is_terminal());
    }

    #[test]
    fn test_state_transitions_rollback_from_prepared() {
        let mut state = XaTransactionState::Prepared;
        assert!(state.can_rollback());
        state = XaTransactionState::RolledBack;
        assert!(state.is_terminal());
    }

    #[test]
    fn test_state_transitions_one_phase_commit() {
        let mut state = XaTransactionState::NotStarted;

        state = XaTransactionState::Active;
        state = XaTransactionState::Idle;

        assert!(state.can_commit());
        state = XaTransactionState::Committed;

        assert!(state.is_terminal());
    }

    #[test]
    fn test_state_transitions_suspend_resume() {
        let mut state = XaTransactionState::NotStarted;

        state = XaTransactionState::Active;

        state = XaTransactionState::Suspended;
        assert!(state.can_start());

        state = XaTransactionState::Active;
        assert!(state.can_end());
    }

    // ========================================================================
    // XA Transaction Type Tests
    // ========================================================================

    #[test]
    fn test_xa_transaction_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<XATransaction>();
    }

    #[test]
    fn test_xid_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Xid>();
    }

    #[test]
    fn test_xa_state_copy() {
        let state = XaTransactionState::Active;
        let copied = state;
        assert_eq!(state, copied);
    }

    #[test]
    fn test_xa_state_debug() {
        let state = XaTransactionState::Prepared;
        let debug = format!("{:?}", state);
        assert!(debug.contains("Prepared"));
    }
}
