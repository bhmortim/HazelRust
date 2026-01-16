//! SQL service for executing queries.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use hazelcast_core::protocol::constants::{
    PARTITION_ID_ANY, SQL_CLOSE, SQL_EXECUTE, SQL_FETCH,
};
use hazelcast_core::protocol::ClientMessage;
use hazelcast_core::{HazelcastError, Result};

use crate::connection::ConnectionManager;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike};
use rust_decimal::Decimal;

use super::{SqlColumnMetadata, SqlColumnType, SqlRow, SqlRowMetadata, SqlStatement, SqlValue};

const EPOCH_DATE: i32 = 719_163; // Days from year 0 to 1970-01-01

static QUERY_ID_COUNTER: AtomicI64 = AtomicI64::new(1);

/// Unique identifier for a SQL query cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlQueryId {
    member_id_high: i64,
    member_id_low: i64,
    local_id_high: i64,
    local_id_low: i64,
}

impl SqlQueryId {
    /// Creates a new query ID from components.
    fn new(member_id_high: i64, member_id_low: i64, local_id_high: i64, local_id_low: i64) -> Self {
        Self {
            member_id_high,
            member_id_low,
            local_id_high,
            local_id_low,
        }
    }

    /// Generates a new query ID for client-initiated queries.
    fn generate() -> Self {
        let id = QUERY_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        Self {
            member_id_high: 0,
            member_id_low: 0,
            local_id_high: timestamp,
            local_id_low: id,
        }
    }

    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.member_id_high.to_le_bytes());
        buf.extend_from_slice(&self.member_id_low.to_le_bytes());
        buf.extend_from_slice(&self.local_id_high.to_le_bytes());
        buf.extend_from_slice(&self.local_id_low.to_le_bytes());
    }

    fn decode(data: &[u8], offset: usize) -> Result<Self> {
        if data.len() < offset + 32 {
            return Err(HazelcastError::Protocol(
                "insufficient data for SqlQueryId".into(),
            ));
        }
        let member_id_high = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        let member_id_low = i64::from_le_bytes(data[offset + 8..offset + 16].try_into().unwrap());
        let local_id_high = i64::from_le_bytes(data[offset + 16..offset + 24].try_into().unwrap());
        let local_id_low = i64::from_le_bytes(data[offset + 24..offset + 32].try_into().unwrap());
        Ok(Self::new(
            member_id_high,
            member_id_low,
            local_id_high,
            local_id_low,
        ))
    }
}

/// Service for executing SQL queries against the Hazelcast cluster.
#[derive(Debug, Clone)]
pub struct SqlService {
    connection_manager: Arc<ConnectionManager>,
}

impl SqlService {
    /// Creates a new SQL service.
    pub(crate) fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }

    /// Executes a SQL statement and returns a result for iterating over rows.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let sql = client.sql();
    /// let statement = SqlStatement::new("SELECT * FROM users WHERE age > ?")
    ///     .add_parameter(SqlValue::Integer(21));
    ///
    /// let mut result = sql.execute(statement).await?;
    /// while let Some(row) = result.next_row().await? {
    ///     println!("Name: {:?}", row.get_by_name("name"));
    /// }
    /// result.close().await?;
    /// ```
    pub async fn execute(&self, statement: SqlStatement) -> Result<SqlResult> {
        let query_id = SqlQueryId::generate();
        let cursor_buffer_size = statement.cursor_buffer_size().unwrap_or(4096);

        let request = self.build_execute_request(&statement, &query_id, cursor_buffer_size)?;
        let response = self.connection_manager.invoke(request).await?;

        self.parse_execute_response(response, query_id, cursor_buffer_size)
    }

    fn build_execute_request(
        &self,
        statement: &SqlStatement,
        query_id: &SqlQueryId,
        cursor_buffer_size: i32,
    ) -> Result<ClientMessage> {
        let mut initial_frame_content = Vec::with_capacity(64);

        // Timeout millis (-1 for no timeout)
        let timeout_millis: i64 = statement
            .timeout()
            .map(|d| d.as_millis() as i64)
            .unwrap_or(-1);
        initial_frame_content.extend_from_slice(&timeout_millis.to_le_bytes());

        // Cursor buffer size
        initial_frame_content.extend_from_slice(&cursor_buffer_size.to_le_bytes());

        // Expected result type: 1 = ROWS, 2 = UPDATE_COUNT, 0 = ANY
        let expected_result_type: i8 = 0; // ANY
        initial_frame_content.push(expected_result_type as u8);

        // Query ID
        query_id.encode(&mut initial_frame_content);

        // Skip update statistics
        initial_frame_content.push(0); // false

        let mut request = ClientMessage::new_request(SQL_EXECUTE, PARTITION_ID_ANY);
        request.add_frame_with_data(initial_frame_content);

        // SQL string frame
        request.add_string_frame(statement.query());

        // Parameters frame - encode as list of Data objects
        self.encode_parameters(&mut request, statement.parameters())?;

        // Schema frame (nullable)
        if let Some(schema) = statement.schema() {
            request.add_string_frame(schema);
        } else {
            request.add_null_frame();
        }

        Ok(request)
    }

    fn encode_parameters(&self, request: &mut ClientMessage, parameters: &[SqlValue]) -> Result<()> {
        if parameters.is_empty() {
            // Empty list frame
            let mut buf = Vec::new();
            buf.extend_from_slice(&0i32.to_le_bytes()); // count = 0
            request.add_frame_with_data(buf);
            return Ok(());
        }

        // Encode parameter count and each parameter
        let mut buf = Vec::new();
        buf.extend_from_slice(&(parameters.len() as i32).to_le_bytes());

        for param in parameters {
            self.encode_sql_value(&mut buf, param)?;
        }

        request.add_frame_with_data(buf);
        Ok(())
    }

    fn encode_sql_value(&self, buf: &mut Vec<u8>, value: &SqlValue) -> Result<()> {
        match value {
            SqlValue::Null => {
                buf.push(0); // null marker
            }
            SqlValue::Varchar(s) => {
                buf.push(1); // non-null marker
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as i32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            SqlValue::Boolean(b) => {
                buf.push(1);
                buf.push(if *b { 1 } else { 0 });
            }
            SqlValue::TinyInt(v) => {
                buf.push(1);
                buf.push(*v as u8);
            }
            SqlValue::SmallInt(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            SqlValue::Integer(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            SqlValue::BigInt(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            SqlValue::Real(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            SqlValue::Double(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            SqlValue::Decimal(d) => {
                buf.push(1);
                let scale = d.scale() as i32;
                buf.extend_from_slice(&scale.to_le_bytes());
                let mantissa = d.mantissa();
                buf.extend_from_slice(&mantissa.to_be_bytes());
            }
            SqlValue::Date(d) => {
                buf.push(1);
                let epoch_days = d.num_days_from_ce() - EPOCH_DATE;
                buf.extend_from_slice(&epoch_days.to_le_bytes());
            }
            SqlValue::Time(t) => {
                buf.push(1);
                let nanos = t.num_seconds_from_midnight() as i64 * 1_000_000_000
                    + t.nanosecond() as i64 % 1_000_000_000;
                buf.extend_from_slice(&nanos.to_le_bytes());
            }
            SqlValue::Timestamp(ts) => {
                buf.push(1);
                let epoch_secs = ts.and_utc().timestamp();
                let nanos = ts.and_utc().timestamp_subsec_nanos() as i32;
                buf.extend_from_slice(&epoch_secs.to_le_bytes());
                buf.extend_from_slice(&nanos.to_le_bytes());
            }
            SqlValue::TimestampWithTimeZone(ts) => {
                buf.push(1);
                let epoch_secs = ts.timestamp();
                let nanos = ts.timestamp_subsec_nanos() as i32;
                let offset_secs = ts.offset().local_minus_utc();
                buf.extend_from_slice(&epoch_secs.to_le_bytes());
                buf.extend_from_slice(&nanos.to_le_bytes());
                buf.extend_from_slice(&offset_secs.to_le_bytes());
            }
            SqlValue::Bytes(b) => {
                buf.push(1);
                buf.extend_from_slice(&(b.len() as i32).to_le_bytes());
                buf.extend_from_slice(b);
            }
        }
        Ok(())
    }

    fn parse_execute_response(
        &self,
        response: ClientMessage,
        query_id: SqlQueryId,
        cursor_buffer_size: i32,
    ) -> Result<SqlResult> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Protocol("empty SQL response".into()));
        }

        let initial_frame = &frames[0];
        let data = initial_frame.content();

        // Parse initial frame content
        // Response header offset (after standard response header)
        let mut offset = 0;

        // Row metadata present flag
        if data.len() < offset + 1 {
            return Err(HazelcastError::Protocol(
                "missing row metadata flag".into(),
            ));
        }
        let row_metadata_present = data[offset] != 0;
        offset += 1;

        // Row page present flag
        if data.len() < offset + 1 {
            return Err(HazelcastError::Protocol("missing row page flag".into()));
        }
        let row_page_present = data[offset] != 0;
        offset += 1;

        // Update count
        if data.len() < offset + 8 {
            return Err(HazelcastError::Protocol("missing update count".into()));
        }
        let update_count = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Is last flag (indicates if more rows available)
        if data.len() < offset + 1 {
            return Err(HazelcastError::Protocol("missing is_last flag".into()));
        }
        let is_last = data[offset] != 0;

        // Parse row metadata from subsequent frames
        let row_metadata = if row_metadata_present && frames.len() > 1 {
            Some(self.parse_row_metadata(&frames[1..])?)
        } else {
            None
        };

        // Parse rows from page data
        let rows = if row_page_present {
            self.parse_row_page(&frames, row_metadata.as_ref())?
        } else {
            VecDeque::new()
        };

        Ok(SqlResult {
            connection_manager: Arc::clone(&self.connection_manager),
            query_id,
            row_metadata,
            rows,
            cursor_buffer_size,
            is_closed: is_last,
            update_count,
        })
    }

    fn parse_row_metadata(&self, frames: &[hazelcast_core::protocol::Frame]) -> Result<SqlRowMetadata> {
        if frames.is_empty() {
            return Ok(SqlRowMetadata::new(Vec::new()));
        }

        let data = frames[0].content();
        let mut offset = 0;

        // Column count
        if data.len() < 4 {
            return Err(HazelcastError::Protocol("missing column count".into()));
        }
        let column_count = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut columns = Vec::with_capacity(column_count);

        for _ in 0..column_count {
            // Column type
            if data.len() < offset + 4 {
                return Err(HazelcastError::Protocol("missing column type".into()));
            }
            let type_id = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;

            let column_type = SqlColumnType::from_id(type_id).ok_or_else(|| {
                HazelcastError::Protocol(format!("unknown column type: {}", type_id))
            })?;

            // Nullable flag
            if data.len() < offset + 1 {
                return Err(HazelcastError::Protocol("missing nullable flag".into()));
            }
            let nullable = data[offset] != 0;
            offset += 1;

            // Column name length and value
            if data.len() < offset + 4 {
                return Err(HazelcastError::Protocol("missing column name length".into()));
            }
            let name_len = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            if data.len() < offset + name_len {
                return Err(HazelcastError::Protocol("missing column name".into()));
            }
            let name = String::from_utf8_lossy(&data[offset..offset + name_len]).to_string();
            offset += name_len;

            columns.push(SqlColumnMetadata::new(name, column_type, nullable));
        }

        Ok(SqlRowMetadata::new(columns))
    }

    fn parse_row_page(
        &self,
        frames: &[hazelcast_core::protocol::Frame],
        metadata: Option<&SqlRowMetadata>,
    ) -> Result<VecDeque<SqlRow>> {
        let mut rows = VecDeque::new();

        // Find the row data frame (after metadata frames)
        let row_frame_idx = if metadata.is_some() { 2 } else { 1 };
        if frames.len() <= row_frame_idx {
            return Ok(rows);
        }

        let data = frames[row_frame_idx].content();
        if data.is_empty() {
            return Ok(rows);
        }

        let mut offset = 0;

        // Row count
        if data.len() < 4 {
            return Ok(rows);
        }
        let row_count = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let metadata = match metadata {
            Some(m) => m.clone(),
            None => return Ok(rows),
        };

        let column_count = metadata.column_count();

        for _ in 0..row_count {
            let mut values = Vec::with_capacity(column_count);

            for col_idx in 0..column_count {
                let value = self.decode_sql_value(data, &mut offset, metadata.column(col_idx))?;
                values.push(value);
            }

            rows.push_back(SqlRow::new(metadata.clone(), values));
        }

        Ok(rows)
    }

    fn decode_sql_value(
        &self,
        data: &[u8],
        offset: &mut usize,
        column: Option<&SqlColumnMetadata>,
    ) -> Result<SqlValue> {
        if data.len() <= *offset {
            return Err(HazelcastError::Protocol("unexpected end of row data".into()));
        }

        // Null check
        let is_null = data[*offset] == 0;
        *offset += 1;

        if is_null {
            return Ok(SqlValue::Null);
        }

        let column_type = column
            .map(|c| c.column_type())
            .unwrap_or(SqlColumnType::Object);

        match column_type {
            SqlColumnType::Varchar | SqlColumnType::Json => {
                if data.len() < *offset + 4 {
                    return Err(HazelcastError::Protocol("missing string length".into()));
                }
                let len = i32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap()) as usize;
                *offset += 4;

                if data.len() < *offset + len {
                    return Err(HazelcastError::Protocol("missing string data".into()));
                }
                let s = String::from_utf8_lossy(&data[*offset..*offset + len]).to_string();
                *offset += len;
                Ok(SqlValue::Varchar(s))
            }
            SqlColumnType::Boolean => {
                let v = data[*offset] != 0;
                *offset += 1;
                Ok(SqlValue::Boolean(v))
            }
            SqlColumnType::TinyInt => {
                let v = data[*offset] as i8;
                *offset += 1;
                Ok(SqlValue::TinyInt(v))
            }
            SqlColumnType::SmallInt => {
                if data.len() < *offset + 2 {
                    return Err(HazelcastError::Protocol("missing smallint data".into()));
                }
                let v = i16::from_le_bytes(data[*offset..*offset + 2].try_into().unwrap());
                *offset += 2;
                Ok(SqlValue::SmallInt(v))
            }
            SqlColumnType::Integer => {
                if data.len() < *offset + 4 {
                    return Err(HazelcastError::Protocol("missing integer data".into()));
                }
                let v = i32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
                *offset += 4;
                Ok(SqlValue::Integer(v))
            }
            SqlColumnType::BigInt => {
                if data.len() < *offset + 8 {
                    return Err(HazelcastError::Protocol("missing bigint data".into()));
                }
                let v = i64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
                *offset += 8;
                Ok(SqlValue::BigInt(v))
            }
            SqlColumnType::Real => {
                if data.len() < *offset + 4 {
                    return Err(HazelcastError::Protocol("missing real data".into()));
                }
                let v = f32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
                *offset += 4;
                Ok(SqlValue::Real(v))
            }
            SqlColumnType::Double => {
                if data.len() < *offset + 8 {
                    return Err(HazelcastError::Protocol("missing double data".into()));
                }
                let v = f64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
                *offset += 8;
                Ok(SqlValue::Double(v))
            }
            SqlColumnType::Decimal => {
                if data.len() < *offset + 4 {
                    return Err(HazelcastError::Protocol("missing decimal scale".into()));
                }
                let scale = i32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
                *offset += 4;

                if data.len() < *offset + 16 {
                    return Err(HazelcastError::Protocol("missing decimal mantissa".into()));
                }
                let mantissa = i128::from_be_bytes(data[*offset..*offset + 16].try_into().unwrap());
                *offset += 16;

                let decimal = Decimal::from_i128_with_scale(mantissa, scale as u32);
                Ok(SqlValue::Decimal(decimal))
            }
            SqlColumnType::Date => {
                if data.len() < *offset + 4 {
                    return Err(HazelcastError::Protocol("missing date data".into()));
                }
                let epoch_days = i32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
                *offset += 4;

                let date = NaiveDate::from_num_days_from_ce_opt(epoch_days + EPOCH_DATE)
                    .ok_or_else(|| HazelcastError::Protocol("invalid date value".into()))?;
                Ok(SqlValue::Date(date))
            }
            SqlColumnType::Time => {
                if data.len() < *offset + 8 {
                    return Err(HazelcastError::Protocol("missing time data".into()));
                }
                let nanos = i64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
                *offset += 8;

                let secs = (nanos / 1_000_000_000) as u32;
                let nano = (nanos % 1_000_000_000) as u32;
                let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
                    .ok_or_else(|| HazelcastError::Protocol("invalid time value".into()))?;
                Ok(SqlValue::Time(time))
            }
            SqlColumnType::Timestamp => {
                if data.len() < *offset + 12 {
                    return Err(HazelcastError::Protocol("missing timestamp data".into()));
                }
                let epoch_secs = i64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
                *offset += 8;
                let nanos = i32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
                *offset += 4;

                let dt = DateTime::from_timestamp(epoch_secs, nanos as u32)
                    .ok_or_else(|| HazelcastError::Protocol("invalid timestamp value".into()))?
                    .naive_utc();
                Ok(SqlValue::Timestamp(dt))
            }
            SqlColumnType::TimestampWithTimeZone => {
                if data.len() < *offset + 16 {
                    return Err(HazelcastError::Protocol(
                        "missing timestamp with timezone data".into(),
                    ));
                }
                let epoch_secs = i64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
                *offset += 8;
                let nanos = i32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
                *offset += 4;
                let offset_secs = i32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
                *offset += 4;

                let fixed_offset = FixedOffset::east_opt(offset_secs)
                    .ok_or_else(|| HazelcastError::Protocol("invalid timezone offset".into()))?;
                let utc_dt = DateTime::from_timestamp(epoch_secs, nanos as u32)
                    .ok_or_else(|| HazelcastError::Protocol("invalid timestamp value".into()))?;
                let dt = utc_dt.with_timezone(&fixed_offset);
                Ok(SqlValue::TimestampWithTimeZone(dt))
            }
            _ => {
                // For other types, read as raw bytes
                if data.len() < *offset + 4 {
                    return Err(HazelcastError::Protocol("missing bytes length".into()));
                }
                let len = i32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap()) as usize;
                *offset += 4;

                if data.len() < *offset + len {
                    return Err(HazelcastError::Protocol("missing bytes data".into()));
                }
                let bytes = data[*offset..*offset + len].to_vec();
                *offset += len;
                Ok(SqlValue::Bytes(bytes))
            }
        }
    }

    fn build_fetch_request(&self, query_id: &SqlQueryId, cursor_buffer_size: i32) -> ClientMessage {
        let mut initial_frame_content = Vec::with_capacity(36);

        // Query ID
        query_id.encode(&mut initial_frame_content);

        // Cursor buffer size
        initial_frame_content.extend_from_slice(&cursor_buffer_size.to_le_bytes());

        let mut request = ClientMessage::new_request(SQL_FETCH, PARTITION_ID_ANY);
        request.add_frame_with_data(initial_frame_content);
        request
    }

    fn build_close_request(&self, query_id: &SqlQueryId) -> ClientMessage {
        let mut initial_frame_content = Vec::with_capacity(32);

        // Query ID
        query_id.encode(&mut initial_frame_content);

        let mut request = ClientMessage::new_request(SQL_CLOSE, PARTITION_ID_ANY);
        request.add_frame_with_data(initial_frame_content);
        request
    }
}

/// Result of a SQL query execution.
///
/// Provides access to row metadata and supports async iteration over result rows.
/// The result must be closed when finished to release server-side resources.
#[derive(Debug)]
pub struct SqlResult {
    connection_manager: Arc<ConnectionManager>,
    query_id: SqlQueryId,
    row_metadata: Option<SqlRowMetadata>,
    rows: VecDeque<SqlRow>,
    cursor_buffer_size: i32,
    is_closed: bool,
    update_count: i64,
}

impl SqlResult {
    /// Returns the row metadata if available.
    ///
    /// Row metadata is present for SELECT queries and describes the columns
    /// in the result set.
    pub fn row_metadata(&self) -> Option<&SqlRowMetadata> {
        self.row_metadata.as_ref()
    }

    /// Returns the update count for DML statements.
    ///
    /// For SELECT queries, this returns -1.
    pub fn update_count(&self) -> i64 {
        self.update_count
    }

    /// Returns true if all rows have been fetched and the cursor is closed.
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    /// Returns the next row from the result set.
    ///
    /// Returns `None` when all rows have been consumed. This method
    /// automatically fetches additional batches from the server as needed.
    pub async fn next_row(&mut self) -> Result<Option<SqlRow>> {
        // Return buffered row if available
        if let Some(row) = self.rows.pop_front() {
            return Ok(Some(row));
        }

        // If closed, no more rows
        if self.is_closed {
            return Ok(None);
        }

        // Fetch next batch
        self.fetch_next_batch().await?;

        // Return first row from new batch
        Ok(self.rows.pop_front())
    }

    /// Fetches the next batch of rows from the server.
    async fn fetch_next_batch(&mut self) -> Result<()> {
        let service = SqlService::new(Arc::clone(&self.connection_manager));
        let request = service.build_fetch_request(&self.query_id, self.cursor_buffer_size);
        let response = self.connection_manager.invoke(request).await?;

        let frames = response.frames();
        if frames.is_empty() {
            self.is_closed = true;
            return Ok(());
        }

        let initial_frame = &frames[0];
        let data = initial_frame.content();

        // Parse is_last flag
        if !data.is_empty() {
            self.is_closed = data[0] != 0;
        }

        // Parse rows
        if frames.len() > 1 {
            let new_rows = service.parse_row_page(&frames, self.row_metadata.as_ref())?;
            self.rows.extend(new_rows);
        }

        Ok(())
    }

    /// Closes the result and releases server-side resources.
    ///
    /// This should be called when you are done with the result, especially
    /// if you did not consume all rows.
    pub async fn close(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }

        let service = SqlService::new(Arc::clone(&self.connection_manager));
        let request = service.build_close_request(&self.query_id);
        let _ = self.connection_manager.invoke(request).await;

        self.is_closed = true;
        self.rows.clear();
        Ok(())
    }

    /// Collects all remaining rows into a vector.
    ///
    /// This consumes the result and automatically closes the cursor.
    pub async fn collect(mut self) -> Result<Vec<SqlRow>> {
        let mut all_rows = Vec::new();

        while let Some(row) = self.next_row().await? {
            all_rows.push(row);
        }

        Ok(all_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_sql_query_id_encode_decode() {
        let id = SqlQueryId::new(1, 2, 3, 4);
        let mut buf = Vec::new();
        id.encode(&mut buf);

        assert_eq!(buf.len(), 32);

        let decoded = SqlQueryId::decode(&buf, 0).unwrap();
        assert_eq!(decoded, id);
    }

    #[test]
    fn test_sql_query_id_generate_unique() {
        let id1 = SqlQueryId::generate();
        let id2 = SqlQueryId::generate();

        assert_ne!(id1.local_id_low, id2.local_id_low);
    }

    #[test]
    fn test_sql_statement_with_service() {
        let stmt = SqlStatement::new("SELECT * FROM users")
            .with_cursor_buffer_size(100)
            .with_timeout(Duration::from_secs(30));

        assert_eq!(stmt.query(), "SELECT * FROM users");
        assert_eq!(stmt.cursor_buffer_size(), Some(100));
    }

    #[test]
    fn test_sql_service_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SqlService>();
    }

    #[test]
    fn test_sql_result_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<SqlResult>();
    }
}
