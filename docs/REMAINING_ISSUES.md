# HazelRust Remaining Protocol Issues

## Status After ThreadId Fix

- **23/50 IMap functional tests pass** (46%)
- **20/20 IMap performance benchmarks pass** (100%, zero errors)
- **7/7 MultiMap data ops fixed** (threadId added)

## Working Methods (Confirmed)

These methods work correctly end-to-end:
- `put()`, `get()`, `remove()`, `delete()`, `set()`
- `contains_key()`, `compute()`, `compute_if_absent()`, `compute_if_present()`
- `put_returns_old()`, `remove_returns_old()`
- `get_nonexistent()`, `size_empty()`, `evict_all()`, `flush()`
- `put_with_ttl()`, `set_with_ttl_and_max_idle()`
- `empty_key_and_value()`, `special_characters()`, `large_values()`
- `integer_keys_values()`, `clone_shares_state()`

## Remaining Failures (Deeper Issues)

### Category 1: Response Decoding Mismatches
These methods execute without error but return wrong values:
- `replace()` — returns None instead of old value
- `replace_if_equal()` — CAS match returns false
- `put_if_absent()` — returns None AND doesn't store the value
- `remove_if_equal()` — always returns false

Root cause: The server may interpret the frame layout differently
than expected, causing the response to have a different structure
than `decode_nullable_response()` expects.

### Category 2: Lock Operations
- `lock()`, `try_lock()`, `unlock()`, `is_locked()`, `force_unlock()`
All lock ops fail. These use Pattern B (separate frames with
self.thread_id) which may have incorrect frame ordering.

### Category 3: Collection Operations
- `size()` — returns 0 for non-empty maps
- `is_empty()` — returns true for non-empty maps
- `clear()` — may not actually clear
- `key_set()`, `entry_set()` — return empty (iterator issue)
- `put_all()`, `set_all()`, `get_all()` — batch ops fail

### Category 4: Advanced Operations
- `evict()`, `get_entry_view()`, `put_transient()`
- `put_with_max_idle()`, `contains_value()`

## Recommended Next Steps

1. **Packet capture comparison**: Run the same operations from the
   Java Hazelcast client and the Rust client, compare wire format
2. **Opcode verification**: Cross-reference every opcode constant
   against the Hazelcast 5.x protocol specification
3. **Response frame analysis**: Add debug logging to dump response
   frames for failing methods, compare with expected format
4. **Lock thread_id investigation**: Verify that self.thread_id is
   initialized correctly and matches the server's expectations
