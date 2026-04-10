# HazelRust: Final Status — 46/50 IMap Tests Pass (92%)

## Working Operations (46/50)

### Core CRUD (14/14) ✅
put, get, remove, delete, set, contains_key, contains_value,
put_if_absent, replace, replace_if_equal, remove_if_equal,
put_returns_old, remove_returns_old, get_nonexistent

### Compute (3/3) ✅
compute, compute_if_absent, compute_if_present

### TTL/Expiration (4/4) ✅
put_with_ttl, set_ttl_expires, set_with_ttl_and_max_idle, put_with_max_idle

### Locking (5/5) ✅
lock_unlock, try_lock, is_locked, force_unlock (+ overwrite_preserves_latest)

### Size/Metadata (3/3) ✅
size, is_empty, clear

### Batch Writes (3/3) ✅
put_all, set_all, put_all_large_batch (via partition-grouped sending)

### Other (14/14) ✅
evict, evict_all, flush, put_transient, try_put,
empty_key_and_value, special_characters, large_values,
integer_keys_values, clone_shares_state, remove_all,
set_fire_forget, replace_if_equal_mismatch, size_empty

## Remaining 4 Failures (Response Decoding)

### 1. get_all — EntryList response decoding
The request is sent correctly (ListMultiFrame encoding with
BEGIN/END frames). The server responds but `decode_entries_response()`
returns 0 entries. The response frame structure likely uses
ListMultiFrame/EntryList encoding that differs from what the
decoder expects (it currently skips BEGIN/END flags and pair-reads
adjacent data frames with 8-byte Data header skip).

### 2. key_set — DistributedIterator returns empty
MAP_FETCH_KEYS (0x013700) iterates partitions correctly.
The response decoding applies Data header skip (8 bytes).
But no keys are returned. The issue may be that the
response uses ListMultiFrame encoding (BEGIN/END) that the
iterator doesn't handle, or the partition table isn't
populated for all 271 partitions.

### 3. entry_set — Same as key_set
MAP_FETCH_ENTRIES (0x013800) has the same issue.

### 4. get_entry_view — Complex response decoding
MAP_GET_ENTRY_VIEW (0x011D00) returns an EntryView with
many typed fields (key, value, cost, creation_time, etc.)
The response decoder needs to handle the multi-field initial
response frame plus the key/value data frames.

## Performance: 20/20 IMap Benchmarks Pass ✅

All performance benchmarks pass with zero errors, including
put_all, lock/unlock, replace, and all value sizes.
