# HazelRust Remaining Issues (7/50 IMap Tests)

## Status: 43/50 IMap Functional Tests Pass (86%)

### Fixed in this round:
- Lock operations: lock/try_lock/unlock/is_locked/force_unlock (moved params to initial frame)
- try_put: moved threadId + timeout to initial frame
- put_with_max_idle: new MAP_PUT_WITH_MAX_IDLE opcode (0x014400)

### Still Failing (7 tests):

1. **put_all** — MapPutAllCodec frame encoding mismatch
   - Entries stored via EntryListCodec (BEGIN/END frames) but server doesn't process them
   - May need partition-grouped sending instead of random member

2. **set_all** — Same issue as put_all

3. **put_all_large_batch** — Same issue (larger scale)

4. **get_all** — Response decoding returns empty
   - Request encoding looks correct (BEGIN/END list frames)
   - decode_entries_response may skip valid data frames

5. **key_set_collects_all** — DistributedIterator returns empty
   - Data header skip applied but may be incorrect
   - Partition iteration may miss data

6. **entry_set_collects_all** — Same as key_set

7. **get_entry_view** — Complex EntryView response decoding
   - Response has many typed fields (key, value, cost, creation_time, etc.)
   - Decoder may not handle the multi-field response correctly
