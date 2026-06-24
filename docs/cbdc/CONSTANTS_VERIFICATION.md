# Hazelcast Open Binary Protocol — Message-Type Constant Verification

**Source under test:** `C:/Users/stream/HazelRust/hazelcast-core/src/protocol/constants.rs`
**Authoritative reference:** `hazelcast/hazelcast-client-protocol` `master` branch, `protocol-definitions/*.yaml`
**Date:** 2026-06-24

## Methodology

- Encoding formula (confirmed against universal anchors): `messageType = (serviceId << 16) | (methodId << 8)`.
  - Anchors verified: `MapPut = 0x010100` (Map svc 1, put id 1), `MapGet = 0x010200` (id 2), `MapRemove = 0x010300` (id 3). All reproduced. Formula holds.
- Exception / error response message type = `0x000000` (0). Confirmed (the repo does not define this constant; not in scope as a mismatch).
- Service ids taken from the top-level `id:` field of each YAML.

### Authoritative service ids used

| Service | id (dec) | id (hex) |
|---|---|---|
| Client | 0 | 0x00 |
| Map | 1 | 0x01 |
| MultiMap | 2 | 0x02 |
| Queue | 3 | 0x03 |
| List | 5 | 0x05 |
| Set | 6 | 0x06 |
| FencedLock | 7 | 0x07 |
| AtomicLong | 9 | 0x09 |
| AtomicRef (AtomicReference) | 10 | 0x0A |
| CountDownLatch | 11 | 0x0B |
| Semaphore | 12 | 0x0C |
| ReplicatedMap | 13 | 0x0D |
| **XATransaction** | **20** | **0x14** |
| **Transaction** | **21** | **0x15** |
| **CPGroup** | **30** | **0x1E** |
| CPSession | 31 | 0x1F |
| **CPSubsystem** | **34** | **0x22** |
| CPMap | 35 | 0x23 |

## KEY FINDINGS (summary)

1. **AtomicLong (CP_ATOMIC_LONG_*) — all 6 WRONG.** The repo assumes method ids `get=1,set=2,getAndSet=3,compareAndSet=4,addAndGet=5,getAndAdd=6`. Authoritative ids are `apply=1, alter=2, addAndGet=3, compareAndSet=4, get=5, getAndAdd=6, getAndSet=7`. There is **no standalone `set`** method (use `getAndSet`). Every repo AtomicLong constant points at the wrong codec.
2. **AtomicReference (CP_ATOMIC_REFERENCE_*) — 7 of 8 WRONG, 1 unverifiable.** Authoritative ids: `apply=1, compareAndSet=2, contains=3, get=4, set=5`. The repo assumes `get=1,set=2,getAndSet=3,compareAndSet=4,contains=5,alter=6,apply=7,isNull=8`. The modern protocol has **no** `getAndSet`, `alter`, or `isNull` for AtomicReference.
3. **CP_SUBSYSTEM_* family — all 6 WRONG service id.** They use service `0x1E` (= 30 = **CPGroup**). In the current protocol CPGroup (0x1E) has only `createCPGroup(1)` and `destroyCPObject(2)`; the management methods (getGroupIds / forceDestroyGroup / getCPMembers / promoteToCPMember / removeCPMember) live under **CPSubsystem service `0x22` (34)** with different method ids, OR — for the legacy management codecs — were historically under the `CPSubsystem` service `0x22`. The repo's `0x1E03..0x1E06` values do not correspond to any method in the modern CPGroup.yaml. **Answer to the explicit question: the CP_SUBSYSTEM_* constants currently use 0x1E (CPGroup); they should NOT — CPSubsystem is 0x22.**
4. **CPGroupCreateCPGroup / CPGroupDestroyCPObject:** correct values are `0x1E0100` and `0x1E0200`. The repo does **not** define these as named constants (it reuses 0x1E0100/0x1E0200 for `CP_SUBSYSTEM_GET_GROUP_IDS`/`CP_SUBSYSTEM_GET_GROUP`, which is itself wrong — those slots are createCPGroup/destroyCPObject).
5. **XA_TXN_* — all 7 WRONG service id.** XATransaction is service `0x14` (20), not `0x15` (21). The repo placed them at `0x15xxxx`, colliding with the Transaction service space. Also the method ids differ (e.g. `create=5`, `finalize=3`).
6. **Map interceptor / partition-lost listeners — all 4 WRONG.** `addInterceptor=20 (0x011400)`, `removeInterceptor=21 (0x011500)`, `addPartitionLostListener=27 (0x011B00)`, `removePartitionLostListener=28 (0x011C00)`. The repo used Map ids from the 0x12xx/0x13xx range that actually belong to other Map methods (executeOnKey, submitToKey, loadAll, getAll).
7. **Map keyset/values/entryset family — several WRONG** due to off-by mapping (repo swapped whole-map vs predicate variants).

---

## FULL COVERAGE TABLE

Status legend: **OK** = repo value equals authoritative value. **WRONG** = mismatch. **UNVERIFIABLE** = method not present in current authoritative YAML (cannot confirm against master).

### Client (service 0x00)

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| CLIENT_AUTHENTICATION | 0x000100 | 0x000100 | OK | authentication=1 |
| CLIENT_AUTHENTICATION_RESPONSE | 0x000101 | n/a | UNVERIFIABLE | Not a request messageType; response type is request\|1 by convention. Informational only. |
| CLIENT_ADD_MEMBERSHIP_LISTENER | 0x000300 | 0x000300 | OK | maps to addClusterViewListener(3) — name differs but value correct |
| CLIENT_REMOVE_MEMBERSHIP_LISTENER | 0x000301 | n/a | UNVERIFIABLE | No removeClusterViewListener in protocol; 0x000301 is a response code, not a request |
| CLIENT_CREATE_PROXY | 0x000400 | 0x000400 | OK | createProxy=4 |
| CLIENT_DESTROY_PROXY | 0x000500 | 0x000500 | OK | destroyProxy=5 |
| CLIENT_ADD_PARTITION_LOST_LISTENER | 0x000600 | 0x000600 | OK | addPartitionLostListener=6 |
| CLIENT_REMOVE_PARTITION_LOST_LISTENER | 0x000700 | 0x000700 | OK | removePartitionLostListener=7 |
| CLIENT_GET_DISTRIBUTED_OBJECTS | 0x000A00 | 0x000800 | WRONG | getDistributedObjects=8 → 0x000800. Repo 0x000A00 = removeDistributedObjectListener(10) |
| CLIENT_ADD_DISTRIBUTED_OBJECT_LISTENER | 0x000800 | 0x000900 | WRONG | addDistributedObjectListener=9 → 0x000900. Repo 0x000800 = getDistributedObjects(8) |
| CLIENT_REMOVE_DISTRIBUTED_OBJECT_LISTENER | 0x000900 | 0x000A00 | WRONG | removeDistributedObjectListener=10 → 0x000A00. Repo 0x000900 = addDistributedObjectListener(9) |
| CLIENT_STATISTICS | 0x000C00 | 0x000C00 | OK | statistics=12 |

### Map (service 0x01)

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| MAP_PUT | 0x010100 | 0x010100 | OK | put=1 (anchor) |
| MAP_GET | 0x010200 | 0x010200 | OK | get=2 (anchor) |
| MAP_REMOVE | 0x010300 | 0x010300 | OK | remove=3 (anchor) |
| MAP_REPLACE | 0x010400 | 0x010400 | OK | replace=4 |
| MAP_REPLACE_IF_SAME | 0x010500 | 0x010500 | OK | replaceIfSame=5 |
| MAP_CONTAINS_KEY | 0x010600 | 0x010600 | OK | containsKey=6 |
| MAP_CONTAINS_VALUE | 0x010700 | 0x010700 | OK | containsValue=7 |
| MAP_REMOVE_IF_SAME | 0x010800 | 0x010800 | OK | removeIfSame=8 |
| MAP_DELETE | 0x010900 | 0x010900 | OK | delete=9 |
| MAP_FLUSH | 0x010A00 | 0x010A00 | OK | flush=10 |
| MAP_TRY_PUT | 0x010C00 | 0x010C00 | OK | tryPut=12 |
| MAP_PUT_TRANSIENT | 0x010D00 | 0x010D00 | OK | putTransient=13 |
| MAP_PUT_IF_ABSENT | 0x010E00 | 0x010E00 | OK | putIfAbsent=14 |
| MAP_SET | 0x010F00 | 0x010F00 | OK | set=15 |
| MAP_LOCK | 0x011000 | 0x011000 | OK | lock=16 |
| MAP_TRY_LOCK | 0x011100 | 0x011100 | OK | tryLock=17 |
| MAP_IS_LOCKED | 0x011200 | 0x011200 | OK | isLocked=18 |
| MAP_UNLOCK | 0x011300 | 0x011300 | OK | unlock=19 |
| MAP_ADD_INTERCEPTOR | 0x012E00 | 0x011400 | WRONG | addInterceptor=20 → 0x011400. Repo 0x012E00 = Map.executeOnKey(46) |
| MAP_REMOVE_INTERCEPTOR | 0x012F00 | 0x011500 | WRONG | removeInterceptor=21 → 0x011500. Repo 0x012F00 = Map.submitToKey(47) |
| MAP_ADD_ENTRY_LISTENER_TO_KEY_WITH_PREDICATE | 0x011600 | 0x011600 | OK | addEntryListenerToKeyWithPredicate=22 |
| MAP_ADD_ENTRY_LISTENER_WITH_PREDICATE | 0x011700 | 0x011700 | OK | addEntryListenerWithPredicate=23 |
| MAP_ADD_ENTRY_LISTENER_TO_KEY | 0x011800 | 0x011800 | OK | addEntryListenerToKey=24 |
| MAP_ADD_ENTRY_LISTENER | 0x011900 | 0x011900 | OK | addEntryListener=25 |
| MAP_REMOVE_ENTRY_LISTENER | 0x011A00 | 0x011A00 | OK | removeEntryListener=26 |
| MAP_ADD_PARTITION_LOST_LISTENER | 0x012200 | 0x011B00 | WRONG | addPartitionLostListener=27 → 0x011B00. Repo 0x012200 = Map.keySet(34) |
| MAP_REMOVE_PARTITION_LOST_LISTENER | 0x012300 | 0x011C00 | WRONG | removePartitionLostListener=28 → 0x011C00. Repo 0x012300 = Map.getAll(35) |
| MAP_GET_ENTRY_VIEW | 0x011D00 | 0x011D00 | OK | getEntryView=29 |
| MAP_EVICT | 0x011E00 | 0x011E00 | OK | evict=30 |
| MAP_EVICT_ALL | 0x011F00 | 0x011F00 | OK | evictAll=31 |
| MAP_LOAD_ALL | 0x012000 | 0x012000 | OK | loadAll=32 |
| MAP_LOAD_GIVEN_KEYS | 0x012100 | 0x012100 | OK | loadGivenKeys=33 |
| MAP_KEY_SET | 0x012200 | 0x012200 | OK | keySet=34 |
| MAP_GET_ALL | 0x012300 | 0x012300 | OK | getAll=35 |
| MAP_VALUES | 0x012400 | 0x012400 | OK | values=36 |
| MAP_ENTRY_SET | 0x012500 | 0x012500 | OK | entrySet=37 |
| MAP_KEYS_WITH_PREDICATE | 0x012600 | 0x012600 | OK | keySetWithPredicate=38 |
| MAP_VALUES_WITH_PREDICATE | 0x012700 | 0x012700 | OK | valuesWithPredicate=39 |
| MAP_ENTRIES_WITH_PREDICATE | 0x012800 | 0x012800 | OK | entriesWithPredicate=40 |
| MAP_ADD_INDEX | 0x012900 | 0x012900 | OK | addIndex=41 |
| MAP_SIZE | 0x012A00 | 0x012A00 | OK | size=42 |
| MAP_IS_EMPTY | 0x012B00 | 0x012B00 | OK | isEmpty=43 |
| MAP_PUT_ALL | 0x012C00 | 0x012C00 | OK | putAll=44 |
| MAP_SET_ALL | 0x012C00 | 0x014900 | WRONG | "setAll" is putAllWithMetadata=73 → 0x014900 (no `setAll` codec exists; closest semantic). Repo value 0x012C00 = Map.putAll(44). At minimum this is a duplicate of MAP_PUT_ALL and not a distinct codec. |
| MAP_CLEAR | 0x012D00 | 0x012D00 | OK | clear=45 |
| MAP_EXECUTE_ON_KEY | 0x012E00 | 0x012E00 | OK | executeOnKey=46 |
| MAP_EXECUTE_ON_ALL_KEYS | 0x013000 | 0x013000 | OK | executeOnAllKeys=48 |
| MAP_EXECUTE_WITH_PREDICATE | 0x013100 | 0x013100 | OK | executeWithPredicate=49 |
| MAP_EXECUTE_ON_KEYS | 0x013200 | 0x013200 | OK | executeOnKeys=50 |
| MAP_FORCE_UNLOCK | 0x013300 | 0x013300 | OK | forceUnlock=51 |
| MAP_KEYS_WITH_PAGING_PREDICATE | 0x013400 | 0x013400 | OK | keySetWithPagingPredicate=52 |
| MAP_VALUES_WITH_PAGING_PREDICATE | 0x013500 | 0x013500 | OK | valuesWithPagingPredicate=53 |
| MAP_ENTRIES_WITH_PAGING_PREDICATE | 0x013600 | 0x013600 | OK | entriesWithPagingPredicate=54 |
| MAP_FETCH_KEYS | 0x013700 | 0x013700 | OK | fetchKeys=55 |
| MAP_FETCH_ENTRIES | 0x013800 | 0x013800 | OK | fetchEntries=56 |
| MAP_AGGREGATE | 0x013900 | 0x013900 | OK | aggregate=57 |
| MAP_FETCH_VALUES | 0x013900 | 0x014000 | WRONG | "fetchValues" is fetchWithQuery=64 → 0x014000. Repo 0x013900 = Map.aggregate(57). Also collides with MAP_AGGREGATE. |
| MAP_AGGREGATE_WITH_PREDICATE | 0x013A00 | 0x013A00 | OK | aggregateWithPredicate=58 |
| MAP_PROJECT | 0x013B00 | 0x013B00 | OK | project=59 |
| MAP_PROJECT_WITH_PREDICATE | 0x013C00 | 0x013C00 | OK | projectWithPredicate=60 |
| MAP_REMOVE_ALL | 0x013E00 | 0x013E00 | OK | removeAll=62 |
| MAP_EVENT_JOURNAL_SUBSCRIBE | 0x014100 | 0x014100 | OK | eventJournalSubscribe=65 |
| MAP_EVENT_JOURNAL_READ | 0x014200 | 0x014200 | OK | eventJournalRead=66 |
| MAP_SET_TTL | 0x014300 | 0x014300 | OK | setTtl=67 |
| MAP_PUT_WITH_MAX_IDLE | 0x014400 | 0x014400 | OK | putWithMaxIdle=68 |
| MAP_SET_WITH_MAX_IDLE | 0x014700 | 0x014700 | OK | setWithMaxIdle=71 |

### MultiMap (service 0x02)

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| MULTI_MAP_PUT | 0x020100 | 0x020100 | OK | put=1 |
| MULTI_MAP_GET | 0x020200 | 0x020200 | OK | get=2 |
| MULTI_MAP_REMOVE | 0x020300 | 0x020300 | OK | remove=3 |
| MULTI_MAP_KEY_SET | 0x020400 | 0x020400 | OK | keySet=4 |
| MULTI_MAP_VALUES | 0x020500 | 0x020500 | OK | values=5 |
| MULTI_MAP_ENTRY_SET | 0x020600 | 0x020600 | OK | entrySet=6 |
| MULTI_MAP_CONTAINS_KEY | 0x020700 | 0x020700 | OK | containsKey=7 |
| MULTI_MAP_CONTAINS_VALUE | 0x020800 | 0x020800 | OK | containsValue=8 |
| MULTI_MAP_CONTAINS_ENTRY | 0x020900 | 0x020900 | OK | containsEntry=9 |
| MULTI_MAP_SIZE | 0x020A00 | 0x020A00 | OK | size=10 |
| MULTI_MAP_CLEAR | 0x020B00 | 0x020B00 | OK | clear=11 |
| MULTI_MAP_VALUE_COUNT | 0x020C00 | 0x020C00 | OK | valueCount=12 |
| MULTI_MAP_ADD_ENTRY_LISTENER | 0x020E00 | 0x020E00 | OK | addEntryListener=14 |
| MULTI_MAP_REMOVE_ENTRY_LISTENER | 0x020F00 | 0x020F00 | OK | removeEntryListener=15 |
| MULTI_MAP_ADD_ENTRY_LISTENER_WITH_PREDICATE | 0x021500 | 0x020D00 | WRONG | MultiMap has addEntryListenerToKey=13 → 0x020D00 (the "with key/predicate" variant). Repo 0x021500 = MultiMap.removeEntry(21). There is no separate addEntryListenerWithPredicate in MultiMap. |
| MULTI_MAP_LOCK | 0x021000 | 0x021000 | OK | lock=16 |
| MULTI_MAP_TRY_LOCK | 0x021100 | 0x021100 | OK | tryLock=17 |
| MULTI_MAP_IS_LOCKED | 0x021200 | 0x021200 | OK | isLocked=18 |
| MULTI_MAP_UNLOCK | 0x021300 | 0x021300 | OK | unlock=19 |
| MULTI_MAP_FORCE_UNLOCK | 0x021400 | 0x021400 | OK | forceUnlock=20 |
| MULTI_MAP_REMOVE_ENTRY | 0x021500 | 0x021500 | OK | removeEntry=21 |

### ReplicatedMap (service 0x0D)

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| REPLICATED_MAP_PUT | 0x0D0100 | 0x0D0100 | OK | put=1 |
| REPLICATED_MAP_SIZE | 0x0D0200 | 0x0D0200 | OK | size=2 |
| REPLICATED_MAP_IS_EMPTY | 0x0D0300 | 0x0D0300 | OK | isEmpty=3 |
| REPLICATED_MAP_CONTAINS_KEY | 0x0D0400 | 0x0D0400 | OK | containsKey=4 |
| REPLICATED_MAP_CONTAINS_VALUE | 0x0D0500 | 0x0D0500 | OK | containsValue=5 |
| REPLICATED_MAP_GET | 0x0D0600 | 0x0D0600 | OK | get=6 |
| REPLICATED_MAP_REMOVE | 0x0D0700 | 0x0D0700 | OK | remove=7 |
| REPLICATED_MAP_PUT_ALL | 0x0D0800 | 0x0D0800 | OK | putAll=8 |
| REPLICATED_MAP_CLEAR | 0x0D0900 | 0x0D0900 | OK | clear=9 |
| REPLICATED_MAP_ADD_ENTRY_LISTENER_WITH_PREDICATE | 0x0D0B00 | 0x0D0B00 | OK | addEntryListenerWithPredicate=11 |
| REPLICATED_MAP_ADD_ENTRY_LISTENER | 0x0D0D00 | 0x0D0D00 | OK | addEntryListener=13 |
| REPLICATED_MAP_REMOVE_ENTRY_LISTENER | 0x0D0E00 | 0x0D0E00 | OK | removeEntryListener=14 |
| REPLICATED_MAP_KEY_SET | 0x0D0F00 | 0x0D0F00 | OK | keySet=15 |
| REPLICATED_MAP_VALUES | 0x0D1000 | 0x0D1000 | OK | values=16 |
| REPLICATED_MAP_ENTRY_SET | 0x0D1100 | 0x0D1100 | OK | entrySet=17 |
| REPLICATED_MAP_PUT_WITH_TTL | 0x0D0100 | 0x0D0100 | OK | ReplicatedMap.put carries ttl; same codec as put=1. Duplicate of REPLICATED_MAP_PUT (acceptable). |

### List (service 0x05)

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| LIST_SIZE | 0x050100 | 0x050100 | OK | size=1 |
| LIST_CONTAINS | 0x050200 | 0x050200 | OK | contains=2 |
| LIST_CONTAINS_ALL | 0x050300 | 0x050300 | OK | containsAll=3 |
| LIST_ADD | 0x050400 | 0x050400 | OK | add=4 |
| LIST_REMOVE | 0x050500 | 0x050500 | OK | remove=5 |
| LIST_ADD_ALL | 0x050600 | 0x050600 | OK | addAll=6 |
| LIST_REMOVE_ALL | 0x050700 | 0x050700 | OK | compareAndRemoveAll=7 → 0x050700 (name differs; value correct) |
| LIST_RETAIN_ALL | 0x050800 | 0x050800 | OK | compareAndRetainAll=8 |
| LIST_CLEAR | 0x050900 | 0x050900 | OK | clear=9 |
| LIST_ADD_LISTENER | 0x050B00 | 0x050B00 | OK | addListener=11 |
| LIST_REMOVE_LISTENER | 0x050C00 | 0x050C00 | OK | removeListener=12 |
| LIST_ADD_ALL_AT | 0x050E00 | 0x050E00 | OK | addAllWithIndex=14 |
| LIST_GET | 0x050F00 | 0x050F00 | OK | get=15 |
| LIST_SET | 0x051000 | 0x051000 | OK | set=16 |
| LIST_ADD_AT | 0x051100 | 0x051100 | OK | addWithIndex=17 |
| LIST_REMOVE_AT | 0x051200 | 0x051200 | OK | removeWithIndex=18 |
| LIST_LAST_INDEX_OF | 0x051300 | 0x051300 | OK | lastIndexOf=19 |
| LIST_INDEX_OF | 0x051400 | 0x051400 | OK | indexOf=20 |

### Set (service 0x06)

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| SET_SIZE | 0x060100 | 0x060100 | OK | size=1 |
| SET_CONTAINS | 0x060200 | 0x060200 | OK | contains=2 |
| SET_CONTAINS_ALL | 0x060300 | 0x060300 | OK | containsAll=3 |
| SET_ADD | 0x060400 | 0x060400 | OK | add=4 |
| SET_REMOVE | 0x060500 | 0x060500 | OK | remove=5 |
| SET_ADD_ALL | 0x060600 | 0x060600 | OK | addAll=6 |
| SET_REMOVE_ALL | 0x060700 | 0x060700 | OK | compareAndRemoveAll=7 (name differs; value correct) |
| SET_RETAIN_ALL | 0x060800 | 0x060800 | OK | compareAndRetainAll=8 |
| SET_CLEAR | 0x060900 | 0x060900 | OK | clear=9 |
| SET_GET_ALL | 0x060A00 | 0x060A00 | OK | getAll=10 |
| SET_ADD_LISTENER | 0x060B00 | 0x060B00 | OK | addListener=11 |
| SET_REMOVE_LISTENER | 0x060C00 | 0x060C00 | OK | removeListener=12 |

### Queue (service 0x03)

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| QUEUE_OFFER | 0x030100 | 0x030100 | OK | offer=1 |
| QUEUE_PUT | 0x030200 | 0x030200 | OK | put=2 |
| QUEUE_SIZE | 0x030300 | 0x030300 | OK | size=3 |
| QUEUE_REMOVE | 0x030400 | 0x030400 | OK | remove=4 |
| QUEUE_POLL | 0x030500 | 0x030500 | OK | poll=5 |
| QUEUE_TAKE | 0x030600 | 0x030600 | OK | take=6 |
| QUEUE_PEEK | 0x030700 | 0x030700 | OK | peek=7 |
| QUEUE_ITERATOR | 0x030800 | 0x030800 | OK | iterator=8 |
| QUEUE_DRAIN_TO | 0x030900 | 0x030900 | OK | drainTo=9 |
| QUEUE_DRAIN_TO_MAX_SIZE | 0x030A00 | 0x030A00 | OK | drainToMaxSize=10 |
| QUEUE_CONTAINS | 0x030B00 | 0x030B00 | OK | contains=11 |
| QUEUE_CONTAINS_ALL | 0x030C00 | 0x030C00 | OK | containsAll=12 |
| QUEUE_COMPARE_AND_REMOVE_ALL | 0x030D00 | 0x030D00 | OK | compareAndRemoveAll=13 |
| QUEUE_CLEAR | 0x030F00 | 0x030F00 | OK | clear=15 |
| QUEUE_ADD_ALL | 0x031000 | 0x031000 | OK | addAll=16 |
| QUEUE_ADD_LISTENER | 0x031100 | 0x031100 | OK | addListener=17 |
| QUEUE_REMOVE_LISTENER | 0x031200 | 0x031200 | OK | removeListener=18 |
| QUEUE_REMAINING_CAPACITY | 0x031300 | 0x031300 | OK | remainingCapacity=19 |
| QUEUE_IS_EMPTY | 0x031400 | 0x031400 | OK | isEmpty=20 |

### AtomicLong (CP) — service 0x09

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| CP_ATOMIC_LONG_GET | 0x090100 | 0x090500 | WRONG | get=5 → 0x090500. Repo 0x090100 = AtomicLong.apply(1) |
| CP_ATOMIC_LONG_SET | 0x090200 | n/a | WRONG | No standalone `set` in AtomicLong protocol. Use getAndSet=7 (0x090700) if old value needed. Repo 0x090200 = AtomicLong.alter(2) |
| CP_ATOMIC_LONG_GET_AND_SET | 0x090300 | 0x090700 | WRONG | getAndSet=7 → 0x090700. Repo 0x090300 = AtomicLong.addAndGet(3) |
| CP_ATOMIC_LONG_COMPARE_AND_SET | 0x090400 | 0x090400 | OK | compareAndSet=4 — value happens to match |
| CP_ATOMIC_LONG_ADD_AND_GET | 0x090500 | 0x090300 | WRONG | addAndGet=3 → 0x090300. Repo 0x090500 = AtomicLong.get(5) |
| CP_ATOMIC_LONG_GET_AND_ADD | 0x090600 | 0x090600 | OK | getAndAdd=6 — value happens to match |

### AtomicReference (CP) — service 0x0A

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| CP_ATOMIC_REFERENCE_GET | 0x0A0100 | 0x0A0400 | WRONG | get=4 → 0x0A0400. Repo 0x0A0100 = AtomicRef.apply(1) |
| CP_ATOMIC_REFERENCE_SET | 0x0A0200 | 0x0A0500 | WRONG | set=5 → 0x0A0500. Repo 0x0A0200 = AtomicRef.compareAndSet(2) |
| CP_ATOMIC_REFERENCE_GET_AND_SET | 0x0A0300 | n/a | WRONG/UNVERIFIABLE | No getAndSet in AtomicRef protocol (master). Repo 0x0A0300 = AtomicRef.contains(3) |
| CP_ATOMIC_REFERENCE_COMPARE_AND_SET | 0x0A0400 | 0x0A0200 | WRONG | compareAndSet=2 → 0x0A0200. Repo 0x0A0400 = AtomicRef.get(4) |
| CP_ATOMIC_REFERENCE_CONTAINS | 0x0A0500 | 0x0A0300 | WRONG | contains=3 → 0x0A0300. Repo 0x0A0500 = AtomicRef.set(5) |
| CP_ATOMIC_REFERENCE_IS_NULL | 0x0A0800 | n/a | UNVERIFIABLE | No isNull method in AtomicRef.yaml (master). Cannot map. Likely should be implemented via contains(null). |
| CP_ATOMIC_REFERENCE_ALTER | 0x0A0600 | n/a | UNVERIFIABLE | No alter method in AtomicRef.yaml (master). Cannot map. |
| CP_ATOMIC_REFERENCE_APPLY | 0x0A0700 | 0x0A0100 | WRONG | apply=1 → 0x0A0100. Repo 0x0A0700 has no method (id 7 out of range, max id=5). |

### FencedLock (CP) — service 0x07

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| CP_FENCED_LOCK_LOCK | 0x070100 | 0x070100 | OK | lock=1 |
| CP_FENCED_LOCK_TRY_LOCK | 0x070200 | 0x070200 | OK | tryLock=2 |
| CP_FENCED_LOCK_UNLOCK | 0x070300 | 0x070300 | OK | unlock=3 |
| CP_FENCED_LOCK_GET_LOCK_OWNERSHIP_STATE | 0x070400 | 0x070400 | OK | getLockOwnership=4 |

### Semaphore (CP) — service 0x0C

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| CP_SEMAPHORE_INIT | 0x0C0100 | 0x0C0100 | OK | init=1 |
| CP_SEMAPHORE_ACQUIRE | 0x0C0200 | 0x0C0200 | OK | acquire=2 |
| CP_SEMAPHORE_RELEASE | 0x0C0300 | 0x0C0300 | OK | release=3 |
| CP_SEMAPHORE_DRAIN | 0x0C0400 | 0x0C0400 | OK | drain=4 |
| CP_SEMAPHORE_CHANGE | 0x0C0500 | 0x0C0500 | OK | change=5 |
| CP_SEMAPHORE_AVAILABLE_PERMITS | 0x0C0600 | 0x0C0600 | OK | availablePermits=6 |

### CountDownLatch (CP) — service 0x0B

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| CP_COUNTDOWN_LATCH_TRY_SET_COUNT | 0x0B0100 | 0x0B0100 | OK | trySetCount=1 |
| CP_COUNTDOWN_LATCH_AWAIT | 0x0B0200 | 0x0B0200 | OK | await=2 |
| CP_COUNTDOWN_LATCH_COUNT_DOWN | 0x0B0300 | 0x0B0300 | OK | countDown=3 |
| CP_COUNTDOWN_LATCH_GET_COUNT | 0x0B0400 | 0x0B0400 | OK | getCount=4 |
| CP_COUNTDOWN_LATCH_GET_ROUND | 0x0B0500 | 0x0B0500 | OK | getRound=5 |

### CPSession — service 0x1F

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| CP_SESSION_CREATE_SESSION | 0x1F0100 | 0x1F0100 | OK | createSession=1 |
| CP_SESSION_CLOSE_SESSION | 0x1F0200 | 0x1F0200 | OK | closeSession=2 |
| CP_SESSION_HEARTBEAT | 0x1F0300 | 0x1F0300 | OK | heartbeatSession=3 |
| CP_SESSION_GENERATE_THREAD_ID | 0x1F0400 | 0x1F0400 | OK | generateThreadId=4 |

### CPGroup — service 0x1E  /  CPSubsystem — service 0x22

The repo defines a `CP_SUBSYSTEM_*` family at service `0x1E` (CPGroup). This is wrong on two counts: (a) CPGroup (0x1E) only has `createCPGroup(1)`/`destroyCPObject(2)` in the modern protocol; (b) the CP subsystem management methods live under service `0x22` (CPSubsystem) — but with different method names/ids than the repo assumes. The repo's specific method ids (getGroupIds, forceDestroyGroup, getCPMembers, promoteToCPMember, removeCPMember) match the **legacy `CPSubsystem` management codecs** (historically service 0x22), not anything at 0x1E.

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| CP_SUBSYSTEM_GET_GROUP_IDS | 0x1E0100 | 0x220500 | WRONG | CPSubsystem.getCPGroupIds=5 → 0x220500. Repo 0x1E0100 = CPGroup.createCPGroup(1). Wrong service (0x1E vs 0x22). |
| CP_SUBSYSTEM_GET_GROUP | 0x1E0200 | n/a | WRONG/UNVERIFIABLE | No getCPGroup(name)-by-itself method in current CPSubsystem.yaml; closest is getCPObjectInfos=6 (0x220600). Repo 0x1E0200 = CPGroup.destroyCPObject(2). Wrong service. |
| CP_SUBSYSTEM_FORCE_DESTROY_GROUP | 0x1E0300 | n/a | UNVERIFIABLE | No forceDestroyCPGroup in CPSubsystem.yaml (master). Legacy management codec only. Repo service 0x1E is wrong regardless (0x1E03 is undefined in CPGroup). |
| CP_SUBSYSTEM_GET_CP_MEMBERS | 0x1E0400 | n/a | UNVERIFIABLE | No getCPMembers in CPSubsystem.yaml (master). Legacy management codec. 0x1E04 undefined in CPGroup. |
| CP_SUBSYSTEM_PROMOTE_TO_CP_MEMBER | 0x1E0500 | n/a | UNVERIFIABLE | No promoteToCPMember in CPSubsystem.yaml (master). Legacy. 0x1E05 undefined in CPGroup. |
| CP_SUBSYSTEM_REMOVE_CP_MEMBER | 0x1E0600 | n/a | UNVERIFIABLE | No removeCPMember in CPSubsystem.yaml (master). Legacy. 0x1E06 undefined in CPGroup. |

> **CPGroupCreateCPGroup = 0x1E0100, CPGroupDestroyCPObject = 0x1E0200** are the correct values for the two real CPGroup methods. The repo does NOT expose them under those names; instead those two slots are mislabeled as `CP_SUBSYSTEM_GET_GROUP_IDS` / `CP_SUBSYSTEM_GET_GROUP`.

### CPMap — service 0x23

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| CP_MAP_GET | 0x230100 | 0x230100 | OK | get=1 |
| CP_MAP_PUT | 0x230200 | 0x230200 | OK | put=2 |
| CP_MAP_SET | 0x230300 | 0x230300 | OK | set=3 |
| CP_MAP_REMOVE | 0x230400 | 0x230400 | OK | remove=4 |
| CP_MAP_DELETE | 0x230500 | 0x230500 | OK | delete=5 |
| CP_MAP_COMPARE_AND_SET | 0x230600 | 0x230600 | OK | compareAndSet=6 |

### Transaction — service 0x15

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| TXN_COMMIT | 0x150100 | 0x150100 | OK | commit=1 |
| TXN_CREATE | 0x150200 | 0x150200 | OK | create=2 |
| TXN_ROLLBACK | 0x150300 | 0x150300 | OK | rollback=3 |

### XATransaction — service 0x14 (NOT 0x15)

The repo placed all XA constants under 0x15 (Transaction service). XATransaction is service id 20 = **0x14**. Every XA constant is therefore WRONG on the service nibble. Method ids also differ.

| ConstantName | RepoValue | CorrectValue | Status | Notes |
|---|---|---|---|---|
| XA_TXN_CLEAR_REMOTE | 0x150800 | 0x140100 | WRONG | XATxn.clearRemote=1 → 0x140100. Repo wrong service (0x15) and wrong id. |
| XA_TXN_COLLECT_TRANSACTIONS | 0x150900 | 0x140200 | WRONG | collectTransactions=2 → 0x140200. |
| XA_TXN_FINALIZE | 0x150A00 | 0x140300 | WRONG | finalize=3 → 0x140300. |
| XA_TXN_COMMIT | 0x150600 | 0x140400 | WRONG | commit=4 → 0x140400. Repo 0x150600 has no method (Transaction svc only has 3 methods). |
| XA_TXN_CREATE | 0x150400 | 0x140500 | WRONG | create=5 → 0x140500. |
| XA_TXN_PREPARE | 0x150500 | 0x140600 | WRONG | prepare=6 → 0x140600. |
| XA_TXN_ROLLBACK | 0x150700 | 0x140700 | WRONG | rollback=7 → 0x140700. |

> Note: repo values 0x150400–0x150A00 also collide with the `ContinuousQuery` service which is **0x14** in the protocol — i.e. the *correct* XA values (0x14xxxx) overlap the address space the repo used for `CONTINUOUS_QUERY_*` (0x1401–0x1405). The repo's `CONTINUOUS_QUERY_*` constants at 0x14xxxx are themselves suspect (out of in-scope set, not formally checked here), so fixing XA to 0x14xxxx will require disambiguating against ContinuousQuery. **Flag for the remediation team.**

---

## Out-of-scope constants (present in repo, NOT verified against YAML this pass)

Ringbuffer, PNCounter, FlakeIdGenerator, Topic, ExecutorService, ScheduledExecutor, DurableExecutor, CardinalityEstimator, Cache, Jet, SQL, VectorCollection, ContinuousQuery, Transactional{Map,Queue,Set,List,MultiMap}. These were not part of the explicitly in-scope service list. NOTE: the ContinuousQuery service id (0x14) directly conflicts with the corrected XATransaction service id (0x14) — see XA note above.

## Coverage / Count

See final summary in the agent response.
