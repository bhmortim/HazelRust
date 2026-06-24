# java_parity server-side infrastructure (CBDC item A-4)

The nine `java_parity_test` cases that previously failed were **not** client
framing/decode bugs — the requests decode cleanly server-side. They failed
because the cluster lacked the server-side artifacts the operations require
(per-entry statistics, a `MapStore`, registered `EntryProcessor` classes) and,
for two cases, because the *test itself* compared/queried string values where
numeric/JSON values were needed. This directory holds the server-side artifacts
+ config so the setup is reproducible; the matching client changes are in the
`fix(cbdc A-4)` commit.

## What each test needed

| Test | Blocker | Fix |
|---|---|---|
| `get_entry_view` | `hits()` is always 0 unless per-entry stats are on | map config `statistics-enabled` + `per-entry-stats-enabled` |
| `load_all`, `load_all_keys` | no `MapStore` configured | `JavaParityMapStore` + `map-store` config on the two load maps |
| `execute_on_key`, `execute_on_keys`, `submit_to_key_async` | processor not reconstructable server-side | `IncrementEntryProcessor` (factoryId 1 / classId 1) + `JavaParityFactory`; client serializes the processor as `IdentifiedDataSerializable` (new `EntryProcessor::factory_id`/`class_id`); also fixed a client decode bug — `decode_entry_processor_results` did not skip the 8-byte Data header, collapsing every key to `""` |
| `execute_on_entries_with_predicate` | `this > 100` over **String** values compares lexically (so "50" matches) | test reworked to i64 values + `IncrementLongEntryProcessor` (classId 2) |
| `entry_set_with_predicate` | same lexical-vs-numeric issue | test reworked to i64 values |
| `project` | projecting `name` over plain String values returns nothing | test stores `HazelcastJsonValue` (type -130, queryable JSON); also fixed a client decode bug — `decode_projection_response` did not skip the 8-byte Data header, so every projected value decoded empty |

`try_lock_with_timeout` is **not** in scope — it is a documented non-defect
(the single-`threadId` client is correctly reentrant; the test assumes Java
multi-threading semantics).

## Cluster config

The dev cluster is 3 Docker members (`hazelcast/hazelcast-enterprise:5.7.0`,
`--network host`, ports 5701/2/3). `start_cluster_a4.sh` (license line redacted
here — it is read at runtime from `~/hz/license.key`) writes a per-member
`hz{1,2,3}.yaml` adding, under `hazelcast:`:

```yaml
  serialization:
    data-serializable-factories:
      - factory-id: 1
        class-name: com.hazelcast.test.JavaParityFactory
  map:
    "java_parity_test_load_all":
      map-store: { enabled: true, class-name: com.hazelcast.test.JavaParityMapStore, initial-mode: LAZY }
    "java_parity_test_load_all_keys":
      map-store: { enabled: true, class-name: com.hazelcast.test.JavaParityMapStore, initial-mode: LAZY }
    "java_parity_test_*":
      statistics-enabled: true
      per-entry-stats-enabled: true
```

and mounts the compiled classes jar onto each member's classpath:

```
-v /home/ec2-user/hz/a4classes.jar:/opt/hazelcast/lib/a4classes.jar
```

## Build + deploy

```sh
# On the instance, EE jars are in ~/hzlib.
CP="$HOME/hzlib/hazelcast-enterprise-5.7.0.jar:$HOME/hzlib/cache-api-1.1.1.jar"
javac -cp "$CP" -d classes src/com/hazelcast/test/*.java
jar cf ~/hz/a4classes.jar -C classes .
bash start_cluster_a4.sh        # regenerates configs + restarts the 3 members
```

Factory/class ids are the contract between client and server: the client's
`IncrementProcessor` declares `factory_id()=1, class_id()=1`;
`LongIncrementProcessor` declares `1/2`; these must equal the server
`JavaParityFactory` mapping.
