package bench;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Operation dispatch + seeding — mirror of hazelrust-bench/src/ops.rs. Split
 * into a {@link Planner} (advances the deterministic key-stream) and
 * {@link Handles} (executes a resolved {@link Plan} against shared proxies).
 */
public final class Ops {

    public static String imapName(int valueSize, long ws) { return "bench-imap-v" + valueSize + "-ws" + ws; }
    public static String cpmapName(long ws) { return "bench-cpmap-ws" + ws; }
    public static String alName(int worker) { return "bench-al-" + worker; }
    public static String arefName(int worker) { return "bench-aref-" + worker; }
    public static String queueName(int valueSize) { return "bench-queue-v" + valueSize; }
    public static String setName(int valueSize) { return "bench-set-v" + valueSize; }
    public static String repmapName(int valueSize, long ws) { return "bench-repmap-v" + valueSize + "-ws" + ws; }

    public static IMap<Long, byte[]> ensureImapSeeded(HazelcastInstance c, int valueSize, long ws, long seed) {
        IMap<Long, byte[]> map = c.getMap(imapName(valueSize, ws));
        if (map.size() >= ws) return map;
        long batch = 1000;
        for (long i = 0; i < ws; ) {
            long end = Math.min(i + batch, ws);
            Map<Long, byte[]> entries = new HashMap<>();
            for (long idx = i; idx < end; idx++) {
                entries.put(Data.keyI64(idx), Data.valueBytes(idx, valueSize, seed));
            }
            map.putAll(entries);
            i = end;
        }
        return map;
    }

    public static CPMap<Long, Long> ensureCpmapSeeded(HazelcastInstance c, long ws) {
        CPMap<Long, Long> cpmap = c.getCPSubsystem().getMap(cpmapName(ws));
        if (cpmap.get(Data.keyI64(ws - 1)) == null) {
            for (long idx = 0; idx < ws; idx++) cpmap.set(Data.keyI64(idx), idx);
        }
        return cpmap;
    }

    public static ReplicatedMap<Long, byte[]> ensureRepmapSeeded(HazelcastInstance c, int valueSize, long ws, long seed) {
        ReplicatedMap<Long, byte[]> rep = c.getReplicatedMap(repmapName(valueSize, ws));
        if (rep.size() >= ws) return rep;
        long batch = 1000;
        for (long i = 0; i < ws; ) {
            long end = Math.min(i + batch, ws);
            Map<Long, byte[]> entries = new HashMap<>();
            for (long idx = i; idx < end; idx++) {
                entries.put(Data.keyI64(idx), Data.valueBytes(idx, valueSize, seed));
            }
            rep.putAll(entries);
            i = end;
        }
        return rep;
    }

    public static void ensureSeeded(HazelcastInstance c, Manifest.Cell cell, long seed) {
        switch (cell.structure) {
            case "imap":
                if (!(cell.op.equals("get") && cell.variant.equals("miss"))) {
                    ensureImapSeeded(c, cell.value_size, Math.max(1, cell.working_set), seed);
                }
                break;
            case "cpmap":
                if (!(cell.op.equals("put") && cell.variant.equals("insert"))) {
                    ensureCpmapSeeded(c, Math.max(1, cell.working_set));
                }
                break;
            case "replicatedmap":
                if (!(cell.op.equals("get") && cell.variant.equals("miss"))) {
                    ensureRepmapSeeded(c, cell.value_size, Math.max(1, cell.working_set), seed);
                }
                break;
            default:
                break;
        }
    }

    public enum T {
        IMAP_GET, IMAP_PUT, IMAP_SET, IMAP_CONTAINS, IMAP_PIA, IMAP_REMOVE, IMAP_GETALL, IMAP_PUTALL,
        CP_GET, CP_PUT, CP_SET, CP_CAS,
        AL_GET, AL_SET, AL_INCR, AL_ADD, AL_GETADD, AL_CAS,
        AREF_GET, AREF_SET, AREF_CAS,
        QUEUE_OFFERPOLL, SET_ADDREMOVE, REP_PUT, REP_GET
    }

    /** A fully-resolved single operation (no RNG state). */
    public static final class Plan {
        public T type;
        public long key;
        public byte[] val;
        public long lval;
        public Set<Long> keys;
        public Map<Long, byte[]> entries;

        static Plan of(T t) { Plan p = new Plan(); p.type = t; return p; }
    }

    /** Advances the deterministic key-stream, producing resolved Plans. */
    public static final class Planner {
        final Manifest.Cell cell;
        final Data.SplitMix64 rng;
        final Data.Dist dist;
        final long ws;
        final int valueSize;
        final long seed;
        final int batch;
        final Map<String, Double> mix;

        public Planner(Manifest.Cell cell, int workerId, long seed) {
            this.cell = cell;
            this.ws = Math.max(1, cell.working_set);
            this.dist = new Data.Dist(cell.distribution, ws, cell.zipf_theta);
            long wseed = seed ^ ((long) workerId * 0x9E3779B97F4A7C15L) ^ fnv1a(cell.id);
            this.rng = new Data.SplitMix64(wseed);
            this.valueSize = cell.value_size;
            this.seed = seed;
            this.batch = cell.batch_size == null ? 1 : cell.batch_size;
            this.mix = cell.mix == null ? null : Data.sortedMix(cell.mix);
        }

        long idx() { return dist.next(rng); }

        public Plan plan() {
            String st = cell.structure, op = cell.op;
            if (st.equals("imap") && op.equals("mixed")) {
                double r = rng.nextDouble();
                double acc = 0.0;
                String chosen = "get";
                if (mix != null) {
                    for (Map.Entry<String, Double> e : mix.entrySet()) {
                        acc += e.getValue();
                        if (r < acc) { chosen = e.getKey(); break; }
                    }
                }
                long i = idx();
                Plan p;
                switch (chosen) {
                    case "put": p = Plan.of(T.IMAP_PUT); p.key = Data.keyI64(i); p.val = Data.valueBytes(i, valueSize, seed); return p;
                    case "rmw": p = Plan.of(T.IMAP_PUT); p.key = Data.keyI64(i); p.val = Data.valueBytes(i, valueSize, seed); return p;
                    default: p = Plan.of(T.IMAP_GET); p.key = Data.keyI64(i); return p;
                }
            }
            if (st.equals("imap")) {
                switch (op) {
                    case "get": {
                        long i = idx();
                        long k = cell.variant.equals("miss") ? i + ws : i;
                        Plan p = Plan.of(T.IMAP_GET); p.key = Data.keyI64(k); return p;
                    }
                    case "contains_key": { Plan p = Plan.of(T.IMAP_CONTAINS); p.key = Data.keyI64(idx()); return p; }
                    case "put": case "get_and_put": { long i = idx(); Plan p = Plan.of(T.IMAP_PUT); p.key = Data.keyI64(i); p.val = Data.valueBytes(i, valueSize, seed); return p; }
                    case "set": { long i = idx(); Plan p = Plan.of(T.IMAP_SET); p.key = Data.keyI64(i); p.val = Data.valueBytes(i, valueSize, seed); return p; }
                    case "put_if_absent": { long i = idx(); Plan p = Plan.of(T.IMAP_PIA); p.key = Data.keyI64(i); p.val = Data.valueBytes(i, valueSize, seed); return p; }
                    case "remove": { Plan p = Plan.of(T.IMAP_REMOVE); p.key = Data.keyI64(idx()); return p; }
                    case "get_all": {
                        Set<Long> keys = new HashSet<>();
                        for (int b = 0; b < batch; b++) keys.add(Data.keyI64(idx()));
                        Plan p = Plan.of(T.IMAP_GETALL); p.keys = keys; return p;
                    }
                    case "put_all": {
                        Map<Long, byte[]> e = new HashMap<>();
                        for (int b = 0; b < batch; b++) { long i = idx(); e.put(Data.keyI64(i), Data.valueBytes(i, valueSize, seed)); }
                        Plan p = Plan.of(T.IMAP_PUTALL); p.entries = e; return p;
                    }
                }
            }
            if (st.equals("cpmap")) {
                switch (op) {
                    case "get": { Plan p = Plan.of(T.CP_GET); p.key = Data.keyI64(idx()); return p; }
                    case "put": { long i = idx(); Plan p = Plan.of(T.CP_PUT); p.key = Data.keyI64(i); p.lval = i; return p; }
                    case "set": { long i = idx(); Plan p = Plan.of(T.CP_SET); p.key = Data.keyI64(i); p.lval = i; return p; }
                    case "compare_and_set": { Plan p = Plan.of(T.CP_CAS); p.key = Data.keyI64(idx()); return p; }
                }
            }
            if (st.equals("atomiclong")) {
                switch (op) {
                    case "get": return Plan.of(T.AL_GET);
                    case "set": { Plan p = Plan.of(T.AL_SET); p.lval = rng.nextU64(); return p; }
                    case "increment_and_get": return Plan.of(T.AL_INCR);
                    case "add_and_get": { Plan p = Plan.of(T.AL_ADD); p.lval = 1; return p; }
                    case "get_and_add": { Plan p = Plan.of(T.AL_GETADD); p.lval = 1; return p; }
                    case "compare_and_set": return Plan.of(T.AL_CAS);
                }
            }
            if (st.equals("atomicref")) {
                switch (op) {
                    case "get": return Plan.of(T.AREF_GET);
                    case "set": { Plan p = Plan.of(T.AREF_SET); p.lval = rng.nextU64(); return p; }
                    case "compare_and_set": return Plan.of(T.AREF_CAS);
                }
            }
            if (st.equals("iqueue") && op.equals("offer_poll")) {
                long i = idx();
                Plan p = Plan.of(T.QUEUE_OFFERPOLL); p.val = Data.valueBytes(i, valueSize, seed); return p;
            }
            if (st.equals("iset") && op.equals("add_remove")) {
                long i = idx();
                Plan p = Plan.of(T.SET_ADDREMOVE); p.val = Data.valueBytes(i, valueSize, seed); return p;
            }
            if (st.equals("replicatedmap")) {
                switch (op) {
                    case "put": case "get_and_put": { long i = idx(); Plan p = Plan.of(T.REP_PUT); p.key = Data.keyI64(i); p.val = Data.valueBytes(i, valueSize, seed); return p; }
                    case "get": { long i = idx(); long k = cell.variant.equals("miss") ? i + ws : i; Plan p = Plan.of(T.REP_GET); p.key = Data.keyI64(k); return p; }
                }
            }
            throw new IllegalArgumentException("planner: unsupported op " + st + "." + op);
        }
    }

    /** Shared cluster proxy handles for one lane. */
    public static final class Handles {
        final boolean isRmw;
        IMap<Long, byte[]> imap;
        CPMap<Long, Long> cpmap;
        IAtomicLong al;
        IAtomicReference<Long> aref;
        IQueue<byte[]> queue;
        ISet<byte[]> set;
        ReplicatedMap<Long, byte[]> rep;

        public static Handles build(HazelcastInstance c, Manifest.Cell cell, int workerId) {
            long ws = Math.max(1, cell.working_set);
            Handles h = new Handles(cell.op.equals("mixed") && cell.mix != null && cell.mix.containsKey("rmw"));
            switch (cell.structure) {
                case "imap": h.imap = c.getMap(imapName(cell.value_size, ws)); break;
                case "cpmap": h.cpmap = c.getCPSubsystem().getMap(cpmapName(ws)); break;
                case "atomiclong": h.al = c.getCPSubsystem().getAtomicLong(alName(workerId)); h.al.set(0); break;
                case "atomicref": h.aref = c.getCPSubsystem().getAtomicReference(arefName(workerId)); h.aref.set(0L); break;
                case "iqueue": h.queue = c.getQueue(queueName(cell.value_size)); break;
                case "iset": h.set = c.getSet(setName(cell.value_size)); break;
                case "replicatedmap": h.rep = c.getReplicatedMap(repmapName(cell.value_size, ws)); break;
                default: throw new IllegalArgumentException("unsupported structure: " + cell.structure);
            }
            return h;
        }

        Handles(boolean isRmw) { this.isRmw = isRmw; }

        public void exec(Plan p) {
            switch (p.type) {
                case IMAP_GET: imap.get(p.key); break;
                case IMAP_PUT:
                    if (isRmw) imap.get(p.key);
                    imap.put(p.key, p.val);
                    break;
                case IMAP_SET: imap.set(p.key, p.val); break;
                case IMAP_CONTAINS: imap.containsKey(p.key); break;
                case IMAP_PIA: imap.putIfAbsent(p.key, p.val); break;
                case IMAP_REMOVE: imap.remove(p.key); break;
                case IMAP_GETALL: imap.getAll(p.keys); break;
                case IMAP_PUTALL: imap.putAll(p.entries); break;
                case CP_GET: cpmap.get(p.key); break;
                case CP_PUT: cpmap.put(p.key, p.lval); break;
                case CP_SET: cpmap.set(p.key, p.lval); break;
                case CP_CAS: {
                    Long cur = cpmap.get(p.key);
                    long c = cur == null ? 0 : cur;
                    cpmap.compareAndSet(p.key, c, c + 1);
                    break;
                }
                case AL_GET: al.get(); break;
                case AL_SET: al.set(p.lval); break;
                case AL_INCR: al.incrementAndGet(); break;
                case AL_ADD: al.addAndGet(p.lval); break;
                case AL_GETADD: al.getAndAdd(p.lval); break;
                case AL_CAS: { long cur = al.get(); al.compareAndSet(cur, cur + 1); break; }
                case AREF_GET: aref.get(); break;
                case AREF_SET: aref.set(p.lval); break;
                case AREF_CAS: { Long cur = aref.get(); long c = cur == null ? 0 : cur; aref.compareAndSet(cur, c + 1); break; }
                case QUEUE_OFFERPOLL: queue.offer(p.val); queue.poll(); break;
                case SET_ADDREMOVE: set.add(p.val); set.remove(p.val); break;
                case REP_PUT: rep.put(p.key, p.val); break;
                case REP_GET: rep.get(p.key); break;
                default: throw new IllegalStateException();
            }
        }
    }

    static long fnv1a(String s) {
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < s.length(); i++) {
            h ^= (s.charAt(i) & 0xFF);
            h *= 0x100000001b3L;
        }
        return h;
    }
}
