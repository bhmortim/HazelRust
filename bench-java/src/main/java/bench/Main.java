package bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.HdrHistogram.Histogram;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * hazeljava-bench — official Hazelcast Java client benchmark harness. Mirrors
 * hazelrust-bench: identical manifest, args, concurrency model, and output
 * schema. GC log + JFR are enabled via JVM flags injected by the orchestrator.
 */
@Command(name = "hazeljava-bench", mixinStandardHelpOptions = true,
        description = "Official Hazelcast Java client benchmark harness")
public class Main implements Callable<Integer> {

    @Option(names = "--manifest", required = true) String manifest;
    @Option(names = "--cell", required = true) String cell;
    @Option(names = "--fork", defaultValue = "0") int fork;
    @Option(names = "--trial", defaultValue = "0") int trial;
    @Option(names = "--out", defaultValue = ".") String out;
    @Option(names = "--cluster", defaultValue = "127.0.0.1:5701") String cluster;
    @Option(names = "--cluster-name", defaultValue = "dev") String clusterName;
    @Option(names = "--seed") Long seedOpt;
    @Option(names = "--target-rate") Double targetRate;
    @Option(names = "--warmup-s") Long warmupSOpt;
    @Option(names = "--measure-s") Long measureSOpt;
    @Option(names = "--commit", defaultValue = "unknown") String commit;
    @Option(names = "--client-version", defaultValue = "5.7.0") String clientVersion;
    @Option(names = "--seed-only", defaultValue = "false") boolean seedOnly;
    @Option(names = "--stdout", defaultValue = "false") boolean stdout;

    static final double NS_PER_US = 1000.0;
    static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Integer call() throws Exception {
        Manifest m = MAPPER.readValue(Path.of(manifest).toFile(), Manifest.class);
        Manifest.Cell c = m.find(cell);
        if (c == null) { System.err.println("cell not found: " + cell); return 2; }
        long seed = seedOpt != null ? seedOpt : m.meta.seed;

        String[] addrs = cluster.split(",");
        ClientConfig cfg = new ClientConfig();
        cfg.setClusterName(clusterName);
        for (String a : addrs) cfg.getNetworkConfig().addAddress(a.trim());
        cfg.getNetworkConfig().setSmartRouting(true);          // §1.4 smart routing ON
        cfg.setProperty("hazelcast.client.statistics.enabled", "false"); // stats OFF
        cfg.getMetricsConfig().setEnabled(false);              // near-cache OFF (default)
        cfg.setProperty("hazelcast.logging.type", "none");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(cfg);
        try {
            Ops.ensureSeeded(client, c, seed);
            if (seedOnly) {
                System.out.println("seeded cell " + c.id + " (structure=" + c.structure + ")");
                return 0;
            }

            long warmupNs = (warmupSOpt != null ? warmupSOpt : c.warmup_s) * 1_000_000_000L;
            long measureNs = (measureSOpt != null ? measureSOpt : c.measure_s) * 1_000_000_000L;

            Runner.RunResult rr;
            if ("open".equals(c.load_model)) {
                double rate = targetRate != null ? targetRate : (c.target_rate != null ? c.target_rate : -1);
                if (rate <= 0) { System.err.println("open-loop cell requires --target-rate"); return 2; }
                rr = Runner.runOpen(client, c, seed, warmupNs, measureNs, rate);
            } else {
                rr = Runner.runClosed(client, c, seed, warmupNs, measureNs);
            }

            double throughput = rr.measureSecs > 0 ? rr.ops / rr.measureSecs : 0.0;
            Map<String, Object> rec = buildRecord(c, m, rr, throughput, seed, addrs);
            String json = MAPPER.writeValueAsString(rec);
            String fname = out.replaceAll("/+$", "") + "/" + c.id + ".java.f" + fork + ".t" + trial + ".json";
            Files.writeString(Path.of(fname), json);
            if (stdout) System.out.println(json);
            @SuppressWarnings("unchecked")
            Map<String, Object> lat = (Map<String, Object>) rec.get("latency");
            System.err.printf("[java] %s f%d t%d: ops=%d thr=%.0f/s p50=%.1fus p99=%.1fus p99.9=%.1fus errors=%d saturated=%b%n",
                    c.id, fork, trial, rr.ops, throughput, lat.get("p50"), lat.get("p99"), lat.get("p999"),
                    rr.errors.total, rr.saturated);
            return 0;
        } finally {
            client.shutdown();
        }
    }

    Map<String, Object> buildRecord(Manifest.Cell c, Manifest m, Runner.RunResult rr,
                                    double throughput, long seed, String[] addrs) {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("client", "java");
        rec.put("version", "0.1.0");
        rec.put("commit", commit);
        rec.put("cell_id", c.id);
        rec.put("suite", c.suite);
        rec.put("structure", c.structure);
        rec.put("op", c.op);
        rec.put("variant", c.variant);
        rec.put("key_type", c.key_type);
        rec.put("value_kind", c.value_kind);
        rec.put("value_size", c.value_size);
        rec.put("working_set", c.working_set);
        rec.put("concurrency", c.concurrency);
        rec.put("load_model", c.load_model);
        rec.put("target_rate", targetRate != null ? targetRate : c.target_rate);
        rec.put("distribution", c.distribution);
        rec.put("batch_size", c.batch_size);
        rec.put("fork", fork);
        rec.put("trial", trial);
        rec.put("ops", rr.ops);
        rec.put("duration_s", rr.measureSecs);
        rec.put("throughput", throughput);
        rec.put("saturated", rr.saturated);

        Map<String, Object> errs = new LinkedHashMap<>();
        errs.put("total", rr.errors.total);
        errs.put("timeouts", rr.errors.timeouts);
        errs.put("server", rr.errors.server);
        errs.put("connection", rr.errors.connection);
        errs.put("other", rr.errors.other);
        rec.put("errors", errs);

        rec.put("latency", latencyMap(rr.hist));
        rec.put("throughput_series", rr.series);

        Map<String, Object> mem = new LinkedHashMap<>();
        mem.put("rss_peak_kb", readVmHwmKb());
        rec.put("mem", mem);

        Map<String, Object> prov = new LinkedHashMap<>();
        prov.put("client", "java");
        prov.put("harness_version", "0.1.0");
        prov.put("commit", commit);
        prov.put("hz_client_version", clientVersion);
        prov.put("java_version", System.getProperty("java.version"));
        prov.put("java_vm", System.getProperty("java.vm.name") + " " + System.getProperty("java.vm.version"));
        prov.put("jvm_args", ManagementFactory.getRuntimeMXBean().getInputArguments());
        prov.put("cluster_name", clusterName);
        prov.put("addresses", List.of(addrs));
        prov.put("smart_routing", true);
        prov.put("statistics", false);
        prov.put("near_cache", false);
        prov.put("pid", ProcessHandle.current().pid());
        prov.put("manifest_tier", m.meta.tier);
        prov.put("manifest_schema", m.meta.schema_version);
        rec.put("provenance", prov);

        rec.put("ts_unix_ms", System.currentTimeMillis());
        return rec;
    }

    Map<String, Object> latencyMap(Histogram h) {
        Map<String, Object> lat = new LinkedHashMap<>();
        long v999 = h.getValueAtPercentile(99.9);
        lat.put("count", h.getTotalCount());
        lat.put("min", h.getMinValue() / NS_PER_US);
        lat.put("p25", h.getValueAtPercentile(25.0) / NS_PER_US);
        lat.put("p50", h.getValueAtPercentile(50.0) / NS_PER_US);
        lat.put("p75", h.getValueAtPercentile(75.0) / NS_PER_US);
        lat.put("p90", h.getValueAtPercentile(90.0) / NS_PER_US);
        lat.put("p95", h.getValueAtPercentile(95.0) / NS_PER_US);
        lat.put("p99", h.getValueAtPercentile(99.0) / NS_PER_US);
        lat.put("p999", h.getValueAtPercentile(99.9) / NS_PER_US);
        lat.put("p9999", h.getValueAtPercentile(99.99) / NS_PER_US);
        lat.put("max", h.getMaxValue() / NS_PER_US);
        lat.put("mean", h.getMean() / NS_PER_US);
        lat.put("stddev", h.getStdDeviation() / NS_PER_US);
        lat.put("samples_above_p999", h.getCountBetweenValues(v999, h.getMaxValue()));
        ByteBuffer buf = ByteBuffer.allocate(h.getNeededByteBufferCapacity());
        int len = h.encodeIntoCompressedByteBuffer(buf);
        byte[] arr = new byte[len];
        System.arraycopy(buf.array(), 0, arr, 0, len);
        lat.put("hgrm_b64", Base64.getEncoder().encodeToString(arr));
        return lat;
    }

    static long readVmHwmKb() {
        try {
            for (String line : Files.readAllLines(Path.of("/proc/self/status"))) {
                if (line.startsWith("VmHWM:")) {
                    String[] parts = line.replaceAll("[^0-9]", " ").trim().split("\\s+");
                    return Long.parseLong(parts[0]);
                }
            }
        } catch (Exception ignored) {}
        return 0;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new Main()).execute(args));
    }
}
