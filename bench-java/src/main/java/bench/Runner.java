package bench;

import com.hazelcast.core.HazelcastInstance;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Load-generation drivers — mirror of hazelrust-bench/src/runner.rs. Closed-loop
 * uses C platform threads each issuing → awaiting → re-issuing (so outstanding
 * ops == C, matching the Rust tokio model). Open-loop uses a fixed-rate clock
 * with C-permit backpressure and coordinated-omission-correct latency
 * (completion − scheduled-start).
 */
public final class Runner {

    static final long LO = 1, HI = 60_000_000_000L; // 1 ns .. 60 s
    static final int SIG = 3;

    public static final class Errors {
        public long total, timeouts, server, connection, other;
        synchronized void bump(int code) {
            total++;
            switch (code) { case 1: timeouts++; break; case 2: connection++; break; case 3: server++; break; default: other++; }
        }
        void add(Errors o) { total += o.total; timeouts += o.timeouts; server += o.server; connection += o.connection; other += o.other; }
    }

    public static final class RunResult {
        public Histogram hist;
        public long ops;
        public Errors errors = new Errors();
        public List<Double> series = new ArrayList<>();
        public boolean saturated;
        public double measureSecs;
    }

    static int classify(Throwable e) {
        String s = (e.toString() + " " + (e.getMessage() == null ? "" : e.getMessage())).toLowerCase();
        if (s.contains("timeout") || s.contains("timed out")) return 1;
        if (s.contains("connection") || s.contains("connect") || s.contains("closed") || s.contains("disconnect")) return 2;
        if (s.contains("exception") || s.contains("server") || s.contains("hazelcast")) return 3;
        return 0;
    }

    static void record(Histogram h, long ns) { h.recordValue(Math.min(ns, HI)); }

    public static RunResult runClosed(HazelcastInstance client, Manifest.Cell cell, long seed,
                                       long warmupNs, long measureNs) throws InterruptedException {
        int c = Math.max(1, cell.concurrency);
        AtomicLong counter = new AtomicLong();
        long start = System.nanoTime();
        long warmupEnd = start + warmupNs;
        long measureEnd = warmupEnd + measureNs;

        Histogram[] hists = new Histogram[c];
        long[] opsArr = new long[c];
        Errors[] errsArr = new Errors[c];
        Thread[] threads = new Thread[c];

        for (int w = 0; w < c; w++) {
            final int wid = w;
            threads[w] = new Thread(() -> {
                Ops.Planner planner = new Ops.Planner(cell, wid, seed);
                Ops.Handles h = Ops.Handles.build(client, cell, wid);
                Histogram hist = new Histogram(LO, HI, SIG);
                long ops = 0;
                Errors errs = new Errors();
                while (true) {
                    long now = System.nanoTime();
                    if (now >= measureEnd) break;
                    boolean measuring = now >= warmupEnd;
                    Ops.Plan p = planner.plan();
                    long t0 = System.nanoTime();
                    try {
                        h.exec(p);
                        counter.incrementAndGet();
                        if (measuring) { record(hist, System.nanoTime() - t0); ops++; }
                    } catch (Throwable e) {
                        if (measuring) errs.bump(classify(e));
                    }
                }
                hists[wid] = hist;
                opsArr[wid] = ops;
                errsArr[wid] = errs;
            }, "bench-w" + w);
            threads[w].start();
        }

        // monitor: per-second cumulative samples for the throughput series
        List<double[]> samples = new ArrayList<>();
        Thread mon = new Thread(() -> {
            while (true) {
                long now = System.nanoTime();
                double elapsed = (now - start) / 1e9;
                synchronized (samples) { samples.add(new double[]{elapsed, counter.get()}); }
                if (now >= measureEnd) break;
                try { Thread.sleep(1000); } catch (InterruptedException ignored) { break; }
            }
        }, "bench-mon");
        mon.start();

        for (Thread t : threads) t.join();
        mon.join();

        Histogram merged = new Histogram(LO, HI, SIG);
        long totalOps = 0;
        Errors errors = new Errors();
        for (int w = 0; w < c; w++) {
            if (hists[w] != null) merged.add(hists[w]);
            totalOps += opsArr[w];
            if (errsArr[w] != null) errors.add(errsArr[w]);
        }

        RunResult rr = new RunResult();
        rr.hist = merged;
        rr.ops = totalOps;
        rr.errors = errors;
        rr.measureSecs = measureNs / 1e9;
        rr.series = perSecondSeries(samples, warmupNs / 1e9, (warmupNs + measureNs) / 1e9);
        rr.saturated = false;
        return rr;
    }

    public static RunResult runOpen(HazelcastInstance client, Manifest.Cell cell, long seed,
                                    long warmupNs, long measureNs, double targetRate) throws InterruptedException {
        int c = Math.max(1, cell.concurrency);
        double intervalNs = 1.0e9 / targetRate;
        Semaphore sem = new Semaphore(c);
        ExecutorService pool = Executors.newFixedThreadPool(c);
        Ops.Handles[] lanes = new Ops.Handles[c];
        for (int w = 0; w < c; w++) lanes[w] = Ops.Handles.build(client, cell, w);
        Ops.Planner planner = new Ops.Planner(cell, 0, seed);

        ConcurrentHistogram hist = new ConcurrentHistogram(LO, HI, SIG);
        AtomicLong ops = new AtomicLong();
        Errors errors = new Errors();

        long start = System.nanoTime();
        long warmupNsAbs = warmupNs;
        long totalNs = warmupNs + measureNs;
        long k = 0;
        double maxLagNs = 0;

        while (true) {
            double schedOff = k * intervalNs;
            if (schedOff >= totalNs) break;
            long scheduled = start + (long) schedOff;
            long now = System.nanoTime();
            if (scheduled > now) {
                long sleep = scheduled - now;
                try { Thread.sleep(sleep / 1_000_000L, (int) (sleep % 1_000_000L)); } catch (InterruptedException ignored) {}
            } else {
                double lag = now - scheduled;
                if (lag > maxLagNs) maxLagNs = lag;
            }
            boolean measuring = schedOff >= warmupNsAbs;
            sem.acquire();
            final Ops.Handles h = lanes[(int) (k % c)];
            final Ops.Plan p = planner.plan();
            final long sched = scheduled;
            pool.submit(() -> {
                try {
                    h.exec(p);
                    long lat = System.nanoTime() - sched;
                    if (measuring) { record(hist, lat); ops.incrementAndGet(); }
                } catch (Throwable e) {
                    if (measuring) errors.bump(classify(e));
                } finally {
                    sem.release();
                }
            });
            k++;
        }
        pool.shutdown();
        // drain outstanding
        sem.acquire(c);

        RunResult rr = new RunResult();
        rr.hist = hist;
        rr.ops = ops.get();
        rr.errors = errors;
        rr.measureSecs = measureNs / 1e9;
        rr.saturated = maxLagNs > Math.max(intervalNs * 5.0, 50_000_000.0);
        return rr;
    }

    static List<Double> perSecondSeries(List<double[]> samples, double lo, double hi) {
        List<Double> out = new ArrayList<>();
        for (int i = 0; i + 1 < samples.size(); i++) {
            double t0 = samples.get(i)[0], c0 = samples.get(i)[1];
            double t1 = samples.get(i + 1)[0], c1 = samples.get(i + 1)[1];
            double mid = (t0 + t1) / 2.0;
            if (mid >= lo && mid <= hi && t1 > t0) out.add((c1 - c0) / (t1 - t0));
        }
        return out;
    }
}
