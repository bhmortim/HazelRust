package bench;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Deterministic data-generation contract — BIT-IDENTICAL to the Rust harness
 * (hazelrust-bench/src/data.rs). Both clients must produce the same key set,
 * value bytes, and distribution draws. Note the use of unsigned operations
 * ({@code >>>}, {@link Long#remainderUnsigned}) to match Rust's u64 semantics.
 */
public final class Data {

    /** SplitMix64 — identical to the Rust implementation (wrapping long math). */
    public static final class SplitMix64 {
        private long state;

        public SplitMix64(long seed) { this.state = seed; }

        public long nextU64() {
            state += 0x9E3779B97F4A7C15L;
            long z = state;
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }

        /** Uniform double in [0,1) using the top 53 bits. */
        public double nextDouble() {
            return (double) (nextU64() >>> 11) * (1.0 / 9007199254740992.0); // 2^53
        }
    }

    public enum Kind { UNIFORM, SEQUENTIAL, ZIPFIAN }

    /** Access distribution over the working set [0, n). */
    public static final class Dist {
        final Kind kind;
        final long n;
        long counter = 0;
        final Zipf zipf;

        public Dist(String kindStr, long n, double theta) {
            this.n = n;
            switch (kindStr) {
                case "uniform": this.kind = Kind.UNIFORM; this.zipf = null; break;
                case "sequential": this.kind = Kind.SEQUENTIAL; this.zipf = null; break;
                case "zipfian": this.kind = Kind.ZIPFIAN; this.zipf = new Zipf(n, theta); break;
                default: throw new IllegalArgumentException("unknown distribution: " + kindStr);
            }
        }

        public long next(SplitMix64 rng) {
            switch (kind) {
                case UNIFORM: return Long.remainderUnsigned(rng.nextU64(), n);
                case SEQUENTIAL: { long v = Long.remainderUnsigned(counter, n); counter++; return v; }
                case ZIPFIAN: return zipf.next(rng);
                default: throw new IllegalStateException();
            }
        }
    }

    /** YCSB-style Zipfian (Gray et al.); hot keys cluster at low indices. */
    public static final class Zipf {
        final long n;
        final double theta, alpha, zetan, eta;

        public Zipf(long n, double theta) {
            this.n = n;
            this.theta = theta;
            this.zetan = zeta(n, theta);
            double zeta2 = zeta(2, theta);
            this.alpha = 1.0 / (1.0 - theta);
            this.eta = (1.0 - Math.pow(2.0 / n, 1.0 - theta)) / (1.0 - zeta2 / zetan);
        }

        public long next(SplitMix64 rng) {
            double u = rng.nextDouble();
            double uz = u * zetan;
            if (uz < 1.0) return 0;
            if (uz < 1.0 + Math.pow(0.5, theta)) return 1;
            long ret = (long) ((double) n * Math.pow(eta * u - eta + 1.0, alpha));
            return Long.remainderUnsigned(ret, n);
        }
    }

    /** zeta(n, theta) summed ascending to match Rust's floating-point result. */
    public static double zeta(long n, double theta) {
        double sum = 0.0;
        for (long i = 1; i <= n; i++) {
            sum += 1.0 / Math.pow((double) i, theta);
        }
        return sum;
    }

    public static long keyI64(long idx) { return idx; }

    public static String keyString(long idx, int width) {
        int digits = width - 3;
        String s = Long.toString(idx);
        StringBuilder sb = new StringBuilder("key");
        for (int i = s.length(); i < digits; i++) sb.append('0');
        sb.append(s);
        return sb.toString();
    }

    /** byte j = (vidx*1000003 + j*31 + seed) mod 256 — identical to Rust. */
    public static byte[] valueBytes(long vidx, int n, long seed) {
        byte[] v = new byte[n];
        long base = vidx * 1000003L + seed;
        for (int j = 0; j < n; j++) {
            v[j] = (byte) ((base + (long) j * 31L) & 0xFF);
        }
        return v;
    }

    /** Stable insertion-ordered copy of a mix map (matches BTreeMap iter order
     * in Rust? No — Rust uses BTreeMap (sorted by key). We sort here to match. */
    public static Map<String, Double> sortedMix(Map<String, Double> mix) {
        Map<String, Double> out = new LinkedHashMap<>();
        mix.keySet().stream().sorted().forEach(k -> out.put(k, mix.get(k)));
        return out;
    }
}
