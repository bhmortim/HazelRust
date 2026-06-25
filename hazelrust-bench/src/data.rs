//! Deterministic data-generation contract — IMPLEMENTED IDENTICALLY in the Java
//! harness (`bench-java/.../Data.java`). Both clients must produce the exact same
//! key set, value bytes, and access-distribution draws so the only variable is
//! the client. Every formula here is reproduced bit-for-bit in Java.

/// SplitMix64 PRNG. Trivially identical across languages (wrapping u64 math,
/// logical right shifts). Seeded per worker so each client's worker `w` draws
/// the same key stream.
#[derive(Clone)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform double in [0,1) using the top 53 bits (IEEE754 mantissa).
    #[inline]
    pub fn next_double(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / 9_007_199_254_740_992.0) // 2^53
    }
}

/// Access distribution over the working set [0, n).
#[derive(Clone)]
pub enum Dist {
    Uniform { n: u64 },
    Sequential { n: u64, counter: u64 },
    Zipfian(Zipf),
}

impl Dist {
    pub fn new(kind: &str, n: u64, theta: f64) -> Self {
        match kind {
            "uniform" => Dist::Uniform { n },
            "sequential" => Dist::Sequential { n, counter: 0 },
            "zipfian" => Dist::Zipfian(Zipf::new(n, theta)),
            other => panic!("unknown distribution: {other}"),
        }
    }

    #[inline]
    pub fn next(&mut self, rng: &mut SplitMix64) -> u64 {
        match self {
            Dist::Uniform { n } => rng.next_u64() % *n,
            Dist::Sequential { n, counter } => {
                let v = *counter % *n;
                *counter = counter.wrapping_add(1);
                v
            }
            Dist::Zipfian(z) => z.next(rng),
        }
    }
}

/// YCSB-style Zipfian generator (Gray et al.). Hot keys cluster at low indices;
/// identical `zetan` is produced by the same summation order in both languages.
#[derive(Clone)]
pub struct Zipf {
    n: u64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
    zeta2: f64,
}

impl Zipf {
    pub fn new(n: u64, theta: f64) -> Self {
        let zetan = zeta(n, theta);
        let zeta2 = zeta(2, theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta =
            (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);
        Self { n, theta, alpha, zetan, eta, zeta2 }
    }

    #[inline]
    pub fn next(&self, rng: &mut SplitMix64) -> u64 {
        let u = rng.next_double();
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return 1;
        }
        let ret = (self.n as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64;
        ret % self.n
    }
}

/// zeta(n, theta) = sum_{i=1..n} 1/i^theta — summed in ascending order so the
/// floating-point result is identical to the Java implementation.
pub fn zeta(n: u64, theta: f64) -> f64 {
    let mut sum = 0.0f64;
    let mut i = 1u64;
    while i <= n {
        sum += 1.0 / (i as f64).powf(theta);
        i += 1;
    }
    sum
}

/// The integer key value for working-set index `idx` and key_type.
/// For i64 keys this is just `idx`. (String keys handled by the caller.)
#[inline]
pub fn key_i64(idx: u64) -> i64 {
    idx as i64
}

/// String key of a fixed width (16 or 64). "key" + zero-padded decimal.
pub fn key_string(idx: u64, width: usize) -> String {
    let digits = width - 3; // "key" prefix
    format!("key{:0>width$}", idx, width = digits)
}

/// Deterministic value bytes of length `n` for logical value index `vidx`.
/// byte j = (vidx*1000003 + j*31 + seed) mod 256. Identical in Java.
pub fn value_bytes(vidx: u64, n: usize, seed: u64) -> Vec<u8> {
    let mut v = vec![0u8; n];
    let base = vidx.wrapping_mul(1_000_003).wrapping_add(seed);
    for (j, b) in v.iter_mut().enumerate() {
        *b = (base.wrapping_add((j as u64).wrapping_mul(31)) & 0xFF) as u8;
    }
    v
}
