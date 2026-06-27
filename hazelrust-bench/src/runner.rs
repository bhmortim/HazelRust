//! Load-generation drivers: closed-loop (max throughput / service time) and
//! open-loop (latency under a fixed arrival rate, coordinated-omission correct).

use anyhow::Result;
use hazelcast_client::HazelcastClient;
use hdrhistogram::Histogram;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};

use crate::manifest::Cell;
use crate::ops::{Handles, Planner};
use crate::record::{classify_code, Errors};

pub struct RunResult {
    pub hist: Histogram<u64>,
    pub ops: u64,
    pub errors: Errors,
    pub series: Vec<f64>,
    pub saturated: bool,
    pub measure_secs: f64,
}

fn new_hist() -> Histogram<u64> {
    // 1 ns .. 60 s, 3 significant figures (methodology §9.1).
    Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("hist bounds")
}

/// Closed-loop: C workers each issue → await → issue. Measures max throughput
/// and service time. Under-reports tail latency under saturation (CO) — use
/// open-loop for authoritative latency.
pub async fn run_closed(
    client: &Arc<HazelcastClient>,
    cell: &Cell,
    seed: u64,
    warmup: Duration,
    measure: Duration,
) -> Result<RunResult> {
    let c = cell.concurrency.max(1);
    let counter = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let warmup_end = start + warmup;
    let measure_end = warmup_end + measure;

    // Monitor task: sample the global counter each second to build a per-second
    // throughput series for the measurement window (stability metric §9.2).
    let mon_counter = counter.clone();
    let monitor = tokio::spawn(async move {
        let mut samples: Vec<(f64, u64)> = Vec::new();
        loop {
            let now = Instant::now();
            if now >= measure_end {
                samples.push((
                    now.duration_since(start).as_secs_f64(),
                    mon_counter.load(Ordering::Relaxed),
                ));
                break;
            }
            samples.push((
                now.duration_since(start).as_secs_f64(),
                mon_counter.load(Ordering::Relaxed),
            ));
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        samples
    });

    let mut tasks = Vec::with_capacity(c as usize);
    for w in 0..c {
        let mut planner = Planner::new(cell, w, seed);
        let h = Handles::build(client, cell, w).await?;
        let counter = counter.clone();
        tasks.push(tokio::spawn(async move {
            let mut hist = new_hist();
            let mut ops = 0u64;
            let mut errs = Errors::default();
            loop {
                let now = Instant::now();
                if now >= measure_end {
                    break;
                }
                let measuring = now >= warmup_end;
                let plan = planner.plan();
                let t0 = Instant::now();
                match h.exec(plan).await {
                    Ok(()) => {
                        counter.fetch_add(1, Ordering::Relaxed);
                        if measuring {
                            let dt = t0.elapsed().as_nanos() as u64;
                            hist.saturating_record(dt);
                            ops += 1;
                        }
                    }
                    Err(e) => {
                        if measuring {
                            errs.bump(classify_code(&e));
                        }
                    }
                }
            }
            (hist, ops, errs)
        }));
    }

    let mut merged = new_hist();
    let mut total_ops = 0u64;
    let mut errors = Errors::default();
    for t in tasks {
        let (h, o, e) = t.await?;
        merged.add(h).ok();
        total_ops += o;
        errors += e;
    }
    let samples = monitor.await?;
    let series = per_second_series(
        &samples,
        warmup.as_secs_f64(),
        (warmup + measure).as_secs_f64(),
    );

    Ok(RunResult {
        hist: merged,
        ops: total_ops,
        errors,
        series,
        saturated: false,
        measure_secs: measure.as_secs_f64(),
    })
}

/// Busy-wait until `scheduled` with µs accuracy: sleep the bulk, spin the last
/// ~1 ms. Runs on a DEDICATED OS thread (not a tokio worker) so spinning never
/// starves the async runtime. This avoids the ~1 ms timer-granularity burst
/// artifact that a sleep-based pacer would inject into the CO-corrected latency.
fn precise_wait_until(scheduled: Instant) {
    loop {
        let now = Instant::now();
        if now >= scheduled {
            return;
        }
        let rem = scheduled - now;
        if rem > Duration::from_micros(1500) {
            std::thread::sleep(rem - Duration::from_micros(1000));
        } else {
            std::hint::spin_loop();
        }
    }
}

/// Open-loop: a fixed-rate clock issues requests regardless of completion (up
/// to C outstanding). Latency = completion − scheduled-start (coordinated
/// omission correct). A dedicated scheduler thread paces with µs accuracy and
/// dispatches onto the tokio runtime. Flags `saturated` if the generator falls
/// behind (had to wait for an outstanding-op permit, i.e. the rate exceeds
/// client capacity).
pub async fn run_open(
    client: &Arc<HazelcastClient>,
    cell: &Cell,
    seed: u64,
    warmup: Duration,
    measure: Duration,
    target_rate: f64,
) -> Result<RunResult> {
    let c = cell.concurrency.max(1) as usize;
    let interval_ns = 1.0e9 / target_rate;
    let sem = Arc::new(Semaphore::new(c));
    let mut lanes = Vec::with_capacity(c);
    for w in 0..c {
        lanes.push(Handles::build(client, cell, w as u32).await?);
    }
    let planner = Planner::new(cell, 0, seed);

    let warmup_ns = warmup.as_nanos() as f64;
    let total_ns = (warmup + measure).as_nanos() as f64;

    let (tx, mut rx) = mpsc::unbounded_channel::<(u64, bool, std::result::Result<(), u8>)>();
    let recorder = tokio::spawn(async move {
        let mut hist = new_hist();
        let mut ops = 0u64;
        let mut errs = Errors::default();
        while let Some((lat, measuring, res)) = rx.recv().await {
            match res {
                Ok(()) => {
                    if measuring {
                        hist.saturating_record(lat);
                        ops += 1;
                    }
                }
                Err(code) => {
                    if measuring {
                        errs.bump(code);
                    }
                }
            }
        }
        (hist, ops, errs)
    });

    let handle = tokio::runtime::Handle::current();
    let sched_tx = tx.clone();
    let scheduler = std::thread::spawn(move || {
        let mut planner = planner;
        let start = Instant::now();
        let mut k = 0u64;
        loop {
            let sched_off = k as f64 * interval_ns;
            if sched_off >= total_ns {
                break;
            }
            let scheduled = start + Duration::from_nanos(sched_off as u64);
            precise_wait_until(scheduled);
            let measuring = sched_off >= warmup_ns;
            // Acquire an outstanding-op permit; spin if none. A transient wait
            // (e.g. a GC pause filling the C window) is a LATENCY event captured
            // in the histogram — not saturation. Sustained inability to keep up
            // shows as wall-clock overrun (computed after the loop).
            let permit = loop {
                match sem.clone().try_acquire_owned() {
                    Ok(p) => break p,
                    Err(_) => std::hint::spin_loop(),
                }
            };
            let h = lanes[(k as usize) % c].clone();
            let plan = planner.plan();
            let txc = sched_tx.clone();
            handle.spawn(async move {
                let _permit = permit;
                let res = h.exec(plan).await;
                let lat = Instant::now()
                    .saturating_duration_since(scheduled)
                    .as_nanos() as u64;
                let code = match &res {
                    Ok(()) => Ok(()),
                    Err(e) => Err(classify_code(e)),
                };
                let _ = txc.send((lat, measuring, code));
            });
            k += 1;
        }
        // Overrun = how far past the intended schedule the generator ended up.
        // Bounded transient lag recovers (overrun ~0); only a generator that
        // genuinely cannot sustain the rate ends materially behind.
        Instant::now().saturating_duration_since(start).as_nanos() as f64 - total_ns
    });
    drop(tx); // scheduler holds the last sender clone; recorder ends when it + tasks finish
    let (hist, ops, errs) = recorder.await?;
    let overrun_ns = tokio::task::spawn_blocking(move || scheduler.join().unwrap()).await?;
    // Saturated only if the generator finished materially behind the schedule.
    let saturated = overrun_ns > (total_ns * 0.05).max(100_000_000.0);

    Ok(RunResult {
        hist,
        ops,
        errors: errs,
        series: Vec::new(),
        saturated,
        measure_secs: measure.as_secs_f64(),
    })
}

/// Convert (elapsed_s, cumulative_count) samples into per-second throughput
/// values that fall within the measurement window [warmup, warmup+measure].
fn per_second_series(samples: &[(f64, u64)], lo: f64, hi: f64) -> Vec<f64> {
    let mut out = Vec::new();
    for win in samples.windows(2) {
        let (t0, c0) = win[0];
        let (t1, c1) = win[1];
        let mid = (t0 + t1) / 2.0;
        if mid >= lo && mid <= hi && t1 > t0 {
            out.push((c1.saturating_sub(c0)) as f64 / (t1 - t0));
        }
    }
    out
}
