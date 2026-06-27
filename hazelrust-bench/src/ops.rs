//! Operation dispatch + data seeding, split into a `Planner` (advances the
//! RNG/distribution — `&mut`) and `Handles` (shared cluster proxies — executes
//! a resolved `Plan`). This split lets the open-loop scheduler advance one
//! deterministic key-stream while fanning execution across C lanes.
//!
//! Data discipline (methodology §3): read-only and update cells share a
//! pre-seeded immutable dataset named by its *shape* (structure,value_size,
//! working_set) so seeding happens once and memory is bounded to one map per
//! shape. Update/set overwrite existing keys with same-size values, so the key
//! set stays [0,ws) and never grows. Destructive ops are excluded from the run.

use anyhow::Result;
use hazelcast_client::proxy::{AtomicLong, CPMap, IQueue, ISet, ReplicatedMap};
use hazelcast_client::{AtomicReference, HazelcastClient, IMap};
use std::collections::HashMap;
use std::sync::Arc;

use crate::data::{key_i64, value_bytes, Dist, SplitMix64};
use crate::manifest::Cell;

pub fn imap_name(value_size: u32, ws: u64) -> String {
    format!("bench-imap-v{value_size}-ws{ws}")
}
pub fn cpmap_name(ws: u64) -> String {
    format!("bench-cpmap-ws{ws}")
}
pub fn al_name(worker: u32) -> String {
    format!("bench-al-{worker}")
}
pub fn aref_name(worker: u32) -> String {
    format!("bench-aref-{worker}")
}
pub fn queue_name(value_size: u32) -> String {
    format!("bench-queue-v{value_size}")
}
pub fn set_name(value_size: u32) -> String {
    format!("bench-set-v{value_size}")
}
pub fn repmap_name(value_size: u32, ws: u64) -> String {
    format!("bench-repmap-v{value_size}-ws{ws}")
}

/// Seed a shared IMap with `ws` entries of `value_size` bytes if not already
/// populated. Idempotent (checks size first); batched put_all.
pub async fn ensure_imap_seeded(
    client: &HazelcastClient,
    value_size: u32,
    ws: u64,
    seed: u64,
) -> Result<IMap<i64, Vec<u8>>> {
    let map: IMap<i64, Vec<u8>> = client.get_map(&imap_name(value_size, ws));
    let have = map.size().await? as u64;
    if have >= ws {
        return Ok(map);
    }
    let batch = 1000u64;
    let mut i = 0u64;
    while i < ws {
        let end = (i + batch).min(ws);
        let mut entries: HashMap<i64, Vec<u8>> = HashMap::with_capacity((end - i) as usize);
        for idx in i..end {
            entries.insert(key_i64(idx), value_bytes(idx, value_size as usize, seed));
        }
        map.put_all(entries).await?;
        i = end;
    }
    Ok(map)
}

pub async fn ensure_cpmap_seeded(client: &HazelcastClient, ws: u64) -> Result<CPMap<i64, i64>> {
    let cpmap: CPMap<i64, i64> = client.get_cp_map(&cpmap_name(ws));
    if cpmap.get(&key_i64(ws - 1)).await?.is_none() {
        for idx in 0..ws {
            cpmap.set(key_i64(idx), idx as i64).await?;
        }
    }
    Ok(cpmap)
}

/// Seed a shared ReplicatedMap with `ws` entries of `value_size` bytes if not
/// already populated. Idempotent (checks size first); batched put_all.
pub async fn ensure_repmap_seeded(
    client: &HazelcastClient,
    value_size: u32,
    ws: u64,
    seed: u64,
) -> Result<ReplicatedMap<i64, Vec<u8>>> {
    let rep: ReplicatedMap<i64, Vec<u8>> = client.get_replicated_map(&repmap_name(value_size, ws));
    if rep.size().await? as u64 >= ws {
        return Ok(rep);
    }
    let batch = 1000u64;
    let mut i = 0u64;
    while i < ws {
        let end = (i + batch).min(ws);
        let mut entries: HashMap<i64, Vec<u8>> = HashMap::with_capacity((end - i) as usize);
        for idx in i..end {
            entries.insert(key_i64(idx), value_bytes(idx, value_size as usize, seed));
        }
        rep.put_all(entries).await?;
        i = end;
    }
    Ok(rep)
}

/// A fully-resolved single operation (no RNG state). Cheap to move into a task.
#[derive(Clone)]
pub enum Plan {
    ImapGet(i64),
    ImapPut(i64, Vec<u8>),
    ImapSet(i64, Vec<u8>),
    ImapContains(i64),
    ImapPutIfAbsent(i64, Vec<u8>),
    ImapRemove(i64),
    ImapGetAll(Vec<i64>),
    ImapPutAll(HashMap<i64, Vec<u8>>),
    CpGet(i64),
    CpPut(i64, i64),
    CpSet(i64, i64),
    CpCas(i64),
    AlGet,
    AlSet(i64),
    AlIncr,
    AlAdd(i64),
    AlGetAdd(i64),
    AlCas,
    ArefGet,
    ArefSet(i64),
    ArefCas,
    QueueOfferPoll(Vec<u8>),
    SetAddRemove(Vec<u8>),
    RepPut(i64, Vec<u8>),
    RepGet(i64),
}

/// Advances the deterministic key-stream and produces resolved `Plan`s.
pub struct Planner {
    cell: Cell,
    rng: SplitMix64,
    dist: Dist,
    ws: u64,
    value_size: usize,
    seed: u64,
    batch: usize,
}

impl Planner {
    pub fn new(cell: &Cell, worker_id: u32, seed: u64) -> Planner {
        let ws = cell.working_set.max(1);
        let dist = Dist::new(&cell.distribution, ws, cell.zipf_theta);
        let wseed = seed ^ (worker_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ fnv1a(&cell.id);
        Planner {
            cell: cell.clone(),
            rng: SplitMix64::new(wseed),
            dist,
            ws,
            value_size: cell.value_size as usize,
            seed,
            batch: cell.batch_size.unwrap_or(1) as usize,
        }
    }

    #[inline]
    fn idx(&mut self) -> u64 {
        self.dist.next(&mut self.rng)
    }

    pub fn plan(&mut self) -> Plan {
        let st = self.cell.structure.as_str();
        let op = self.cell.op.as_str();
        let vs = self.value_size;
        let seed = self.seed;
        match (st, op) {
            ("imap", "mixed") => {
                let r = self.rng.next_double();
                let mut acc = 0.0;
                let mut chosen = String::from("get");
                if let Some(mix) = &self.cell.mix {
                    for (k, p) in mix.iter() {
                        acc += *p;
                        if r < acc {
                            chosen = k.clone();
                            break;
                        }
                    }
                }
                let i = self.idx();
                match chosen.as_str() {
                    "put" => Plan::ImapPut(key_i64(i), value_bytes(i, vs, seed)),
                    "rmw" => Plan::ImapPut(key_i64(i), value_bytes(i, vs, seed)), // exec does get+put for rmw via Mix marker
                    _ => Plan::ImapGet(key_i64(i)),
                }
            }
            ("imap", "get") => {
                let i = self.idx();
                let k = if self.cell.variant == "miss" {
                    i + self.ws
                } else {
                    i
                };
                Plan::ImapGet(key_i64(k))
            }
            ("imap", "contains_key") => Plan::ImapContains(key_i64(self.idx())),
            ("imap", "put") | ("imap", "get_and_put") => {
                let i = self.idx();
                Plan::ImapPut(key_i64(i), value_bytes(i, vs, seed))
            }
            ("imap", "set") => {
                let i = self.idx();
                Plan::ImapSet(key_i64(i), value_bytes(i, vs, seed))
            }
            ("imap", "put_if_absent") => {
                let i = self.idx();
                Plan::ImapPutIfAbsent(key_i64(i), value_bytes(i, vs, seed))
            }
            ("imap", "remove") => Plan::ImapRemove(key_i64(self.idx())),
            ("imap", "get_all") => {
                let mut keys = Vec::with_capacity(self.batch);
                for _ in 0..self.batch {
                    keys.push(key_i64(self.idx()));
                }
                Plan::ImapGetAll(keys)
            }
            ("imap", "put_all") => {
                let mut e = HashMap::with_capacity(self.batch);
                for _ in 0..self.batch {
                    let i = self.idx();
                    e.insert(key_i64(i), value_bytes(i, vs, seed));
                }
                Plan::ImapPutAll(e)
            }
            ("cpmap", "get") => Plan::CpGet(key_i64(self.idx())),
            ("cpmap", "put") => {
                let i = self.idx();
                Plan::CpPut(key_i64(i), i as i64)
            }
            ("cpmap", "set") => {
                let i = self.idx();
                Plan::CpSet(key_i64(i), i as i64)
            }
            ("cpmap", "compare_and_set") => Plan::CpCas(key_i64(self.idx())),
            ("atomiclong", "get") => Plan::AlGet,
            ("atomiclong", "set") => Plan::AlSet(self.rng.next_u64() as i64),
            ("atomiclong", "increment_and_get") => Plan::AlIncr,
            ("atomiclong", "add_and_get") => Plan::AlAdd(1),
            ("atomiclong", "get_and_add") => Plan::AlGetAdd(1),
            ("atomiclong", "compare_and_set") => Plan::AlCas,
            ("atomicref", "get") => Plan::ArefGet,
            ("atomicref", "set") => Plan::ArefSet(self.rng.next_u64() as i64),
            ("atomicref", "compare_and_set") => Plan::ArefCas,
            ("iqueue", "offer_poll") => {
                let i = self.idx();
                Plan::QueueOfferPoll(value_bytes(i, vs, seed))
            }
            ("iset", "add_remove") => {
                let i = self.idx();
                Plan::SetAddRemove(value_bytes(i, vs, seed))
            }
            ("replicatedmap", "put") | ("replicatedmap", "get_and_put") => {
                let i = self.idx();
                Plan::RepPut(key_i64(i), value_bytes(i, vs, seed))
            }
            ("replicatedmap", "get") => {
                let i = self.idx();
                let k = if self.cell.variant == "miss" {
                    i + self.ws
                } else {
                    i
                };
                Plan::RepGet(key_i64(k))
            }
            (s, o) => panic!("planner: unsupported op {s}.{o}"),
        }
    }
}

/// Shared cluster proxy handles for one lane. Cheap to clone (proxies wrap Arc).
#[derive(Clone)]
pub struct Handles {
    is_rmw: bool,
    imap: Option<IMap<i64, Vec<u8>>>,
    cpmap: Option<CPMap<i64, i64>>,
    al: Option<AtomicLong>,
    aref: Option<AtomicReference<i64>>,
    queue: Option<IQueue<Vec<u8>>>,
    set: Option<ISet<Vec<u8>>>,
    rep: Option<ReplicatedMap<i64, Vec<u8>>>,
}

impl Handles {
    pub async fn build(
        client: &Arc<HazelcastClient>,
        cell: &Cell,
        worker_id: u32,
    ) -> Result<Handles> {
        let ws = cell.working_set.max(1);
        let mut h = Handles {
            is_rmw: cell.op == "mixed"
                && cell
                    .mix
                    .as_ref()
                    .map(|m| m.contains_key("rmw"))
                    .unwrap_or(false),
            imap: None,
            cpmap: None,
            al: None,
            aref: None,
            queue: None,
            set: None,
            rep: None,
        };
        match cell.structure.as_str() {
            "imap" => h.imap = Some(client.get_map(&imap_name(cell.value_size, ws))),
            "cpmap" => h.cpmap = Some(client.get_cp_map(&cpmap_name(ws))),
            "atomiclong" => {
                let al = client.get_atomic_long(&al_name(worker_id));
                al.set(0).await?;
                h.al = Some(al);
            }
            "atomicref" => {
                let aref = client.get_atomic_reference(&aref_name(worker_id));
                aref.set(Some(0)).await?;
                h.aref = Some(aref);
            }
            "iqueue" => h.queue = Some(client.get_queue(&queue_name(cell.value_size))),
            "iset" => h.set = Some(client.get_set(&set_name(cell.value_size))),
            "replicatedmap" => {
                h.rep = Some(client.get_replicated_map(&repmap_name(cell.value_size, ws)))
            }
            other => anyhow::bail!("unsupported structure: {other}"),
        }
        Ok(h)
    }

    pub async fn exec(&self, plan: Plan) -> Result<()> {
        match plan {
            Plan::ImapGet(k) => {
                let m = self.imap.as_ref().unwrap();
                let _ = m.get(&k).await?;
                // rmw: the planner emitted ImapPut for rmw draws; pure-get cells
                // and mixed-get both land here. rmw is modeled as get+put below.
            }
            Plan::ImapPut(k, v) => {
                let m = self.imap.as_ref().unwrap();
                if self.is_rmw {
                    let _ = m.get(&k).await?;
                }
                let _ = m.put(k, v).await?;
            }
            Plan::ImapSet(k, v) => self.imap.as_ref().unwrap().set(k, v).await?,
            Plan::ImapContains(k) => {
                let _ = self.imap.as_ref().unwrap().contains_key(&k).await?;
            }
            Plan::ImapPutIfAbsent(k, v) => {
                let _ = self.imap.as_ref().unwrap().put_if_absent(k, v).await?;
            }
            Plan::ImapRemove(k) => {
                let _ = self.imap.as_ref().unwrap().remove(&k).await?;
            }
            Plan::ImapGetAll(keys) => {
                let _ = self.imap.as_ref().unwrap().get_all(&keys).await?;
            }
            Plan::ImapPutAll(e) => self.imap.as_ref().unwrap().put_all(e).await?,
            Plan::CpGet(k) => {
                let _ = self.cpmap.as_ref().unwrap().get(&k).await?;
            }
            Plan::CpPut(k, v) => {
                let _ = self.cpmap.as_ref().unwrap().put(k, v).await?;
            }
            Plan::CpSet(k, v) => self.cpmap.as_ref().unwrap().set(k, v).await?,
            Plan::CpCas(k) => {
                let cp = self.cpmap.as_ref().unwrap();
                let cur = cp.get(&k).await?.unwrap_or(0);
                let _ = cp.compare_and_set(&k, &cur, cur + 1).await?;
            }
            Plan::AlGet => {
                let _ = self.al.as_ref().unwrap().get().await?;
            }
            Plan::AlSet(v) => self.al.as_ref().unwrap().set(v).await?,
            Plan::AlIncr => {
                let _ = self.al.as_ref().unwrap().increment_and_get().await?;
            }
            Plan::AlAdd(d) => {
                let _ = self.al.as_ref().unwrap().add_and_get(d).await?;
            }
            Plan::AlGetAdd(d) => {
                let _ = self.al.as_ref().unwrap().get_and_add(d).await?;
            }
            Plan::AlCas => {
                let al = self.al.as_ref().unwrap();
                let cur = al.get().await?;
                let _ = al.compare_and_set(cur, cur + 1).await?;
            }
            Plan::ArefGet => {
                let _ = self.aref.as_ref().unwrap().get().await?;
            }
            Plan::ArefSet(v) => self.aref.as_ref().unwrap().set(Some(v)).await?,
            Plan::ArefCas => {
                let aref = self.aref.as_ref().unwrap();
                let cur = aref.get().await?;
                let _ = aref
                    .compare_and_set(cur.as_ref(), Some(cur.unwrap_or(0) + 1))
                    .await?;
            }
            Plan::QueueOfferPoll(v) => {
                let q = self.queue.as_ref().unwrap();
                let _ = q.offer(v).await?;
                let _ = q.poll().await?;
            }
            Plan::SetAddRemove(v) => {
                let s = self.set.as_ref().unwrap();
                let _ = s.add(v.clone()).await?;
                let _ = s.remove(&v).await?;
            }
            Plan::RepPut(k, v) => {
                let _ = self.rep.as_ref().unwrap().put(k, v).await?;
            }
            Plan::RepGet(k) => {
                let _ = self.rep.as_ref().unwrap().get(&k).await?;
            }
        }
        Ok(())
    }
}

/// Seed whatever shared dataset the cell needs (called once before workers run).
pub async fn ensure_seeded(client: &Arc<HazelcastClient>, cell: &Cell, seed: u64) -> Result<()> {
    match cell.structure.as_str() {
        "imap" => {
            // get(miss) needs no seed; everything else reads/updates [0,ws).
            if !(cell.op == "get" && cell.variant == "miss") {
                ensure_imap_seeded(client, cell.value_size, cell.working_set.max(1), seed).await?;
            }
        }
        "cpmap" => {
            if cell.op != "put" || cell.variant != "insert" {
                ensure_cpmap_seeded(client, cell.working_set.max(1)).await?;
            }
        }
        "replicatedmap" => {
            // get(miss) needs no seed; get(hit)/put read/update [0,ws).
            if !(cell.op == "get" && cell.variant == "miss") {
                ensure_repmap_seeded(client, cell.value_size, cell.working_set.max(1), seed)
                    .await?;
            }
        }
        // iqueue (offer+poll) and iset (add+remove) are self-balancing and need
        // no pre-seed; their per-op state returns to baseline.
        _ => {}
    }
    Ok(())
}

/// 64-bit FNV-1a over the cell id, used to diversify per-worker seeds.
fn fnv1a(s: &str) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}
