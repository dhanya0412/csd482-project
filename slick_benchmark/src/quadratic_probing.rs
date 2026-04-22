// =============================================================================
// quadratic_probing.rs  —  reusable module
// =============================================================================
// Quadratic probing collision resolution for open-addressing hash tables.
//
// Probe sequence:  h(k, i) = ( h'(k) + i² )  mod  m     (c₁=0, c₂=1)
//
// Table capacity is always rounded up to the next prime to guarantee
// that the first ⌊m/2⌋ probes are distinct slots.
// =============================================================================

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fmt::Debug;

#[derive(Clone)]
enum Slot<K> {
    Empty,
    Deleted,
    Occupied(K),
}

// ── Stats snapshot returned after each benchmark run ─────────────────────────
#[derive(Debug, Clone)]
pub struct BenchStats {
    pub dataset:           String,
    pub load_factor:       f64,
    pub capacity:          usize,
    pub size:              usize,
    pub empty_cells:       usize,
    pub tombstones:        usize,
    pub avg_probes_insert: f64,
    pub avg_probes_find_hit:  f64,
    pub avg_probes_find_miss: f64,
    pub insert_ns_per_op:  f64,
    pub find_ns_per_op:    f64,
}

// ── Prime number helper ──────────────────────────────────────────────────────
/// Returns the smallest prime >= n.  Used to choose table capacity so that
/// the quadratic probe sequence `h + 0², h + 1², h + 2², …` generates
/// ⌊m/2⌋ distinct indices, guaranteeing an empty slot can be found.
fn next_prime(n: usize) -> usize {
    if n <= 2 { return 2; }
    let mut candidate = if n % 2 == 0 { n + 1 } else { n };
    loop {
        if is_prime(candidate) { return candidate; }
        candidate += 2;
    }
}

fn is_prime(n: usize) -> bool {
    if n < 2 { return false; }
    if n < 4 { return true; }
    if n % 2 == 0 || n % 3 == 0 { return false; }
    let mut i = 5;
    while i * i <= n {
        if n % i == 0 || n % (i + 2) == 0 { return false; }
        i += 6;
    }
    true
}

// ── Hash table ────────────────────────────────────────────────────────────────
pub struct QuadraticProbingHashTable<K> {
    table:    Vec<Slot<K>>,
    capacity: usize,
    size:     usize,

    total_probes_insert:    u64,
    total_probes_find_hit:  u64,
    total_probes_find_miss: u64,
    insert_calls:           u64,
    find_hit_calls:         u64,
    find_miss_calls:        u64,
}

impl<K: Hash + Eq + Clone + Debug> QuadraticProbingHashTable<K> {
    /// Create a new quadratic-probing table.
    /// `min_capacity` is rounded up to the next prime.
    pub fn new(min_capacity: usize) -> Self {
        assert!(min_capacity > 0);
        let capacity = next_prime(min_capacity);
        Self {
            table:    (0..capacity).map(|_| Slot::Empty).collect(),
            capacity,
            size: 0,
            total_probes_insert:    0,
            total_probes_find_hit:  0,
            total_probes_find_miss: 0,
            insert_calls:           0,
            find_hit_calls:         0,
            find_miss_calls:        0,
        }
    }

    fn hash(&self, key: &K) -> usize {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() as usize) % self.capacity
    }

    /// Insert key. Returns true if newly inserted.
    pub fn insert(&mut self, key: K) -> bool {
        self.insert_calls += 1;
        let start = self.hash(&key);
        let mut first_del: Option<usize> = None;
        let mut probes = 0u64;

        for i in 0..self.capacity {
            // ── QUADRATIC PROBE: offset = i² ────────────────────────────
            let idx = (start + i * i) % self.capacity;
            probes += 1;
            match &self.table[idx] {
                Slot::Empty => {
                    let t = first_del.unwrap_or(idx);
                    self.table[t] = Slot::Occupied(key);
                    self.size += 1;
                    self.total_probes_insert += probes;
                    return true;
                }
                Slot::Deleted => {
                    if first_del.is_none() { first_del = Some(idx); }
                }
                Slot::Occupied(k) => {
                    if *k == key {
                        self.total_probes_insert += probes;
                        return false;
                    }
                }
            }
        }
        // Fallback: if the quadratic sequence didn't find Empty but we
        // encountered a Deleted slot earlier, insert there.
        if let Some(t) = first_del {
            self.table[t] = Slot::Occupied(key);
            self.size += 1;
            self.total_probes_insert += probes;
            return true;
        }
        self.total_probes_insert += probes;
        false
    }

    /// Find key. Returns true if present.
    pub fn find(&mut self, key: &K) -> bool {
        let start = self.hash(key);
        let mut probes = 0u64;

        for i in 0..self.capacity {
            // ── QUADRATIC PROBE: offset = i² ────────────────────────────
            let idx = (start + i * i) % self.capacity;
            probes += 1;
            match &self.table[idx] {
                Slot::Empty => {
                    self.total_probes_find_miss += probes;
                    self.find_miss_calls += 1;
                    return false;
                }
                Slot::Deleted => continue,
                Slot::Occupied(k) => {
                    if k == key {
                        self.total_probes_find_hit += probes;
                        self.find_hit_calls += 1;
                        return true;
                    }
                }
            }
        }
        self.total_probes_find_miss += probes;
        self.find_miss_calls += 1;
        false
    }

    pub fn bulk_load(&mut self, keys: impl IntoIterator<Item = K>) {
        for k in keys { self.insert(k); }
    }

    pub fn size(&self)     -> usize { self.size }
    pub fn capacity(&self) -> usize { self.capacity }
    pub fn load_factor(&self) -> f64 { self.size as f64 / self.capacity as f64 }

    pub fn empty_cell_count(&self) -> usize {
        self.table.iter().filter(|s| matches!(s, Slot::Empty)).count()
    }
    pub fn tombstone_count(&self) -> usize {
        self.table.iter().filter(|s| matches!(s, Slot::Deleted)).count()
    }

    pub fn avg_probes_insert(&self) -> f64 {
        if self.insert_calls == 0 { 0.0 }
        else { self.total_probes_insert as f64 / self.insert_calls as f64 }
    }
    pub fn avg_probes_find_hit(&self) -> f64 {
        if self.find_hit_calls == 0 { 0.0 }
        else { self.total_probes_find_hit as f64 / self.find_hit_calls as f64 }
    }
    pub fn avg_probes_find_miss(&self) -> f64 {
        if self.find_miss_calls == 0 { 0.0 }
        else { self.total_probes_find_miss as f64 / self.find_miss_calls as f64 }
    }
}
