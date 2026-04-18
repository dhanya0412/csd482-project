// =============================================================================
// Linear Probing Hash Table
// =============================================================================
// Project: Practical Implementation and Benchmarking of Sliding Block Hashing
//          (Slick) Against Classical Hashing Techniques — CSD482
//
// Operations:  insert, find, delete, bulk_load
// Deletion:    Lazy (tombstone) — preserves probe chains
// Stats:       Probe counts, load factor, empty cells — for benchmarking
// =============================================================================

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fmt::Debug;

// -----------------------------------------------------------------------------
// Slot: represents the state of each table cell
// -----------------------------------------------------------------------------
#[derive(Debug, Clone)]
enum Slot<K> {
    Empty,
    Deleted,
    Occupied(K),
}

// -----------------------------------------------------------------------------
// Stats: all benchmarking counters in one struct
// -----------------------------------------------------------------------------
#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub capacity:          usize,
    pub size:              usize,
    pub load_factor:       f64,
    pub empty_cells:       usize,
    pub tombstones:        usize,
    pub insert_calls:      u64,
    pub find_calls:        u64,
    pub delete_calls:      u64,
    pub successful_finds:  u64,
    pub failed_finds:      u64,
    pub total_probes_insert: u64,
    pub total_probes_find:   u64,
    pub total_probes_delete: u64,
    pub avg_probes_insert: f64,
    pub avg_probes_find:   f64,
    pub avg_probes_delete: f64,
}

// -----------------------------------------------------------------------------
// LinearProbingHashTable
// -----------------------------------------------------------------------------
pub struct LinearProbingHashTable<K> {
    table:    Vec<Slot<K>>,
    capacity: usize,
    size:     usize,   // live elements only (no tombstones)

    // Benchmarking counters
    total_probes_insert: u64,
    total_probes_find:   u64,
    total_probes_delete: u64,
    insert_calls:        u64,
    find_calls:          u64,
    delete_calls:        u64,
    successful_finds:    u64,
    failed_finds:        u64,
}

impl<K: Hash + Eq + Clone + Debug> LinearProbingHashTable<K> {

    /// Create a new hash table with the given capacity.
    /// Capacity should be >= number of elements / desired load factor.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "Capacity must be > 0");
        Self {
            table:    (0..capacity).map(|_| Slot::Empty).collect(),
            capacity,
            size: 0,
            total_probes_insert: 0,
            total_probes_find:   0,
            total_probes_delete: 0,
            insert_calls:        0,
            find_calls:          0,
            delete_calls:        0,
            successful_finds:    0,
            failed_finds:        0,
        }
    }

    // -------------------------------------------------------------------------
    // Hash function
    // -------------------------------------------------------------------------

    /// Map a key to a slot index using Rust's DefaultHasher.
    fn hash(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.capacity
    }

    // -------------------------------------------------------------------------
    // insert
    // -------------------------------------------------------------------------

    /// Insert a key into the table.
    ///
    /// Returns:
    ///   `true`  — key was newly inserted.
    ///   `false` — key already existed (no-op) or table is completely full.
    ///
    /// Probe sequence: h(k), h(k)+1, h(k)+2, ... (mod capacity)
    /// Reuses the first tombstone slot if the key is not found.
    pub fn insert(&mut self, key: K) -> bool {
        self.insert_calls += 1;
        let start = self.hash(&key);
        let mut first_deleted: Option<usize> = None;
        let mut probes: u64 = 0;

        for i in 0..self.capacity {
            let idx = (start + i) % self.capacity;
            probes += 1;

            match &self.table[idx] {
                Slot::Empty => {
                    // Key not found — insert at tombstone if available, else here
                    let target = first_deleted.unwrap_or(idx);
                    self.table[target] = Slot::Occupied(key);
                    self.size += 1;
                    self.total_probes_insert += probes;
                    return true;
                }
                Slot::Deleted => {
                    if first_deleted.is_none() {
                        first_deleted = Some(idx);
                    }
                }
                Slot::Occupied(k) => {
                    if *k == key {
                        // Duplicate — do not insert
                        self.total_probes_insert += probes;
                        return false;
                    }
                }
            }
        }

        // All slots visited — reuse tombstone if any, else table is full
        if let Some(target) = first_deleted {
            self.table[target] = Slot::Occupied(key);
            self.size += 1;
            self.total_probes_insert += probes;
            return true;
        }

        self.total_probes_insert += probes;
        false // Table genuinely full
    }

    // -------------------------------------------------------------------------
    // find
    // -------------------------------------------------------------------------

    /// Search for a key in the table.
    ///
    /// Returns `true` if found, `false` otherwise.
    ///
    /// Terminates early at the first `Empty` slot — skips over tombstones.
    /// This is the standard correctness requirement for linear probing.
    pub fn find(&mut self, key: &K) -> bool {
        self.find_calls += 1;
        let start = self.hash(key);
        let mut probes: u64 = 0;

        for i in 0..self.capacity {
            let idx = (start + i) % self.capacity;
            probes += 1;

            match &self.table[idx] {
                Slot::Empty => {
                    // Unbroken empty slot — key cannot be further along
                    self.total_probes_find += probes;
                    self.failed_finds += 1;
                    return false;
                }
                Slot::Deleted => {
                    continue; // skip tombstone, keep probing
                }
                Slot::Occupied(k) => {
                    if k == key {
                        self.total_probes_find += probes;
                        self.successful_finds += 1;
                        return true;
                    }
                }
            }
        }

        self.total_probes_find += probes;
        self.failed_finds += 1;
        false
    }

    // -------------------------------------------------------------------------
    // delete
    // -------------------------------------------------------------------------

    /// Delete a key using lazy deletion (tombstone strategy).
    ///
    /// Returns `true` if key was found and deleted, `false` otherwise.
    ///
    /// Physical removal would break probe chains for other displaced keys,
    /// so we mark the slot as `Deleted` instead. Tombstones are reused on
    /// future inserts.
    pub fn delete(&mut self, key: &K) -> bool {
        self.delete_calls += 1;
        let start = self.hash(key);
        let mut probes: u64 = 0;

        for i in 0..self.capacity {
            let idx = (start + i) % self.capacity;
            probes += 1;

            match &self.table[idx] {
                Slot::Empty => {
                    self.total_probes_delete += probes;
                    return false; // key not present
                }
                Slot::Deleted => {
                    continue;
                }
                Slot::Occupied(k) => {
                    if k == key {
                        self.table[idx] = Slot::Deleted;
                        self.size -= 1;
                        self.total_probes_delete += probes;
                        return true;
                    }
                }
            }
        }

        self.total_probes_delete += probes;
        false
    }

    // -------------------------------------------------------------------------
    // bulk_load
    // -------------------------------------------------------------------------

    /// Insert all keys from an iterator sequentially.
    /// Returns the number of keys successfully inserted (duplicates excluded).
    pub fn bulk_load(&mut self, keys: impl IntoIterator<Item = K>) -> usize {
        let mut inserted = 0;
        for k in keys {
            if self.insert(k) {
                inserted += 1;
            }
        }
        inserted
    }

    // -------------------------------------------------------------------------
    // Benchmarking helpers
    // -------------------------------------------------------------------------

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn load_factor(&self) -> f64 {
        self.size as f64 / self.capacity as f64
    }

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

    pub fn avg_probes_find(&self) -> f64 {
        if self.find_calls == 0 { 0.0 }
        else { self.total_probes_find as f64 / self.find_calls as f64 }
    }

    pub fn avg_probes_delete(&self) -> f64 {
        if self.delete_calls == 0 { 0.0 }
        else { self.total_probes_delete as f64 / self.delete_calls as f64 }
    }

    /// Return a snapshot of all benchmarking metrics.
    pub fn stats(&self) -> Stats {
        Stats {
            capacity:            self.capacity,
            size:                self.size,
            load_factor:         self.load_factor(),
            empty_cells:         self.empty_cell_count(),
            tombstones:          self.tombstone_count(),
            insert_calls:        self.insert_calls,
            find_calls:          self.find_calls,
            delete_calls:        self.delete_calls,
            successful_finds:    self.successful_finds,
            failed_finds:        self.failed_finds,
            total_probes_insert: self.total_probes_insert,
            total_probes_find:   self.total_probes_find,
            total_probes_delete: self.total_probes_delete,
            avg_probes_insert:   self.avg_probes_insert(),
            avg_probes_find:     self.avg_probes_find(),
            avg_probes_delete:   self.avg_probes_delete(),
        }
    }

    /// Zero out all counters without touching table contents.
    /// Call this between insert and find phases in benchmarks.
    pub fn reset_stats(&mut self) {
        self.total_probes_insert = 0;
        self.total_probes_find   = 0;
        self.total_probes_delete = 0;
        self.insert_calls        = 0;
        self.find_calls          = 0;
        self.delete_calls        = 0;
        self.successful_finds    = 0;
        self.failed_finds        = 0;
    }
}

// =============================================================================
// Tests
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_find() {
        let mut ht = LinearProbingHashTable::new(16);
        assert!(ht.insert("apple"));
        assert!(ht.insert("banana"));
        assert!(ht.find(&"apple"));
        assert!(!ht.find(&"mango"));
    }

    #[test]
    fn test_duplicate_insert() {
        let mut ht = LinearProbingHashTable::new(16);
        assert!(ht.insert(42));
        assert!(!ht.insert(42));   // duplicate — returns false
        assert_eq!(ht.size(), 1);
    }

    #[test]
    fn test_delete_and_find() {
        let mut ht = LinearProbingHashTable::new(16);
        ht.insert("cherry");
        ht.insert("date");
        assert!(ht.delete(&"cherry"));
        assert!(!ht.find(&"cherry"));
        assert!(ht.find(&"date"));  // date still accessible through tombstone
    }

    #[test]
    fn test_delete_nonexistent() {
        let mut ht = LinearProbingHashTable::new(16);
        assert!(!ht.delete(&"ghost"));
    }

    #[test]
    fn test_reuse_tombstone() {
        let mut ht = LinearProbingHashTable::new(8);
        ht.insert(1u32);
        ht.insert(2u32);
        ht.delete(&1u32);
        ht.insert(3u32);             // should reuse tombstone
        assert!(ht.find(&2u32));
        assert!(ht.find(&3u32));
        assert_eq!(ht.size(), 2);
    }

    #[test]
    fn test_load_factor() {
        let mut ht = LinearProbingHashTable::<u64>::new(100);
        ht.bulk_load(0u64..80);
        assert!((ht.load_factor() - 0.80).abs() < 0.01);
    }

    #[test]
    fn test_probe_counts_increase_with_load() {
        let cap = 10_000usize;

        let mut ht50 = LinearProbingHashTable::<u64>::new(cap);
        ht50.bulk_load((0..5_000u64).map(|x| x * 7));
        ht50.reset_stats();
        for k in (0..5_000u64).map(|x| x * 7) { ht50.find(&k); }
        let probes_50 = ht50.avg_probes_find();

        let mut ht90 = LinearProbingHashTable::<u64>::new(cap);
        ht90.bulk_load((0..9_000u64).map(|x| x * 7));
        ht90.reset_stats();
        for k in (0..9_000u64).map(|x| x * 7) { ht90.find(&k); }
        let probes_90 = ht90.avg_probes_find();

        assert!(
            probes_90 > probes_50,
            "Expected more probes at 90% load than 50% load: {} vs {}",
            probes_90, probes_50
        );
    }
}

// =============================================================================
// main — smoke test + load factor sweep
// =============================================================================
fn main() {
    println!("{}", "=".repeat(65));
    println!("  Linear Probing Hash Table — Smoke Test & Load Factor Sweep");
    println!("{}", "=".repeat(65));

    // ── Correctness ──────────────────────────────────────────────────────
    let mut ht: LinearProbingHashTable<&str> = LinearProbingHashTable::new(16);
    ht.insert("apple");
    ht.insert("banana");
    ht.insert("cherry");

    assert!(ht.find(&"apple"),   "apple should be found");
    assert!(!ht.find(&"mango"),  "mango should not be found");
    ht.delete(&"banana");
    assert!(!ht.find(&"banana"), "banana should be gone");
    assert!(ht.find(&"cherry"),  "cherry should still be there");
    println!("Correctness checks passed.\n");

    // ── Load factor sweep ────────────────────────────────────────────────
    let capacity: usize = 100_000;

    println!(
        "{:>12}  {:>20}  {:>18}  {:>12}",
        "Load Factor", "Avg Probes Insert", "Avg Probes Find", "Empty Cells"
    );
    println!("{}", "-".repeat(68));

    for &lf in &[0.50f64, 0.70, 0.80, 0.90, 0.95] {
        let n = (capacity as f64 * lf) as u64;
        let mut ht = LinearProbingHashTable::<u64>::new(capacity);

        // Bulk insert
        ht.bulk_load(0..n);
        let avg_ins = ht.avg_probes_insert();
        let empty   = ht.empty_cell_count();
        ht.reset_stats();

        // Find — mix of hits and misses
        for k in 0..n                       { ht.find(&k); }           // all hits
        for k in (n * 10)..(n * 10 + 5000) { ht.find(&k); }           // misses

        println!(
            "  {:>10.0}%  {:>20.2}  {:>18.2}  {:>12}",
            lf * 100.0,
            avg_ins,
            ht.avg_probes_find(),
            empty,
        );
    }

    println!("\nDone.");
}