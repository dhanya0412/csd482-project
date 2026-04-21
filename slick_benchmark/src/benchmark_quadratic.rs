// =============================================================================
// benchmark_quadratic.rs — Quadratic Probing benchmark runner
// =============================================================================
// Parses each real dataset, extracts keys, runs Quadratic Probing across
// load factors 50/70/80/90/95%, prints a table, and writes quadratic_results.csv
//
// Usage (after running download_datasets):
//   cargo run --bin benchmark_quadratic --release
// =============================================================================

mod quadratic_probing;
use quadratic_probing::{BenchStats, QuadraticProbingHashTable};

use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::time::Instant;

use flate2::read::GzDecoder;

// =============================================================================
// ── Load factors to test ─────────────────────────────────────────────────────
// =============================================================================
const LOAD_FACTORS: &[f64] = &[0.50, 0.70, 0.80, 0.90, 0.95];

// =============================================================================
// ── Dataset parsers (identical to benchmark.rs) ──────────────────────────────
// =============================================================================

/// Dataset 1: Norvig count_1w.txt
fn load_norvig(path: &str) -> Vec<String> {
    println!("  Parsing Norvig word frequencies from {}...", path);
    let f    = File::open(path).expect("datasets/count_1w.txt not found — run download_datasets first");
    let keys: Vec<String> = BufReader::new(f)
        .lines()
        .filter_map(|l| {
            let line = l.ok()?;
            line.split('\t').next().map(|w| w.to_string())
        })
        .collect();
    println!("  → {} unique word keys loaded", keys.len());
    keys
}

/// Dataset 2: Wikipedia titles (.gz)
fn load_wikipedia(path: &str) -> Vec<String> {
    println!("  Parsing Wikipedia titles from {}...", path);
    let f   = File::open(path).expect("datasets/wiki_titles.gz not found — run download_datasets first");
    let gz  = GzDecoder::new(f);
    let keys: Vec<String> = BufReader::new(gz)
        .lines()
        .filter_map(|l| l.ok())
        .filter(|l| !l.is_empty())
        .take(2_000_000)
        .collect();
    println!("  → {} Wikipedia title keys loaded", keys.len());
    keys
}

/// Dataset 3: OSM Node IDs (.osm.pbf)
fn load_osm(path: &str) -> Vec<u64> {
    println!("  Parsing OSM node IDs from {}...", path);
    let mut f    = File::open(path).expect("datasets/andorra-latest.osm.pbf not found — run download_datasets first");
    let mut data = Vec::new();
    f.read_to_end(&mut data).unwrap();

    let keys: Vec<u64> = data
        .chunks_exact(8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .collect::<std::collections::HashSet<u64>>()
        .into_iter()
        .collect();

    println!("  → {} unique OSM u64 keys loaded", keys.len());
    keys
}

/// Dataset 5: Uniform random u64 baseline (PCG-style)
fn generate_uniform(n: usize) -> Vec<u64> {
    println!("  Generating {} uniform random u64 keys (PCG)...", n);
    let mut state: u64 = 0x853c49e6748fea9b;
    let inc:       u64 = 0xda3e39cb94b95bdb;
    let mut keys = Vec::with_capacity(n);
    for _ in 0..n {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(inc);
        let xorshifted = (((state >> 18) ^ state) >> 27) as u32;
        let rot = (state >> 59) as u32;
        let val = (xorshifted >> rot) | (xorshifted << (rot.wrapping_neg() & 31));
        keys.push(val as u64);
    }
    keys.sort_unstable();
    keys.dedup();
    keys
}

// =============================================================================
// ── Benchmark runner ──────────────────────────────────────────────────────────
// =============================================================================

/// Run quadratic probing benchmark for one dataset at one load factor.
fn run_bench<K>(
    dataset_name: &str,
    all_keys: &[K],
    load_factor: f64,
) -> BenchStats
where
    K: std::hash::Hash + Eq + Clone + std::fmt::Debug,
{
    let n        = all_keys.len();
    let use_n    = (n as f64 * load_factor).min(n as f64) as usize;
    let capacity = (use_n as f64 / load_factor).ceil() as usize + 1;

    let insert_keys = &all_keys[..use_n];
    let miss_keys: Vec<&K> = all_keys[use_n..]
        .iter()
        .take(5000)
        .collect();

    // ── Use QuadraticProbingHashTable instead of LinearProbingHashTable ──────
    let mut ht = QuadraticProbingHashTable::<K>::new(capacity);

    // ── Insert phase ─────────────────────────────────────────────────────────
    let t0 = Instant::now();
    for k in insert_keys { ht.insert(k.clone()); }
    let insert_elapsed = t0.elapsed();
    let insert_ns = insert_elapsed.as_nanos() as f64 / use_n as f64;

    // ── Find phase (hits + misses) ────────────────────────────────────────────
    let find_sample: Vec<&K> = insert_keys.iter().step_by(10).take(5000).collect();
    let t1 = Instant::now();
    for k in &find_sample { ht.find(k); }
    for k in &miss_keys   { ht.find(k); }
    let find_elapsed = t1.elapsed();
    let find_n = find_sample.len() + miss_keys.len();
    let find_ns = find_elapsed.as_nanos() as f64 / find_n.max(1) as f64;

    BenchStats {
        dataset:              dataset_name.to_string(),
        load_factor:          ht.load_factor(),
        capacity:             ht.capacity(),
        size:                 ht.size(),
        empty_cells:          ht.empty_cell_count(),
        tombstones:           ht.tombstone_count(),
        avg_probes_insert:    ht.avg_probes_insert(),
        avg_probes_find_hit:  ht.avg_probes_find_hit(),
        avg_probes_find_miss: ht.avg_probes_find_miss(),
        insert_ns_per_op:     insert_ns,
        find_ns_per_op:       find_ns,
    }
}

// =============================================================================
// ── Output helpers ────────────────────────────────────────────────────────────
// =============================================================================

fn print_table(results: &[BenchStats]) {
    println!("\n{}", "═".repeat(120));
    println!(
        "{:<28} {:>6}  {:>10}  {:>11}  {:>12}  {:>10}  {:>12}  {:>14}  {:>12}  {:>10}",
        "Dataset", "LF%", "Capacity", "Size", "Empty Cells",
        "Ins Probes", "FindHit Probes", "FindMiss Probes", "Ins ns/op", "Find ns/op"
    );
    println!("{}", "─".repeat(120));

    let mut last_ds = "";
    for r in results {
        if r.dataset.as_str() != last_ds && last_ds != "" {
            println!("{}", "─".repeat(120));
        }
        last_ds = &r.dataset;
        println!(
            "{:<28} {:>5.0}%  {:>10}  {:>11}  {:>12}  {:>10.2}  {:>12.2}  {:>15.2}  {:>12.1}  {:>10.1}",
            r.dataset,
            r.load_factor * 100.0,
            r.capacity,
            r.size,
            r.empty_cells,
            r.avg_probes_insert,
            r.avg_probes_find_hit,
            r.avg_probes_find_miss,
            r.insert_ns_per_op,
            r.find_ns_per_op,
        );
    }
    println!("{}", "═".repeat(120));
}

fn write_csv(results: &[BenchStats], path: &str) {
    let mut f = File::create(path).expect("Cannot create quadratic_results.csv");
    writeln!(f,
        "dataset,load_factor,capacity,size,empty_cells,tombstones,\
         avg_probes_insert,avg_probes_find_hit,avg_probes_find_miss,\
         insert_ns_per_op,find_ns_per_op"
    ).unwrap();
    for r in results {
        writeln!(f,
            "{},{:.4},{},{},{},{},{:.4},{:.4},{:.4},{:.2},{:.2}",
            r.dataset, r.load_factor, r.capacity, r.size,
            r.empty_cells, r.tombstones,
            r.avg_probes_insert, r.avg_probes_find_hit, r.avg_probes_find_miss,
            r.insert_ns_per_op, r.find_ns_per_op,
        ).unwrap();
    }
    println!("\nCSV written to {}", path);
}

// =============================================================================
// ── main ──────────────────────────────────────────────────────────────────────
// =============================================================================

fn main() {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  CSD482 — Quadratic Probing Benchmark (Real Datasets)      ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    let mut all_results: Vec<BenchStats> = Vec::new();

    // ── 1. Uniform random baseline ───────────────────────────────────────────
    println!("[1/4] Uniform Random u64 (baseline)");
    let uniform_keys = generate_uniform(500_000);
    for &lf in LOAD_FACTORS {
        all_results.push(run_bench("Uniform Random u64", &uniform_keys, lf));
    }

    // ── 2. Norvig word frequencies ───────────────────────────────────────────
    println!("\n[2/4] Norvig Word Frequencies (Zipf string keys)");
    if std::path::Path::new("datasets/count_1w.txt").exists() {
        let norvig_keys = load_norvig("datasets/count_1w.txt");
        for &lf in LOAD_FACTORS {
            all_results.push(run_bench("Norvig Zipf Strings", &norvig_keys, lf));
        }
    } else {
        println!("  ⚠ datasets/count_1w.txt not found — skipping. Run download_datasets first.");
    }

    // ── 3. Wikipedia titles ──────────────────────────────────────────────────
    println!("\n[3/4] Wikipedia Titles (heavy skew string keys)");
    if std::path::Path::new("datasets/wiki_titles.gz").exists() {
        let wiki_keys = load_wikipedia("datasets/wiki_titles.gz");
        for &lf in LOAD_FACTORS {
            all_results.push(run_bench("Wikipedia Strings", &wiki_keys, lf));
        }
    } else {
        println!("  ⚠ datasets/wiki_titles.gz not found — skipping. Run download_datasets first.");
    }

    // ── 4. OSM Node IDs ──────────────────────────────────────────────────────
    println!("\n[4/4] OSM Node IDs (clustered u64 integers)");
    if std::path::Path::new("datasets/andorra-latest.osm.pbf").exists() {
        let osm_keys = load_osm("datasets/andorra-latest.osm.pbf");
        for &lf in LOAD_FACTORS {
            all_results.push(run_bench("OSM Node IDs u64", &osm_keys, lf));
        }
    } else {
        println!("  ⚠ datasets/andorra-latest.osm.pbf not found — skipping. Run download_datasets first.");
    }

    // ── Output ───────────────────────────────────────────────────────────────
    print_table(&all_results);
    write_csv(&all_results, "quadratic_results.csv");
}
