# CSD482 — Linear Probing Benchmark

Benchmarks Linear Probing against all 5 datasets from the project proposal
across load factors: 50%, 70%, 80%, 90%, 95%.

---

## Prerequisites

- Rust installed → https://rustup.rs  (one command: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
- Internet connection (for dataset downloads)

---

## Project Structure

```
slick_benchmark/
├── Cargo.toml
├── README.md
├── datasets/          ← created automatically by download_datasets
└── src/
    ├── linear_probing.rs   ← hash table implementation
    ├── download_datasets.rs← downloads all 4 real datasets
    └── benchmark.rs        ← parses datasets, runs benchmark, outputs table + CSV
```

---

## Step 1 — Build the project

```bash
cd slick_benchmark
cargo build --release
```

---

## Step 2 — Download datasets

```bash
cargo run --bin download_datasets --release
```

This will download into `./datasets/`:

| File | Dataset | Size |
|------|---------|------|
| `count_1w.txt` | Norvig word frequencies | ~4 MB |
| `wiki_titles.gz` | Wikipedia article titles | ~250 MB |
| `andorra-latest.osm.pbf` | OSM node IDs (Andorra) | ~1 MB |
| `maccdc2012_00000.pcap.gz` | PCAP network trace | ~200 MB |

> You only need to run this **once**.

---

## Step 3 — Run the benchmark

```bash
cargo run --bin benchmark --release
```

Output:
- Printed table in the terminal
- `results.csv` in the project root

---

## Output columns

| Column | Meaning |
|--------|---------|
| `LF%` | Actual load factor achieved |
| `Empty Cells` | Slots with no element ever inserted (key for H1) |
| `Ins Probes` | Avg probes per insert call |
| `FindHit Probes` | Avg probes for successful finds |
| `FindMiss Probes` | Avg probes for failed finds |
| `Ins ns/op` | Nanoseconds per insert |
| `Find ns/op` | Nanoseconds per find |

---

## Datasets and their roles in your hypotheses

| Dataset | Keys | Hypothesis tested |
|---------|------|------------------|
| Uniform Random | u64 | Baseline (H1, H2, H4) |
| Norvig Word Freq | Strings, Zipf skew | H2 — hot key bumping |
| Wikipedia Titles | Strings, heavy skew | H2, H3 — large vocabulary |
| OSM Node IDs | Clustered u64 | H4 — block alignment sensitivity |
| PCAP IP Addresses | Bursty u32 | H2 — non-uniform frequency |
