// =============================================================================
// download_datasets.rs
// =============================================================================
// Downloads all 4 real datasets from your project proposal into ./datasets/
// Run this ONCE before running the benchmark binary.
//
// Usage:
//   cargo run --bin download_datasets
// =============================================================================

use std::fs::{self, File};
use std::io::{self, Write, BufWriter};
use std::path::Path;

fn main() {
    fs::create_dir_all("datasets").expect("Failed to create datasets/ directory");

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         CSD482 — Dataset Downloader                         ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    let tasks: Vec<(&str, &str, &str)> = vec![
        (
            "Norvig Word Frequencies (Zipf string keys)",
            "https://norvig.com/ngrams/count_1w.txt",
            "datasets/count_1w.txt",
        ),
        (
            "Wikipedia Word Frequencies (heavy skew string keys)",
            "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-all-titles-in-ns0.gz",
            "datasets/wiki_titles.gz",
        ),
        (
            "OSM Node IDs — Andorra (small region, clustered u64 integers)",
            "https://download.geofabrik.de/europe/andorra-latest.osm.pbf",
            "datasets/andorra-latest.osm.pbf",
        ),
        (
            "PCAP Network Trace — Maccdc 2012 (IP address keys)",
            "https://download.netresec.com/pcap/maccdc-2012/maccdc2012_00000.pcap.gz",
            "datasets/maccdc2012_00000.pcap.gz",
        ),
    ];

    for (name, url, dest) in &tasks {
        print!("[↓] {}...", name);
        io::stdout().flush().unwrap();

        if Path::new(dest).exists() {
            println!(" already exists, skipping.");
            continue;
        }

        match download(url, dest) {
            Ok(bytes) => println!(" done ({:.2} MB)", bytes as f64 / 1_048_576.0),
            Err(e)    => println!(" FAILED: {}", e),
        }
    }

    println!("\nAll downloads attempted. Files are in ./datasets/");
    println!("Now run:  cargo run --bin benchmark --release");
}

fn download(url: &str, dest: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let resp = ureq::get(url)
        .set("User-Agent", "CSD482-Benchmark/1.0")
        .call()?;

    let mut reader = resp.into_reader();
    let file  = File::create(dest)?;
    let mut writer = BufWriter::new(file);

    let mut buf   = [0u8; 65536];
    let mut total = 0usize;

    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 { break; }
        writer.write_all(&buf[..n])?;
        total += n;
        print!("\r[↓] Downloading... {:.2} MB", total as f64 / 1_048_576.0);
        io::stdout().flush().unwrap();
    }

    Ok(total)
}
