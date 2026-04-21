# 1. Install Rust (skip if already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

# 2. Navigate to project
cd csd482-project/slick_benchmark

# 3. Build (downloads all crate dependencies automatically)
cargo build --release

# 4. Download datasets (one-time, ~455 MB)
cargo run --bin download_datasets --release

# 5. Run linear probing benchmark → results.csv
cargo run --bin benchmark --release

# 6. Run quadratic probing benchmark → quadratic_results.csv
cargo run --bin benchmark_quadratic --release
