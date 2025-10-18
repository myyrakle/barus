rustup component add llvm-tools-preview
RUSTFLAGS="-Cprofile-generate=./target/pgo-data" cargo run --release
llvm-profdata merge -o ./target/pgo-data/merged.profdata ./target/pgo-data

# call apis to generate profile data...

RUSTFLAGS="-Cprofile-use=$(pwd)/target/pgo-data/merged.profdata" cargo run --release 
