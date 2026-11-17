#!/usr/bin/env bash
# Runs the five simulator experiments with 20 seeds each into results/.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build --release
BIN=target/release/oxtorrent
RUNS=${RUNS:-20}
mkdir -p results

for s in rarest_first random sequential; do
  $BIN simulate --scenario scenarios/piece_strategies.toml --runs "$RUNS" \
    --piece-strategy $s --out results/exp1_$s.csv
done
for s in rarest_first random sequential; do
  $BIN simulate --scenario scenarios/seeder_leaves.toml --runs "$RUNS" \
    --piece-strategy $s --out results/exp2_$s.csv --availability-out results/exp2_${s}_availability.csv
done
for c in tit_for_tat random no_reward; do
  $BIN simulate --scenario scenarios/free_riders.toml --runs "$RUNS" \
    --choke-strategy $c --out results/exp3_$c.csv
done
$BIN simulate --scenario scenarios/flash_crowd.toml --runs "$RUNS" \
  --out results/exp4_flash_crowd.csv --availability-out results/exp4_availability.csv
for n in 2 4 8 16; do
  $BIN simulate --scenario scenarios/upload_slots.toml --runs "$RUNS" \
    --slots $n --out results/exp5_slots_$n.csv
done

python3 scripts/charts.py
