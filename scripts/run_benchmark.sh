#!/usr/bin/env sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
host=${1:-127.0.0.1}; port=${2:-9090}; seconds=${3:-15}; out=${4:-$root/results/phase1.csv}
mkdir -p "$(dirname "$out")"
for clients in 1 2 4 8 16 32 64 128; do "$root/build/benchmark" "$host" "$port" "$clients" "$seconds" 1 "$out"; done
