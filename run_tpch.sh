#!/bin/bash
# Parameters
# BUFFER_POOL_SIZE=${1:?Usage: $0 <BUFFER_POOL_SIZE>}
BUFFER_POOL_SIZE=30000
# NUM_THREADS=${2:?Usage: $0 <BUFFER_POOL_SIZE> <NUM_THREADS>}
NUM_THREADS=20
MEMORY_SIZE=$BUFFER_POOL_SIZE
# MEMORY_SIZE=10000
# WORKING_MEM=142000
WORKING_MEM=$BUFFER_POOL_SIZE
QUERY=100
SF=10

# Set environment variables
export NUM_THREADS=$NUM_THREADS
export WORKING_MEM=$WORKING_MEM
export QUANTILE_METHOD=TPCH_100
export DATA_SOURCE="TPCH"
export SF=$SF
export QUERY_NUM=$QUERY
export NUM_TUPLES=$((6005720 * SF))  
# export NUM_TUPLES=$((1501530 * NUM_THREADS)) 
export TARGET_RUN_COUNT=150000
export BUFFER_POOL_SIZE=$BUFFER_POOL_SIZE

BP_DIR="bp-dir-tpch-sf-$SF"

# Clean up previous runs
find "$BP_DIR"/0 -mindepth 1 -maxdepth 1 -name '??*' -exec rm -rf {} +

RUST_BACKTRACE=1 cargo run --release --bin sort_run -- -q "$QUERY" -p "$BP_DIR" -n 1 -b "$BUFFER_POOL_SIZE" -m "$MEMORY_SIZE"

find "$BP_DIR"/0 -mindepth 1 -maxdepth 1 -name '??*' -exec rm -rf {} +
